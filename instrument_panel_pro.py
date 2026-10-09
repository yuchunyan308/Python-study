#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仪器控制面板 Pro —— PyQt6 + pyqtgraph 单文件 Demo
=================================================
运行：
    pip install PyQt6 pyqtgraph numpy
    python instrument_panel_pro.py

所有数据均为模拟，没有真实通讯。功能一览：
  · pyqtgraph 实时曲线：示波器（4 通道/时基/量程/触发/光标）、60 秒趋势（电压/电流/功率/温度）、FFT 频谱
  · 参数预设：内置 + 自定义，保存/加载/删除/导入/导出（保存在 ~/.instrument_panel_presets.json）
  · 电压斜坡序列：单次 / 往复，带预览曲线与进度
  · 报警系统：过温/过载阈值、自动跳闸、故障注入、报警历史
  · 数据录制 CSV、曲线截图 PNG、日志导出
  · 快捷键：Space 启停输出 · Esc 紧急停止 · F 冻结画面 · Ctrl+S 保存预设

接入真实设备时，从这几处入手：
    Panel.toggle_connect / on_start / on_stop / on_estop   连接与输出控制
    Panel.update_scope / update_spectrum                   换成真实采样数据
    Panel.refresh_readouts                                 换成真实读数
"""
import sys
import csv
import json
import math
import time
import random
from collections import deque
from pathlib import Path

try:
    import numpy as np
    from PyQt6.QtCore import Qt, QTimer, QRectF, QDateTime
    from PyQt6.QtGui import QColor, QPainter, QPen, QRadialGradient, QBrush, QShortcut, QKeySequence
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QFrame, QLabel, QPushButton, QComboBox, QSlider,
        QDoubleSpinBox, QSpinBox, QDial, QPlainTextEdit, QVBoxLayout, QHBoxLayout, QGridLayout,
        QButtonGroup, QStatusBar, QMessageBox, QTabWidget, QListWidget, QListWidgetItem,
        QCheckBox, QProgressBar, QScrollArea, QInputDialog, QFileDialog,
    )
    import pyqtgraph as pg
except ImportError as e:  # pragma: no cover
    print(f"缺少依赖：{e}\n请先执行：pip install PyQt6 pyqtgraph numpy")
    sys.exit(1)

pg.setConfigOptions(antialias=True, foreground="#8FA3C8")

CYAN, GREEN, AMBER, RED, GRAY, PINK = "#22D3EE", "#22C55E", "#F59E0B", "#EF4444", "#64748B", "#E879F9"
CH_COLORS = ["#FACC15", "#22D3EE", "#E879F9", "#4ADE80"]
CH_NAMES = ["电压", "电流", "谐波", "参考"]
PLOT_BG = "#070C18"
PRESET_FILE = Path.home() / ".instrument_panel_presets.json"

BUILTIN_PRESETS = {
    "出厂默认": dict(voltage=12.0, current=3.0, kind="正弦波", freq=50, channels=[True, False, False, False], mode=0, note="常规工作参数"),
    "低压精密": dict(voltage=3.3, current=0.5, kind="正弦波", freq=10, channels=[True, True, False, False], mode=0, note="3.3 V 低噪声场景"),
    "高频测试": dict(voltage=5.0, current=1.0, kind="方波", freq=800, channels=[True, True, True, False], mode=0, note="观察谐波与边沿"),
    "大电流老化": dict(voltage=24.0, current=5.0, kind="三角波", freq=20, channels=[True, True, False, True], mode=1, note="恒流老化测试"),
}
MODE_NAMES = ["恒压 CV", "恒流 CC", "恒功率 CP"]

QSS = """
* { font-family: "Segoe UI", "Microsoft YaHei UI", "PingFang SC", "Noto Sans CJK SC", sans-serif; }
QWidget { font-size: 13px; color: #D6E0F5; }
QMainWindow { background: #0B1220; }
QToolTip { background: #111A2E; color: #D6E0F5; border: 1px solid #263556; padding: 4px 8px; }

#topbar { background: #0F1729; border-bottom: 1px solid #1F2A44; }
#title { font-size: 17px; font-weight: bold; color: #FFFFFF; }
#subtitle { color: #7E8BA8; font-size: 12px; }
#clock { font-family: Consolas, "JetBrains Mono", "Courier New", monospace; color: #8FA3C8; font-size: 14px; }
#state { font-weight: bold; }
#rec { color: #EF4444; font-weight: bold; }
#alarmOn { background: #7F1D1D; color: #FFFFFF; border: 1px solid #EF4444; border-radius: 8px; padding: 5px 14px; font-weight: bold; }
#alarmOff { background: #B91C1C; color: #FFFFFF; border: 1px solid #FCA5A5; border-radius: 8px; padding: 5px 14px; font-weight: bold; }

#card { background: #111A2E; border: 1px solid #1F2A44; border-radius: 12px; }
#mini { background: #0B1220; border: 1px solid #1F2A44; border-radius: 8px; }
#cardTitle { color: #8FA3C8; font-size: 12px; font-weight: bold; }
#key { color: #7E8BA8; }
#val { color: #E6EEFF; }
#readTitle { color: #8FA3C8; font-size: 12px; }
#readValue { font-family: Consolas, "JetBrains Mono", "Courier New", monospace; font-size: 32px; font-weight: bold; }
#miniValue { font-family: Consolas, "JetBrains Mono", "Courier New", monospace; font-size: 16px; font-weight: bold; }
#unit { color: #7E8BA8; font-size: 14px; }
#dialValue { font-family: Consolas, "JetBrains Mono", "Courier New", monospace; font-size: 18px; font-weight: bold; color: #22D3EE; }
#hint { color: #7E8BA8; font-size: 12px; }
#pageInner { background: #0B1220; }

QPushButton { background: #17223B; border: 1px solid #263556; border-radius: 8px; padding: 7px 14px; }
QPushButton:hover { border-color: #22D3EE; color: #22D3EE; }
QPushButton:pressed { background: #1D2B4A; }
QPushButton:disabled { color: #4B587A; border-color: #1F2A44; background: #121B30; }
QPushButton:checked { background: rgba(34,211,238,38); border-color: #22D3EE; color: #22D3EE; font-weight: bold; }
QPushButton#rec:checked { background: rgba(239,68,68,45); border-color: #EF4444; color: #FCA5A5; }
QPushButton#connect { background: #22D3EE; border-color: #22D3EE; color: #06202A; font-weight: bold; }
QPushButton#connect:hover { background: #67E8F9; color: #06202A; }
QPushButton#connect:disabled { background: #17536A; color: #7FB3C4; }
QPushButton#start { background: #16A34A; border-color: #16A34A; color: #FFFFFF; font-weight: bold; padding: 10px; }
QPushButton#start:hover { background: #22C55E; color: #FFFFFF; }
QPushButton#start:disabled { background: #14301F; border-color: #14301F; color: #4E7A5D; }
QPushButton#stop { background: #B45309; border-color: #B45309; color: #FFFFFF; font-weight: bold; padding: 10px; }
QPushButton#stop:hover { background: #F59E0B; color: #FFFFFF; }
QPushButton#stop:disabled { background: #3A2A14; border-color: #3A2A14; color: #8A7350; }
QPushButton#estop { background: #B91C1C; border-color: #EF4444; color: #FFFFFF; font-weight: bold; font-size: 15px; padding: 11px; }
QPushButton#estop:hover { background: #EF4444; color: #FFFFFF; }
QPushButton#estop:disabled { background: #3A1818; border-color: #3A1818; color: #8A5050; }
QPushButton#seg { padding: 6px 0; border-radius: 6px; }
QPushButton#tool { padding: 5px 12px; border-radius: 6px; }
QPushButton#fault { color: #FCA5A5; border-color: #5B2A2A; }
QPushButton#fault:hover { background: #7F1D1D; color: #FFFFFF; border-color: #EF4444; }

QComboBox, QSpinBox, QDoubleSpinBox { background: #0B1220; border: 1px solid #263556; border-radius: 6px; padding: 5px 10px; selection-background-color: #22D3EE; selection-color: #06202A; }
QComboBox:hover, QSpinBox:hover, QDoubleSpinBox:hover { border-color: #3B4F7D; }
QComboBox:focus, QSpinBox:focus, QDoubleSpinBox:focus { border-color: #22D3EE; }
QComboBox:disabled, QSpinBox:disabled, QDoubleSpinBox:disabled { color: #4B587A; }
QComboBox QAbstractItemView { background: #111A2E; border: 1px solid #263556; selection-background-color: #1D2B4A; selection-color: #22D3EE; outline: none; }
QCheckBox { spacing: 8px; }

QSlider::groove:horizontal { height: 6px; background: #1F2A44; border-radius: 3px; }
QSlider::sub-page:horizontal { background: #22D3EE; border-radius: 3px; }
QSlider::handle:horizontal { background: #FFFFFF; border: 3px solid #22D3EE; width: 12px; height: 12px; margin: -8px 0; border-radius: 9px; }
QSlider::handle:horizontal:disabled { border-color: #3B4F7D; }
QSlider::sub-page:horizontal:disabled { background: #3B4F7D; }

QTabWidget::pane { border: none; top: 4px; }
QTabBar { background: transparent; }
QTabBar::tab { background: transparent; color: #7E8BA8; padding: 8px 16px; margin-right: 2px; border-bottom: 2px solid transparent; }
QTabBar::tab:selected { color: #22D3EE; border-bottom: 2px solid #22D3EE; font-weight: bold; }
QTabBar::tab:hover:!selected { color: #D6E0F5; }

QListWidget { background: #0B1220; border: 1px solid #263556; border-radius: 8px; padding: 4px; outline: none; }
QListWidget::item { padding: 8px 10px; border-radius: 6px; }
QListWidget::item:selected { background: rgba(34,211,238,38); color: #22D3EE; }
QListWidget::item:hover:!selected { background: #17223B; }

QProgressBar { background: #0B1220; border: 1px solid #263556; border-radius: 6px; text-align: center; height: 16px; color: #D6E0F5; }
QProgressBar::chunk { background: #22D3EE; border-radius: 5px; }

QPlainTextEdit { background: #0A0F1C; border: none; border-radius: 8px; padding: 8px; font-family: Consolas, "JetBrains Mono", "Courier New", monospace; font-size: 12px; color: #9FB3D9; }
QScrollArea { background: #0B1220; border: none; }
QScrollBar:vertical { background: transparent; width: 10px; margin: 2px; }
QScrollBar::handle:vertical { background: #263556; border-radius: 4px; min-height: 30px; }
QScrollBar::add-line, QScrollBar::sub-line { width: 0; height: 0; }
QScrollBar::add-page, QScrollBar::sub-page { background: transparent; }

QStatusBar { background: #0F1729; border-top: 1px solid #1F2A44; color: #7E8BA8; }
QStatusBar::item { border: none; }
"""


# ============================================================ 工具函数
def wave_np(kind, ph):
    """ph 为以"周期"为单位的相位数组。"""
    if kind == "正弦波":
        return np.sin(2 * np.pi * ph)
    if kind == "方波":
        return np.where(np.sin(2 * np.pi * ph) >= 0, 1.0, -1.0)
    if kind == "三角波":
        return 2 / np.pi * np.arcsin(np.sin(2 * np.pi * ph))
    return 2 * (ph - np.floor(ph + 0.5))  # 锯齿波


def style_pi(pi, left=None, bottom=None):
    pi.showGrid(x=True, y=True, alpha=0.16)
    pi.hideButtons()
    pi.setMenuEnabled(False)
    for name in ("left", "bottom"):
        ax = pi.getAxis(name)
        ax.setPen(pg.mkPen("#2C4170"))
        ax.setTextPen(pg.mkPen("#8FA3C8"))
        ax.enableAutoSIPrefix(False)
    pi.getAxis("left").setWidth(58)
    if left:
        pi.setLabel("left", left)
    if bottom:
        pi.setLabel("bottom", bottom)


def card(title=None):
    f = QFrame()
    f.setObjectName("card")
    lay = QVBoxLayout(f)
    lay.setContentsMargins(16, 14, 16, 14)
    lay.setSpacing(9)
    if title:
        t = QLabel(title)
        t.setObjectName("cardTitle")
        lay.addWidget(t)
    return f, lay


def kv_row(key, value="--"):
    row = QHBoxLayout()
    k = QLabel(key)
    k.setObjectName("key")
    v = QLabel(value)
    v.setObjectName("val")
    v.setAlignment(Qt.AlignmentFlag.AlignRight)
    row.addWidget(k)
    row.addStretch(1)
    row.addWidget(v)
    return row, v


def mini_stat(title, color):
    f = QFrame()
    f.setObjectName("mini")
    lay = QVBoxLayout(f)
    lay.setContentsMargins(12, 7, 12, 7)
    lay.setSpacing(0)
    t = QLabel(title)
    t.setObjectName("hint")
    v = QLabel("--")
    v.setObjectName("miniValue")
    v.setStyleSheet(f"color: {color};")
    lay.addWidget(t)
    lay.addWidget(v)
    return f, v


def scroll_page(widget):
    widget.setObjectName("pageInner")
    sa = QScrollArea()
    sa.setWidgetResizable(True)
    sa.setFrameShape(QFrame.Shape.NoFrame)
    sa.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
    sa.setWidget(widget)
    return sa


def hms(sec):
    sec = int(sec)
    return f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


# ============================================================ 自绘组件
class Led(QWidget):
    def __init__(self, color=GRAY):
        super().__init__()
        self.setFixedSize(16, 16)
        self.color = QColor(color)

    def set_color(self, color):
        self.color = QColor(color)
        self.update()

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        glow = QColor(self.color)
        glow.setAlpha(60)
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(glow)
        p.drawEllipse(0, 0, 16, 16)
        g = QRadialGradient(8, 7, 6)
        g.setColorAt(0, self.color.lighter(170))
        g.setColorAt(1, self.color)
        p.setBrush(QBrush(g))
        p.drawEllipse(3, 3, 10, 10)
        p.end()


class ArcGauge(QWidget):
    def __init__(self, title, unit, vmax, color):
        super().__init__()
        self.title, self.unit, self.vmax, self.color = title, unit, vmax, QColor(color)
        self.value = 0.0
        self.warn = None
        self.setMinimumSize(110, 110)

    def set_value(self, v):
        self.value = max(0.0, min(v, self.vmax))
        self.update()

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        s = min(self.width(), self.height()) - 16
        rect = QRectF((self.width() - s) / 2, 6, s, s)
        pw = 9
        r = rect.adjusted(pw / 2, pw / 2, -pw / 2, -pw / 2)
        pen = QPen(QColor("#1F2A44"), pw)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        p.setPen(pen)
        p.drawArc(r, 225 * 16, -270 * 16)
        pen.setColor(QColor(RED) if (self.warn is not None and self.value >= self.warn) else self.color)
        p.setPen(pen)
        p.drawArc(r, 225 * 16, int(-270 * 16 * self.value / self.vmax))
        AC = Qt.AlignmentFlag.AlignCenter
        p.setPen(QColor("#E6EEFF"))
        f = p.font()
        f.setFamily("Consolas")
        f.setPointSize(14)
        f.setBold(True)
        p.setFont(f)
        p.drawText(QRectF(rect.x(), rect.center().y() - 15, rect.width(), 24), AC, f"{self.value:.0f}{self.unit}")
        f.setPointSize(9)
        f.setBold(False)
        p.setFont(f)
        p.setPen(QColor("#7E8BA8"))
        p.drawText(QRectF(rect.x(), rect.center().y() + 9, rect.width(), 18), AC, self.title)
        p.end()


# ============================================================ 右侧功能页：预设
class PresetPage(QWidget):
    def __init__(self, panel):
        super().__init__()
        self.p = panel
        self.user = self.load_user()
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 6, 0)
        lay.setSpacing(12)
        c, l = card("参数预设")
        self.list = QListWidget()
        self.list.setMinimumHeight(190)
        self.list.currentItemChanged.connect(self.show_detail)
        self.list.itemDoubleClicked.connect(lambda _i: self.load())
        l.addWidget(self.list)
        self.detail = QLabel("选择一个预设查看详情")
        self.detail.setObjectName("hint")
        self.detail.setWordWrap(True)
        self.detail.setMinimumHeight(72)
        self.detail.setAlignment(Qt.AlignmentFlag.AlignTop)
        l.addWidget(self.detail)
        row = QHBoxLayout()
        self.btn_load = QPushButton("加载")
        self.btn_load.setObjectName("connect")
        self.btn_load.clicked.connect(self.load)
        self.btn_save = QPushButton("保存当前…")
        self.btn_save.clicked.connect(self.save_current)
        self.btn_del = QPushButton("删除")
        self.btn_del.setObjectName("fault")
        self.btn_del.clicked.connect(self.delete)
        for b in (self.btn_load, self.btn_save, self.btn_del):
            row.addWidget(b)
        l.addLayout(row)
        row2 = QHBoxLayout()
        b1, b2 = QPushButton("导入…"), QPushButton("导出…")
        b1.clicked.connect(self.import_json)
        b2.clicked.connect(self.export_json)
        row2.addWidget(b1)
        row2.addWidget(b2)
        l.addLayout(row2)
        lay.addWidget(c)
        tip = QLabel("提示：双击预设可直接加载；★ 为内置预设（不可删除/覆盖）。\n自定义预设保存在用户目录的 JSON 文件中。")
        tip.setObjectName("hint")
        tip.setWordWrap(True)
        lay.addWidget(tip)
        lay.addStretch(1)
        self.refresh()

    # ---- 存取
    @staticmethod
    def load_user():
        try:
            data = json.loads(PRESET_FILE.read_text(encoding="utf-8"))
            return {k: v for k, v in data.items() if isinstance(v, dict)}
        except (OSError, ValueError):
            return {}

    def persist(self):
        try:
            PRESET_FILE.write_text(json.dumps(self.user, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError as e:
            QMessageBox.warning(self, "保存失败", str(e))

    def all_presets(self):
        d = {k: dict(v, builtin=True) for k, v in BUILTIN_PRESETS.items()}
        d.update({k: dict(v, builtin=False) for k, v in self.user.items()})
        return d

    def refresh(self, select=None):
        self.list.clear()
        for name, d in self.all_presets().items():
            it = QListWidgetItem(("★  " if d["builtin"] else "●  ") + name)
            it.setData(Qt.ItemDataRole.UserRole, name)
            self.list.addItem(it)
            if name == select:
                self.list.setCurrentItem(it)
        if self.list.currentItem() is None and self.list.count():
            self.list.setCurrentRow(0)

    def current_name(self):
        it = self.list.currentItem()
        return it.data(Qt.ItemDataRole.UserRole) if it else None

    def show_detail(self, *_):
        name = self.current_name()
        d = self.all_presets().get(name) if name else None
        if not d:
            self.detail.setText("选择一个预设查看详情")
            self.btn_del.setEnabled(False)
            return
        chs = " ".join(f"CH{i + 1}" for i, on in enumerate(d.get("channels", [])) if on) or "无"
        mode = MODE_NAMES[min(int(d.get("mode", 0)), 2)]
        self.detail.setText(f"电压 {d['voltage']:.1f} V · 限流 {d['current']:.1f} A\n"
                            f"{d['kind']} · {d['freq']} Hz · {mode}\n通道：{chs}\n{d.get('note', '')}")
        self.btn_del.setEnabled(not d["builtin"])

    # ---- 操作
    def load(self):
        name = self.current_name()
        d = self.all_presets().get(name) if name else None
        if not d:
            return
        self.p.apply_params(d)
        self.p.log(f"已加载预设「{name}」", CYAN)
        self.p.toast(f"已加载预设：{name}")

    def save_current(self):
        name, ok = QInputDialog.getText(self, "保存预设", "预设名称：")
        name = name.strip()
        if not ok or not name:
            return
        if name in BUILTIN_PRESETS:
            QMessageBox.information(self, "提示", "内置预设不可覆盖，请换一个名称。")
            return
        if name in self.user and QMessageBox.question(self, "覆盖确认", f"预设「{name}」已存在，是否覆盖？") != QMessageBox.StandardButton.Yes:
            return
        d = self.p.collect_params()
        d["note"] = "自定义预设 · " + QDateTime.currentDateTime().toString("MM-dd HH:mm")
        self.user[name] = d
        self.persist()
        self.refresh(select=name)
        self.p.log(f"已保存预设「{name}」", GREEN)
        self.p.toast(f"已保存预设：{name}")

    def delete(self):
        name = self.current_name()
        if name in self.user and QMessageBox.question(self, "删除确认", f"确定删除预设「{name}」吗？") == QMessageBox.StandardButton.Yes:
            del self.user[name]
            self.persist()
            self.refresh()
            self.p.log(f"已删除预设「{name}」", AMBER)

    def export_json(self):
        path, _ = QFileDialog.getSaveFileName(self, "导出预设", "presets.json", "JSON 文件 (*.json)")
        if path:
            try:
                Path(path).write_text(json.dumps(self.user, ensure_ascii=False, indent=2), encoding="utf-8")
                self.p.log(f"已导出 {len(self.user)} 个自定义预设 → {path}")
            except OSError as e:
                QMessageBox.warning(self, "导出失败", str(e))

    def import_json(self):
        path, _ = QFileDialog.getOpenFileName(self, "导入预设", "", "JSON 文件 (*.json)")
        if not path:
            return
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
            n = 0
            for k, v in data.items():
                if isinstance(v, dict) and {"voltage", "current", "kind", "freq"} <= set(v) and k not in BUILTIN_PRESETS:
                    v.setdefault("channels", [True, False, False, False])
                    v.setdefault("mode", 0)
                    self.user[k] = v
                    n += 1
            self.persist()
            self.refresh()
            self.p.log(f"已导入 {n} 个预设", GREEN)
        except (OSError, ValueError, AttributeError) as e:
            QMessageBox.warning(self, "导入失败", f"文件格式不正确：{e}")


# ============================================================ 右侧功能页：序列
class SequencePage(QWidget):
    def __init__(self, panel):
        super().__init__()
        self.p = panel
        self.active = False
        self.t0 = 0.0
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 6, 0)
        lay.setSpacing(12)
        c, l = card("电压斜坡 / 扫描")
        g = QGridLayout()
        g.setHorizontalSpacing(10)
        g.setVerticalSpacing(8)
        self.s_start = QDoubleSpinBox()
        self.s_end = QDoubleSpinBox()
        for s, v in ((self.s_start, 0.0), (self.s_end, 20.0)):
            s.setRange(0, 30)
            s.setDecimals(1)
            s.setSuffix(" V")
            s.setValue(v)
            s.valueChanged.connect(self.update_preview)
        self.s_dur = QSpinBox()
        self.s_dur.setRange(2, 600)
        self.s_dur.setValue(20)
        self.s_dur.setSuffix(" s")
        self.s_dur.valueChanged.connect(self.update_preview)
        self.s_mode = QComboBox()
        self.s_mode.addItems(["单次", "往复循环"])
        self.s_mode.currentIndexChanged.connect(self.update_preview)
        for r, (name, w) in enumerate((("起始电压", self.s_start), ("终止电压", self.s_end),
                                       ("持续时间", self.s_dur), ("运行方式", self.s_mode))):
            g.addWidget(QLabel(name), r, 0)
            g.addWidget(w, r, 1)
        l.addLayout(g)
        self.preview = pg.PlotWidget()
        self.preview.setBackground(PLOT_BG)
        self.preview.setFixedHeight(130)
        self.preview.setMouseEnabled(False, False)
        style_pi(self.preview.getPlotItem(), "V", None)
        self.preview.getPlotItem().getAxis("left").setWidth(40)
        self.curve = self.preview.plot(pen=pg.mkPen(CYAN, width=2))
        self.marker = pg.InfiniteLine(angle=90, pen=pg.mkPen(AMBER, width=2))
        self.preview.addItem(self.marker)
        l.addWidget(self.preview)
        self.bar = QProgressBar()
        self.bar.setRange(0, 100)
        l.addWidget(self.bar)
        row = QHBoxLayout()
        self.btn_go = QPushButton("▶ 开始序列")
        self.btn_go.setObjectName("start")
        self.btn_go.clicked.connect(self.start)
        self.btn_no = QPushButton("■ 停止")
        self.btn_no.setObjectName("stop")
        self.btn_no.clicked.connect(lambda: self.stop())
        self.btn_no.setEnabled(False)
        row.addWidget(self.btn_go)
        row.addWidget(self.btn_no)
        l.addLayout(row)
        lay.addWidget(c)
        tip = QLabel("序列会自动按时间线性调节输出电压，并同步反映在波形、读数和趋势曲线上。")
        tip.setObjectName("hint")
        tip.setWordWrap(True)
        lay.addWidget(tip)
        lay.addStretch(1)
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.tick)
        self.update_preview()

    def update_preview(self, *_):
        s, e, d = self.s_start.value(), self.s_end.value(), self.s_dur.value()
        if self.s_mode.currentIndex() == 1:
            self.curve.setData([0, d / 2, d], [s, e, s])
        else:
            self.curve.setData([0, d], [s, e])
        pi = self.preview.getPlotItem()
        pi.setXRange(0, d, padding=0)
        pi.setYRange(min(s, e) - 2, max(s, e) + 2, padding=0)

    def start(self):
        if not self.p.connected:
            QMessageBox.information(self, "提示", "请先连接设备。")
            return
        if not self.p.running:
            self.p.on_start()
        self.active = True
        self.t0 = time.monotonic()
        self.btn_go.setEnabled(False)
        self.btn_no.setEnabled(True)
        self.p.log(f"序列开始：{self.s_start.value():.1f} V → {self.s_end.value():.1f} V / {self.s_dur.value()} s "
                   f"({self.s_mode.currentText()})", CYAN)
        self.timer.start(100)

    def stop(self, finished=False, silent=False):
        if not self.active:
            return
        self.active = False
        self.timer.stop()
        self.btn_go.setEnabled(True)
        self.btn_no.setEnabled(False)
        if not silent:
            self.p.log("序列已完成" if finished else "序列已停止", GREEN if finished else AMBER)

    def tick(self):
        el = time.monotonic() - self.t0
        d = float(self.s_dur.value())
        s, e = self.s_start.value(), self.s_end.value()
        if self.s_mode.currentIndex() == 1:
            cyc = (el % d) / d
            val = s + (e - s) * (1 - abs(2 * cyc - 1))
            pos, prog = el % d, cyc
        else:
            prog = min(1.0, el / d)
            val = s + (e - s) * prog
            pos = min(el, d)
        self.p.v_spin.setValue(round(val, 1))
        self.marker.setPos(pos)
        self.bar.setValue(int(prog * 100))
        if self.s_mode.currentIndex() == 0 and el >= d:
            self.stop(finished=True)


# ============================================================ 右侧功能页：报警
class AlarmPage(QWidget):
    def __init__(self, panel):
        super().__init__()
        self.p = panel
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 6, 0)
        lay.setSpacing(12)
        c, l = card("报警阈值")
        g = QGridLayout()
        g.setHorizontalSpacing(10)
        g.setVerticalSpacing(8)
        self.t_max = QSpinBox()
        self.t_max.setRange(40, 100)
        self.t_max.setValue(65)
        self.t_max.setSuffix(" ℃")
        self.p_max = QSpinBox()
        self.p_max.setRange(10, 300)
        self.p_max.setValue(120)
        self.p_max.setSuffix(" W")
        self.t_max.valueChanged.connect(self.sync)
        g.addWidget(QLabel("过温报警"), 0, 0)
        g.addWidget(self.t_max, 0, 1)
        g.addWidget(QLabel("过载报警"), 1, 0)
        g.addWidget(self.p_max, 1, 1)
        l.addLayout(g)
        self.trip = QCheckBox("报警时自动切断输出（跳闸）")
        self.trip.setChecked(True)
        l.addWidget(self.trip)
        lay.addWidget(c)

        c2, l2 = card("故障注入（演示用）")
        row = QHBoxLayout()
        b1, b2 = QPushButton("模拟过温 8s"), QPushButton("模拟过载 8s")
        for b in (b1, b2):
            b.setObjectName("fault")
            row.addWidget(b)
        b1.clicked.connect(lambda: self.p.inject("temp"))
        b2.clicked.connect(lambda: self.p.inject("power"))
        l2.addLayout(row)
        lay.addWidget(c2)

        c3, l3 = card("报警历史")
        self.hist = QListWidget()
        self.hist.setMinimumHeight(150)
        l3.addWidget(self.hist)
        clr = QPushButton("清空历史")
        clr.clicked.connect(self.hist.clear)
        l3.addWidget(clr)
        lay.addWidget(c3, 1)
        self.sync()

    def sync(self):
        self.p.gauge_temp.warn = self.t_max.value()

    def add(self, text, color=RED):
        it = QListWidgetItem(f"[{QDateTime.currentDateTime().toString('HH:mm:ss')}] {text}")
        it.setForeground(QColor(color))
        self.hist.insertItem(0, it)


# ============================================================ 主窗口
class Panel(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("仪器控制面板 Pro")
        self.resize(1520, 920)
        self.setMinimumSize(1320, 800)

        self.connected = self.running = self.frozen = False
        self.phase, self.frame = 0.0, 0
        self.tdiv = 2.0
        self.scope_t = np.linspace(0, 20, 900)
        self.temp = 28.0
        self.run_seconds, self.energy_wh = 0.0, 0.0
        self.inject_until = {"temp": 0.0, "power": 0.0}
        self.alarms = set()
        self.blink = False
        self.hist = {k: deque(maxlen=240) for k in ("t", "v", "i", "p", "T")}
        self.rec_file = self.rec_writer = None
        self.rec_rows = 0

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        root.addWidget(self.build_topbar())
        body = QWidget()
        bl = QVBoxLayout(body)
        bl.setContentsMargins(16, 12, 16, 10)
        bl.setSpacing(12)
        mid = QHBoxLayout()
        mid.setSpacing(14)
        mid.addWidget(self.build_left())
        mid.addLayout(self.build_center(), 1)
        mid.addWidget(self.build_right())
        bl.addLayout(mid, 1)
        bl.addWidget(self.build_log())
        root.addWidget(body, 1)

        sb = QStatusBar()
        self.setStatusBar(sb)
        sb.showMessage("就绪 · 请先连接设备（演示模式，所有数据均为模拟） · 快捷键：Space 启停 / Esc 急停 / F 冻结 / Ctrl+S 存预设")

        self.apply_scope_range()
        self.set_connected(False)
        for key, fn in (("Space", self.toggle_output), ("Esc", self.on_estop), ("F", self.btn_freeze.toggle),
                        ("Ctrl+S", self.preset_page.save_current)):
            QShortcut(QKeySequence(key), self, activated=fn)

        self.t_scope = QTimer(self)
        self.t_scope.timeout.connect(self.update_scope)
        self.t_scope.start(33)
        self.t_spec = QTimer(self)
        self.t_spec.timeout.connect(self.update_spectrum)
        self.t_spec.start(120)
        self.t_slow = QTimer(self)
        self.t_slow.timeout.connect(self.refresh_readouts)
        self.t_slow.start(500)
        self.log("系统启动完成，等待连接设备", "#7E8BA8")
        self.refresh_readouts()

    # ================================================== 构建界面
    def build_topbar(self):
        bar = QFrame()
        bar.setObjectName("topbar")
        bar.setFixedHeight(62)
        h = QHBoxLayout(bar)
        h.setContentsMargins(20, 0, 20, 0)
        col = QVBoxLayout()
        col.setSpacing(0)
        t = QLabel("◉ 仪器控制面板 Pro")
        t.setObjectName("title")
        s = QLabel("可编程电源 · 实时监测 / 预设 / 序列 / 报警")
        s.setObjectName("subtitle")
        col.addStretch(1)
        col.addWidget(t)
        col.addWidget(s)
        col.addStretch(1)
        h.addLayout(col)
        h.addStretch(1)
        self.banner = QLabel("")
        self.banner.setObjectName("alarmOn")
        self.banner.hide()
        h.addWidget(self.banner)
        h.addStretch(1)
        self.rec_label = QLabel("")
        self.rec_label.setObjectName("rec")
        h.addWidget(self.rec_label)
        h.addSpacing(18)
        self.top_led = Led()
        self.top_state = QLabel("未连接")
        self.top_state.setObjectName("state")
        h.addWidget(self.top_led)
        h.addWidget(self.top_state)
        h.addSpacing(24)
        self.clock = QLabel()
        self.clock.setObjectName("clock")
        h.addWidget(self.clock)
        return bar

    def build_left(self):
        wrap = QWidget()
        wrap.setFixedWidth(272)
        lay = QVBoxLayout(wrap)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(12)

        c1, l1 = card("设备连接")
        self.port = QComboBox()
        self.port.addItems(["COM3", "COM4", "/dev/ttyUSB0", "TCP 192.168.1.50:5025"])
        self.baud = QComboBox()
        self.baud.addItems(["9600", "19200", "57600", "115200"])
        self.baud.setCurrentText("115200")
        l1.addWidget(self.port)
        l1.addWidget(self.baud)
        self.btn_connect = QPushButton("连接设备")
        self.btn_connect.setObjectName("connect")
        self.btn_connect.setMinimumHeight(38)
        self.btn_connect.clicked.connect(self.toggle_connect)
        l1.addWidget(self.btn_connect)
        lay.addWidget(c1)

        c2, l2 = card("仪器信息")
        self.info = {}
        for k in ("型号", "序列号", "固件版本", "累计输出", "累计能量"):
            row, v = kv_row(k)
            self.info[k] = v
            l2.addLayout(row)
        lay.addWidget(c2)

        c3, l3 = card("运行状态")
        g = QHBoxLayout()
        self.gauge_load = ArcGauge("负载率", "%", 100, CYAN)
        self.gauge_temp = ArcGauge("温度", "℃", 100, AMBER)
        g.addWidget(self.gauge_load)
        g.addWidget(self.gauge_temp)
        l3.addLayout(g)
        lay.addWidget(c3, 1)
        return wrap

    def build_center(self):
        col = QVBoxLayout()
        col.setSpacing(12)
        c, l = card()
        # 工具栏
        tb = QHBoxLayout()
        tb.setSpacing(8)
        t = QLabel("实时监测")
        t.setObjectName("cardTitle")
        tb.addWidget(t)
        tb.addStretch(1)
        self.tbase = QComboBox()
        self.tbase.addItems(["0.5 ms/div", "1 ms/div", "2 ms/div", "5 ms/div", "10 ms/div", "20 ms/div"])
        self.tbase.setCurrentIndex(2)
        self.vscale = QComboBox()
        self.vscale.addItems(["±5 V", "±15 V", "±35 V", "自动"])
        self.vscale.setCurrentIndex(1)
        self.trig = QComboBox()
        self.trig.addItems(["触发：滚动", "触发：稳定"])
        for w in (self.tbase, self.vscale, self.trig):
            w.currentIndexChanged.connect(self.apply_scope_range)
            tb.addWidget(w)
        self.btn_freeze = QPushButton("⏸ 冻结")
        self.btn_cursor = QPushButton("⌖ 光标")
        self.btn_snap = QPushButton("📷 截图")
        self.btn_rec = QPushButton("● 录制")
        self.btn_rec.setObjectName("rec")
        for b in (self.btn_freeze, self.btn_cursor, self.btn_rec):
            b.setCheckable(True)
        for b in (self.btn_freeze, self.btn_cursor, self.btn_snap, self.btn_rec):
            b.setObjectName(b.objectName() or "tool")
            tb.addWidget(b)
        self.btn_rec.setStyleSheet("")
        self.btn_freeze.toggled.connect(self.on_freeze)
        self.btn_cursor.toggled.connect(self.on_cursor)
        self.btn_snap.clicked.connect(self.on_snapshot)
        self.btn_rec.toggled.connect(self.on_record)
        l.addLayout(tb)

        self.plot_tabs = QTabWidget()
        self.plot_tabs.setDocumentMode(True)
        self.plot_tabs.addTab(self.build_scope_page(), "示波器")
        self.plot_tabs.addTab(self.build_trend_page(), "趋势（60 秒）")
        self.plot_tabs.addTab(self.build_spectrum_page(), "频谱 FFT")
        l.addWidget(self.plot_tabs, 1)
        col.addWidget(c, 1)

        row = QHBoxLayout()
        row.setSpacing(12)
        self.reads = {}
        for key, title, unit, color in (("v", "输出电压", "V", CYAN), ("i", "输出电流", "A", AMBER),
                                        ("p", "输出功率", "W", GREEN), ("f", "输出频率", "Hz", PINK)):
            f, ll = card()
            ll.setSpacing(0)
            tl = QLabel(title)
            tl.setObjectName("readTitle")
            v = QLabel("0.000")
            v.setObjectName("readValue")
            v.setStyleSheet(f"color: {color};")
            u = QLabel(unit)
            u.setObjectName("unit")
            hl = QHBoxLayout()
            hl.addWidget(v)
            hl.addWidget(u, 0, Qt.AlignmentFlag.AlignBottom)
            hl.addStretch(1)
            ll.addWidget(tl)
            ll.addLayout(hl)
            self.reads[key] = v
            row.addWidget(f)
        col.addLayout(row)
        return col

    def build_scope_page(self):
        page = QWidget()
        v = QVBoxLayout(page)
        v.setContentsMargins(0, 0, 0, 0)
        v.setSpacing(8)
        self.scope = pg.PlotWidget()
        self.scope.setBackground(PLOT_BG)
        self.scope.setMouseEnabled(False, False)
        pi = self.scope.getPlotItem()
        style_pi(pi, "幅值 (V / A)", "时间 (ms)")
        self.glows, self.lines = [], []
        for c in CH_COLORS:
            g = QColor(c)
            g.setAlpha(48)
            self.glows.append(pi.plot(pen=pg.mkPen(g, width=7)))
            self.lines.append(pi.plot(pen=pg.mkPen(QColor(c), width=2)))
        opts = dict(angle=90, movable=True, pen=pg.mkPen("#FACC15", width=1, style=Qt.PenStyle.DashLine))
        self.cur_a = pg.InfiniteLine(pos=3, label="A", labelOpts=dict(position=0.93, color="#FACC15"), **opts)
        self.cur_b = pg.InfiniteLine(pos=7, label="B", labelOpts=dict(position=0.93, color="#FACC15"), **opts)
        for c in (self.cur_a, self.cur_b):
            c.setVisible(False)
            c.sigPositionChanged.connect(self.update_cursor_info)
            pi.addItem(c)
        v.addWidget(self.scope, 1)

        strip = QHBoxLayout()
        strip.setSpacing(8)
        self.meas = {}
        for key, title, color in (("vpp", "峰峰值 Vpp", CYAN), ("vrms", "有效值 Vrms", GREEN),
                                  ("mean", "平均值 Vavg", AMBER), ("freq", "频率 Freq", PINK),
                                  ("dt", "光标 Δt / 1/Δt", "#FACC15")):
            f, lab = mini_stat(title, color)
            self.meas[key] = lab
            strip.addWidget(f)
        v.addLayout(strip)
        return page

    def build_trend_page(self):
        self.trend = pg.GraphicsLayoutWidget()
        self.trend.setBackground(PLOT_BG)
        self.trend_curves = {}
        first = None
        specs = (("v", "电压 (V)", CYAN), ("i", "电流 (A)", AMBER), ("p", "功率 (W)", GREEN), ("T", "温度 (℃)", "#F87171"))
        for r, (k, label, color) in enumerate(specs):
            pi = self.trend.addPlot(row=r, col=0)
            style_pi(pi, label, "时间 (s)" if r == len(specs) - 1 else None)
            pi.setMouseEnabled(x=False, y=False)
            if r < len(specs) - 1:
                pi.getAxis("bottom").setStyle(showValues=False)
            c = QColor(color)
            fill = QColor(c)
            fill.setAlpha(40)
            self.trend_curves[k] = pi.plot(pen=pg.mkPen(c, width=2), fillLevel=0, fillBrush=pg.mkBrush(fill))
            if first is None:
                first = pi
                pi.setXRange(-60, 0, padding=0)
            else:
                pi.setXLink(first)
        return self.trend

    def build_spectrum_page(self):
        self.spec = pg.PlotWidget()
        self.spec.setBackground(PLOT_BG)
        self.spec.setMouseEnabled(False, False)
        pi = self.spec.getPlotItem()
        style_pi(pi, "幅度 (dBV)", "频率 (Hz)")
        pi.setXRange(0, 2000, padding=0)
        pi.setYRange(-70, 40, padding=0)
        fill = QColor(CYAN)
        fill.setAlpha(50)
        self.spec_curve = pi.plot(pen=pg.mkPen(CYAN, width=2), fillLevel=-70, fillBrush=pg.mkBrush(fill))
        self.spec_peak = pg.TextItem(color="#FACC15", anchor=(0.5, 1.3))
        pi.addItem(self.spec_peak)
        return self.spec

    def build_right(self):
        tabs = QTabWidget()
        tabs.setDocumentMode(True)
        tabs.setFixedWidth(372)
        self.ctrl_page = self.build_control_page()
        self.preset_page = PresetPage(self)
        self.seq_page = SequencePage(self)
        self.alarm_page = AlarmPage(self)
        tabs.addTab(scroll_page(self.ctrl_page), "控制")
        tabs.addTab(scroll_page(self.preset_page), "预设")
        tabs.addTab(scroll_page(self.seq_page), "序列")
        tabs.addTab(scroll_page(self.alarm_page), "报警")
        self.right_tabs = tabs
        return tabs

    def build_control_page(self):
        page = QWidget()
        lay = QVBoxLayout(page)
        lay.setContentsMargins(0, 0, 6, 0)
        lay.setSpacing(12)

        c1, l1 = card("通道 / 模式")
        ch = QHBoxLayout()
        self.ch_btns = []
        for i in range(4):
            b = QPushButton(f"CH{i + 1}")
            b.setObjectName("seg")
            b.setCheckable(True)
            b.setChecked(i == 0)
            b.setToolTip(f"CH{i + 1} · {CH_NAMES[i]}")
            b.setStyleSheet(f"QPushButton {{ color: {CH_COLORS[i]}; }}")
            b.toggled.connect(lambda on, i=i: self.log(f"CH{i + 1}（{CH_NAMES[i]}）{'开启' if on else '关闭'}"))
            ch.addWidget(b)
            self.ch_btns.append(b)
        l1.addLayout(ch)
        mode = QHBoxLayout()
        self.mode_group = QButtonGroup(self)
        self.mode_btns = []
        for i, name in enumerate(MODE_NAMES):
            b = QPushButton(name)
            b.setObjectName("seg")
            b.setCheckable(True)
            b.setChecked(i == 0)
            self.mode_group.addButton(b)
            b.clicked.connect(lambda _=False, n=name: self.log(f"工作模式切换为 {n}"))
            mode.addWidget(b)
            self.mode_btns.append(b)
        l1.addLayout(mode)
        lay.addWidget(c1)

        c2, l2 = card("参数设置")
        l2.addWidget(QLabel("输出电压 (V)"))
        vr = QHBoxLayout()
        self.v_slider = QSlider(Qt.Orientation.Horizontal)
        self.v_slider.setRange(0, 300)
        self.v_slider.setValue(120)
        self.v_spin = QDoubleSpinBox()
        self.v_spin.setRange(0, 30)
        self.v_spin.setDecimals(1)
        self.v_spin.setSingleStep(0.1)
        self.v_spin.setValue(12.0)
        self.v_spin.setFixedWidth(84)
        vr.addWidget(self.v_slider, 1)
        vr.addWidget(self.v_spin)
        l2.addLayout(vr)
        self.v_slider.valueChanged.connect(self.on_v_slider)
        self.v_spin.valueChanged.connect(self.on_v_spin)
        self.v_slider.sliderReleased.connect(lambda: self.log(f"设定电压 {self.v_spin.value():.1f} V"))
        self.v_spin.editingFinished.connect(lambda: self.log(f"设定电压 {self.v_spin.value():.1f} V"))

        r2 = QHBoxLayout()
        a, b = QVBoxLayout(), QVBoxLayout()
        a.addWidget(QLabel("波形类型"))
        self.kind = QComboBox()
        self.kind.addItems(["正弦波", "方波", "三角波", "锯齿波"])
        self.kind.currentTextChanged.connect(lambda t: self.log(f"波形类型：{t}"))
        a.addWidget(self.kind)
        b.addWidget(QLabel("频率 (Hz)"))
        self.freq = QSpinBox()
        self.freq.setRange(1, 1000)
        self.freq.setValue(50)
        self.freq.editingFinished.connect(lambda: self.log(f"设定频率 {self.freq.value()} Hz"))
        b.addWidget(self.freq)
        r2.addLayout(a, 1)
        r2.addLayout(b, 1)
        l2.addLayout(r2)

        l2.addWidget(QLabel("电流限制 (A)"))
        dr = QHBoxLayout()
        self.dial = QDial()
        self.dial.setRange(0, 50)
        self.dial.setValue(30)
        self.dial.setNotchesVisible(True)
        self.dial.setFixedSize(84, 84)
        self.dial_val = QLabel("3.0 A")
        self.dial_val.setObjectName("dialValue")
        self.dial.valueChanged.connect(lambda v: self.dial_val.setText(f"{v / 10:.1f} A"))
        self.dial.sliderReleased.connect(lambda: self.log(f"电流限制 {self.dial.value() / 10:.1f} A"))
        dr.addStretch(1)
        dr.addWidget(self.dial)
        dr.addSpacing(10)
        dr.addWidget(self.dial_val)
        dr.addStretch(1)
        l2.addLayout(dr)
        lay.addWidget(c2)

        c3, l3 = card("输出控制")
        br = QHBoxLayout()
        self.btn_start = QPushButton("▶ 启动输出")
        self.btn_start.setObjectName("start")
        self.btn_stop = QPushButton("■ 停止")
        self.btn_stop.setObjectName("stop")
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        br.addWidget(self.btn_start)
        br.addWidget(self.btn_stop)
        l3.addLayout(br)
        self.btn_estop = QPushButton("🛑 紧急停止  (Esc)")
        self.btn_estop.setObjectName("estop")
        self.btn_estop.clicked.connect(lambda: self.on_estop())
        l3.addWidget(self.btn_estop)
        reset = QPushButton("恢复默认参数")
        reset.clicked.connect(lambda: (self.apply_params(BUILTIN_PRESETS["出厂默认"]), self.log("参数已恢复默认")))
        l3.addWidget(reset)
        lay.addWidget(c3)
        lay.addStretch(1)
        return page

    def build_log(self):
        c, l = card()
        head = QHBoxLayout()
        t = QLabel("通讯日志")
        t.setObjectName("cardTitle")
        head.addWidget(t)
        head.addStretch(1)
        b1, b2 = QPushButton("导出"), QPushButton("清空")
        for b in (b1, b2):
            b.setObjectName("tool")
            head.addWidget(b)
        b1.clicked.connect(self.export_log)
        l.addLayout(head)
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setFixedHeight(92)
        self.log_view.setMaximumBlockCount(1000)
        b2.clicked.connect(self.log_view.clear)
        l.addWidget(self.log_view)
        return c

    # ================================================== 日志 / 状态
    def log(self, msg, color="#9FB3D9"):
        ts = QDateTime.currentDateTime().toString("HH:mm:ss")
        self.log_view.appendHtml(f'<span style="color:#4B587A">[{ts}]</span> <span style="color:{color}">{msg}</span>')

    def toast(self, msg):
        self.statusBar().showMessage(msg, 5000)

    def export_log(self):
        path, _ = QFileDialog.getSaveFileName(self, "导出日志", "instrument_log.txt", "文本文件 (*.txt)")
        if path:
            try:
                Path(path).write_text(self.log_view.toPlainText(), encoding="utf-8")
                self.toast(f"日志已导出：{path}")
            except OSError as e:
                QMessageBox.warning(self, "导出失败", str(e))

    def set_state_ui(self, text, color):
        self.top_led.set_color(color)
        self.top_state.setText(text)
        self.top_state.setStyleSheet(f"color: {color};")

    def set_connected(self, on):
        self.connected = on
        self.ctrl_page.setEnabled(on)
        self.btn_connect.setText("断开连接" if on else "连接设备")
        self.port.setEnabled(not on)
        self.baud.setEnabled(not on)
        if on:
            self.info["型号"].setText("XD-3030 可编程电源")
            self.info["序列号"].setText("XD3030-240815")
            self.info["固件版本"].setText("v2.4.1")
            self.set_state_ui("已连接 · 待机", GREEN)
        else:
            for k in ("型号", "序列号", "固件版本"):
                self.info[k].setText("--")
            self.set_state_ui("未连接", GRAY)
        self.update_run_buttons()

    def update_run_buttons(self):
        self.btn_start.setEnabled(self.connected and not self.running)
        self.btn_stop.setEnabled(self.connected and self.running)
        self.btn_estop.setEnabled(self.connected)

    # ================================================== 参数 / 预设
    def mode_index(self):
        return next((i for i, b in enumerate(self.mode_btns) if b.isChecked()), 0)

    def collect_params(self):
        return dict(voltage=round(self.v_spin.value(), 1), current=self.dial.value() / 10,
                    kind=self.kind.currentText(), freq=self.freq.value(),
                    channels=[b.isChecked() for b in self.ch_btns], mode=self.mode_index())

    def apply_params(self, d):
        self.v_spin.setValue(float(d.get("voltage", 12.0)))
        self.dial.setValue(int(round(float(d.get("current", 3.0)) * 10)))
        self.kind.setCurrentText(d.get("kind", "正弦波"))
        self.freq.setValue(int(d.get("freq", 50)))
        for b, on in zip(self.ch_btns, d.get("channels", [True, False, False, False])):
            b.setChecked(bool(on))
        self.mode_btns[min(int(d.get("mode", 0)), 2)].setChecked(True)

    def on_v_slider(self, v):
        self.v_spin.blockSignals(True)
        self.v_spin.setValue(v / 10)
        self.v_spin.blockSignals(False)

    def on_v_spin(self, v):
        self.v_slider.blockSignals(True)
        self.v_slider.setValue(int(round(v * 10)))
        self.v_slider.blockSignals(False)

    # ================================================== 连接 / 输出
    def toggle_connect(self):
        if self.connected:
            self.seq_page.stop(silent=True)
            if self.running:
                self.on_stop()
            self.set_connected(False)
            self.log("设备已断开", AMBER)
            self.toast("设备已断开")
            return
        self.btn_connect.setEnabled(False)
        self.btn_connect.setText("连接中…")
        self.set_state_ui("连接中…", AMBER)
        self.log(f"正在连接 {self.port.currentText()} @ {self.baud.currentText()} …")

        def done():
            self.btn_connect.setEnabled(True)
            self.set_connected(True)
            self.log("连接成功，已读取设备信息", GREEN)
            self.toast("设备已连接")
        QTimer.singleShot(700, done)

    def toggle_output(self):
        if not self.connected:
            return
        self.on_stop() if self.running else self.on_start()

    def on_start(self):
        if not self.connected or self.running:
            return
        self.running = True
        self.set_state_ui("输出中", CYAN)
        self.update_run_buttons()
        self.log(f"输出已启动：{self.v_spin.value():.1f} V / 限流 {self.dial.value() / 10:.1f} A", GREEN)

    def on_stop(self):
        if not self.running:
            return
        self.running = False
        self.seq_page.stop(silent=True)
        self.set_state_ui("已连接 · 待机", GREEN)
        self.update_run_buttons()
        self.log("输出已停止", AMBER)

    def on_estop(self, reason="手动"):
        if not self.connected:
            return
        self.running = False
        self.seq_page.stop(silent=True)
        self.set_state_ui("急停触发" if reason == "手动" else "报警跳闸", RED)
        self.update_run_buttons()
        self.log(f"!! {'紧急停止已触发' if reason == '手动' else reason}，输出已切断 !!", RED)
        self.toast("输出已切断")

    def inject(self, kind):
        self.inject_until[kind] = time.monotonic() + 8
        self.log(f"已注入故障：{'过温' if kind == 'temp' else '过载'}（8 秒）", AMBER)

    # ================================================== 示波器 / 光标 / 录制
    def apply_scope_range(self, *_):
        self.tdiv = float(self.tbase.currentText().split()[0])
        T = 10 * self.tdiv
        self.scope_t = np.linspace(0, T, 900)
        pi = self.scope.getPlotItem()
        pi.setXRange(0, T, padding=0)
        sel = self.vscale.currentText()
        if sel == "自动":
            pi.enableAutoRange(axis="y", enable=True)
        else:
            r = float(sel.replace("±", "").replace(" V", ""))
            pi.setYRange(-r, r, padding=0)
        self.cur_a.setValue(3 * self.tdiv)
        self.cur_b.setValue(7 * self.tdiv)

    def on_freeze(self, on):
        self.frozen = on
        self.btn_freeze.setText("▶ 继续" if on else "⏸ 冻结")
        self.log("画面已冻结" if on else "画面已恢复")

    def on_cursor(self, on):
        for c in (self.cur_a, self.cur_b):
            c.setVisible(on)
        self.update_cursor_info()

    def update_cursor_info(self, *_):
        if not self.btn_cursor.isChecked():
            self.meas["dt"].setText("--")
            return
        dt = abs(self.cur_b.value() - self.cur_a.value())
        self.meas["dt"].setText(f"{dt:.2f} ms / {1000 / dt:.1f} Hz" if dt > 1e-6 else "0 ms")

    def on_snapshot(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "保存截图", f"snapshot_{QDateTime.currentDateTime().toString('yyyyMMdd_HHmmss')}.png", "PNG 图片 (*.png)")
        if path and self.plot_tabs.currentWidget().grab().save(path):
            self.log(f"截图已保存：{path}", GREEN)

    def on_record(self, on):
        if on:
            path, _ = QFileDialog.getSaveFileName(
                self, "选择录制文件", f"record_{QDateTime.currentDateTime().toString('yyyyMMdd_HHmmss')}.csv", "CSV 文件 (*.csv)")
            if not path:
                self.btn_rec.blockSignals(True)
                self.btn_rec.setChecked(False)
                self.btn_rec.blockSignals(False)
                return
            try:
                self.rec_file = open(path, "w", newline="", encoding="utf-8-sig")
            except OSError as e:
                QMessageBox.warning(self, "无法写入", str(e))
                self.btn_rec.blockSignals(True)
                self.btn_rec.setChecked(False)
                self.btn_rec.blockSignals(False)
                return
            self.rec_writer = csv.writer(self.rec_file)
            self.rec_writer.writerow(["时间", "电压(V)", "电流(A)", "功率(W)", "频率(Hz)", "温度(℃)"])
            self.rec_rows = 0
            self.log(f"开始录制 → {path}", RED)
        else:
            self.stop_record()

    def stop_record(self):
        if self.rec_file:
            self.rec_file.close()
            self.log(f"录制结束，共 {self.rec_rows} 行", GREEN)
        self.rec_file = self.rec_writer = None
        self.rec_label.setText("")
        if self.btn_rec.isChecked():
            self.btn_rec.blockSignals(True)
            self.btn_rec.setChecked(False)
            self.btn_rec.blockSignals(False)

    # ================================================== 实时刷新
    def update_scope(self):
        if self.frozen:
            return
        self.frame += 1
        f, v = self.freq.value(), self.v_spin.value()
        il, kind, run = self.dial.value() / 10, self.kind.currentText(), self.running
        t = self.scope_t
        if self.trig.currentIndex() == 0:
            self.phase = (self.phase + 0.006 + f * 0.00002) % 1.0
        else:
            self.phase = random.gauss(0, 0.0008)
        ph = f * t / 1000.0 + self.phase
        a = v if run else 0.0
        ai = min(il, v / 8.0) if run else 0.0
        ys = [a * wave_np(kind, ph), ai * wave_np(kind, ph - 0.06),
              0.3 * a * np.sin(4 * np.pi * ph), 0.5 * a * wave_np("三角波", ph * 0.5)]
        n = len(t)
        for i, y in enumerate(ys):
            y = y + np.random.normal(0, 0.012 + 0.0008 * a, n)
            on = self.ch_btns[i].isChecked()
            self.lines[i].setVisible(on)
            self.glows[i].setVisible(on)
            if on:
                self.lines[i].setData(t, y)
                self.glows[i].setData(t, y)
            if i == 0 and self.frame % 6 == 0:
                self.meas["vpp"].setText(f"{np.ptp(y):.3f} V")
                self.meas["vrms"].setText(f"{math.sqrt(float(np.mean(y * y))):.3f} V")
                self.meas["mean"].setText(f"{float(np.mean(y)):+.3f} V")
                self.meas["freq"].setText(f"{f + random.gauss(0, 0.02):.2f} Hz" if run else "0.00 Hz")

    def update_spectrum(self):
        if self.frozen or self.plot_tabs.currentIndex() != 2:
            return
        fs, n = 8192, 2048
        tt = np.arange(n) / fs
        f, v, run = self.freq.value(), self.v_spin.value(), self.running
        sig = (v if run else 0.0) * wave_np(self.kind.currentText(), f * tt + random.random())
        sig = sig + np.random.normal(0, 0.01, n)
        amp = np.abs(np.fft.rfft(sig * np.hanning(n))) / (n / 4)
        db = 20 * np.log10(amp + 1e-7)
        fr = np.fft.rfftfreq(n, 1 / fs)
        m = fr <= 2000
        self.spec_curve.setData(fr[m], db[m])
        if run:
            k = int(np.argmax(db[m]))
            self.spec_peak.setText(f"{fr[m][k]:.0f} Hz · {db[m][k]:.1f} dBV")
            self.spec_peak.setPos(fr[m][k], db[m][k])
        else:
            self.spec_peak.setText("")

    def refresh_readouts(self):
        now = time.monotonic()
        self.clock.setText(QDateTime.currentDateTime().toString("yyyy-MM-dd  HH:mm:ss"))
        v = i = p = 0.0
        if self.running:
            v = max(0.0, self.v_spin.value() + random.gauss(0, 0.01))
            i = max(0.0, min(self.dial.value() / 10, v / 8.0) + random.gauss(0, 0.004))
            p = v * i
            if now < self.inject_until["power"]:
                p += 140 + random.gauss(0, 2)
                i = p / max(v, 1.0)
            self.run_seconds += 0.5
            self.energy_wh += p * 0.5 / 3600
        target = 28 + p / 150 * 38 + (32 if now < self.inject_until["temp"] else 0)
        self.temp += (target - self.temp) * 0.12 + random.gauss(0, 0.08)
        f = self.freq.value() if self.running else 0.0

        self.reads["v"].setText(f"{v:6.3f}")
        self.reads["i"].setText(f"{i:6.3f}")
        self.reads["p"].setText(f"{p:6.2f}")
        self.reads["f"].setText(f"{f:6.1f}")
        self.gauge_load.set_value(p / 150 * 100)
        self.gauge_temp.set_value(self.temp)
        if self.connected:
            self.info["累计输出"].setText(hms(self.run_seconds))
            self.info["累计能量"].setText(f"{self.energy_wh:.4f} Wh")

        for k, val in (("t", now), ("v", v), ("i", i), ("p", p), ("T", self.temp)):
            self.hist[k].append(val)
        if not self.frozen:
            x = np.array(self.hist["t"]) - now
            for k in ("v", "i", "p", "T"):
                self.trend_curves[k].setData(x, np.array(self.hist[k]))

        if self.rec_writer:
            self.rec_writer.writerow([QDateTime.currentDateTime().toString("yyyy-MM-dd HH:mm:ss"),
                                      f"{v:.4f}", f"{i:.4f}", f"{p:.3f}", f"{f:.1f}", f"{self.temp:.2f}"])
            self.rec_rows += 1
            self.rec_label.setText(f"● REC  {self.rec_rows} 行")

        self.check_alarms(p)

    def check_alarms(self, p):
        now_alarms = set()
        if self.temp > self.alarm_page.t_max.value():
            now_alarms.add("过温")
        if p > self.alarm_page.p_max.value():
            now_alarms.add("过载")
        for name in now_alarms - self.alarms:
            val = f"{self.temp:.1f} ℃ > {self.alarm_page.t_max.value()} ℃" if name == "过温" else f"{p:.1f} W > {self.alarm_page.p_max.value()} W"
            self.alarm_page.add(f"{name}报警：{val}")
            self.log(f"⚠ {name}报警：{val}", RED)
            if self.alarm_page.trip.isChecked() and self.running:
                self.on_estop(f"{name}报警跳闸")
        for name in self.alarms - now_alarms:
            self.alarm_page.add(f"{name}报警已解除", GREEN)
            self.log(f"{name}报警已解除", GREEN)
        self.alarms = now_alarms
        if self.alarms:
            self.blink = not self.blink
            self.banner.setObjectName("alarmOn" if self.blink else "alarmOff")
            self.banner.setStyleSheet("")  # 触发样式刷新
            self.banner.style().unpolish(self.banner)
            self.banner.style().polish(self.banner)
            self.banner.setText("⚠ " + " / ".join(sorted(self.alarms)) + "报警")
            self.banner.show()
        else:
            self.banner.hide()

    def closeEvent(self, e):
        if self.running and QMessageBox.question(self, "确认退出", "设备仍在输出，确定要退出吗？") != QMessageBox.StandardButton.Yes:
            e.ignore()
            return
        self.stop_record()
        e.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(QSS)
    w = Panel()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
