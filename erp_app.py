#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
星辰 ERP —— PyQt6 单文件骨架
================================
运行方式：
    pip install PyQt6
    python erp_app.py

演示账号：admin / 123456（想跳过登录，把下面的 SHOW_LOGIN 改成 False）

结构说明（后续扩展从这里入手）：
    PAGES      业务列表页配置（列、示例数据生成方式），新增模块只需加一项
    MENU       左侧菜单结构，key 对应 PAGES / PLACEHOLDERS / 自定义页面
    ModulePage 通用列表页（搜索/筛选/新增/编辑/删除/导出CSV/详情）
    Dashboard  工作台（KPI、图表、快捷入口、待办）
    THEMES/QSS 浅色 / 深色主题
"""
import sys
import re
import csv
import random
import datetime

from PyQt6.QtCore import Qt, QVariantAnimation, QEasingCurve, QRectF, QTimer, QDateTime
from PyQt6.QtGui import QColor, QPainter, QPen, QLinearGradient, QBrush, QPainterPath, QShortcut, QKeySequence
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QDialog, QFrame, QLabel, QPushButton, QToolButton,
    QLineEdit, QComboBox, QCheckBox, QTableWidget, QTableWidgetItem, QAbstractItemView,
    QHeaderView, QVBoxLayout, QHBoxLayout, QGridLayout, QFormLayout, QTabWidget, QTabBar,
    QTreeWidget, QTreeWidgetItem, QStatusBar, QMenu, QMessageBox, QFileDialog, QScrollArea,
    QListWidget, QListWidgetItem,
)

# ============================================================== 基本配置
APP_NAME = "星辰 ERP"
COMPANY = "星辰科技"
VERSION = "1.0.0"
SHOW_LOGIN = True
DEMO_USER, DEMO_PWD = "admin", "123456"
SIDEBAR_W = 232

# ============================================================== 主题
THEMES = {
    "light": dict(
        bg="#F3F5F9", surface="#FFFFFF", sidebar="#1B2540", sidebar_text="#B8C2DB",
        sidebar_hover="#27345A", primary="#3B6CF6", primary_hover="#2F59D0",
        text="#1F2937", subtext="#6B7280", border="#E5E7EB", header_bg="#F8FAFC",
        alt_row="#FAFBFD", select_bg="#E8EEFF", danger="#DC2626",
    ),
    "dark": dict(
        bg="#0F172A", surface="#1E293B", sidebar="#0B1220", sidebar_text="#94A3B8",
        sidebar_hover="#1A2740", primary="#4F7DFF", primary_hover="#3F6AE6",
        text="#E5E7EB", subtext="#94A3B8", border="#334155", header_bg="#243247",
        alt_row="#1A2539", select_bg="#2A3B66", danger="#EF4444",
    ),
}
PAL = dict(THEMES["light"])

QSS = """
* { font-family: "Microsoft YaHei UI", "PingFang SC", "Noto Sans CJK SC", "Segoe UI", sans-serif; }
QWidget { font-size: 13px; color: %(text)s; }
QMainWindow, QDialog, QMessageBox { background: %(bg)s; }
QToolTip { background: %(surface)s; color: %(text)s; border: 1px solid %(border)s; padding: 4px 8px; }

#sidebar { background: %(sidebar)s; }
#brand { color: #FFFFFF; font-size: 19px; font-weight: bold; padding: 22px 18px 2px 18px; }
#brandSub { color: %(sidebar_text)s; font-size: 11px; padding: 0 18px 14px 18px; }
#version { color: %(sidebar_text)s; font-size: 11px; padding: 6px 18px; }
QTreeWidget#menu { background: transparent; border: none; outline: none; color: %(sidebar_text)s; font-size: 14px; }
QTreeWidget#menu::item { height: 38px; margin: 2px 10px; padding-left: 6px; border-radius: 8px; }
QTreeWidget#menu::item:hover { background: %(sidebar_hover)s; color: #FFFFFF; }
QTreeWidget#menu::item:selected { background: %(primary)s; color: #FFFFFF; }

#topbar { background: %(surface)s; border-bottom: 1px solid %(border)s; }
#crumb { font-size: 14px; }
QToolButton#iconBtn { background: transparent; border: none; border-radius: 8px; padding: 6px 10px; font-size: 16px; }
QToolButton#iconBtn:hover { background: %(select_bg)s; }
QToolButton#userBtn { background: transparent; border: none; border-radius: 8px; padding: 4px 8px; }
QToolButton#userBtn:hover { background: %(select_bg)s; }
QToolButton#userBtn::menu-indicator { image: none; }
#avatar { background: %(primary)s; color: #FFFFFF; border-radius: 16px; font-weight: bold; font-size: 14px; }

QTabWidget::pane { border: none; top: 0px; }
QTabBar { background: transparent; }
QTabBar::tab { background: transparent; color: %(subtext)s; padding: 8px 16px; margin: 0 4px 6px 0; border-radius: 8px; }
QTabBar::tab:selected { background: %(surface)s; color: %(primary)s; font-weight: bold; border: 1px solid %(border)s; }
QTabBar::tab:hover:!selected { background: %(select_bg)s; }

#card { background: %(surface)s; border: 1px solid %(border)s; border-radius: 10px; }
#pageTitle { font-size: 20px; font-weight: bold; }
#cardTitle { font-size: 15px; font-weight: bold; }
#hint { color: %(subtext)s; }
#kpiValue { font-size: 24px; font-weight: bold; }
#bigIcon { font-size: 56px; }

QPushButton { background: %(surface)s; border: 1px solid %(border)s; border-radius: 6px; padding: 6px 16px; }
QPushButton:hover { border-color: %(primary)s; color: %(primary)s; }
QPushButton:pressed { background: %(select_bg)s; }
QPushButton:disabled { color: %(subtext)s; }
QPushButton#primary { background: %(primary)s; border: 1px solid %(primary)s; color: #FFFFFF; }
QPushButton#primary:hover { background: %(primary_hover)s; border-color: %(primary_hover)s; color: #FFFFFF; }
QPushButton#danger { color: %(danger)s; }
QPushButton#danger:hover { background: %(danger)s; border-color: %(danger)s; color: #FFFFFF; }
QPushButton#tile { background: %(bg)s; border-radius: 10px; padding: 14px 8px; }
QPushButton#tile:hover { background: %(select_bg)s; border-color: %(primary)s; color: %(primary)s; }

QLineEdit, QComboBox, QTextEdit { background: %(surface)s; border: 1px solid %(border)s; border-radius: 6px; padding: 6px 10px; selection-background-color: %(primary)s; selection-color: #FFFFFF; }
QLineEdit:focus, QComboBox:focus, QTextEdit:focus { border: 1px solid %(primary)s; }
QLineEdit:read-only { background: %(header_bg)s; }
QComboBox QAbstractItemView { background: %(surface)s; border: 1px solid %(border)s; selection-background-color: %(select_bg)s; selection-color: %(primary)s; outline: none; }
QCheckBox { spacing: 8px; }

QMenu { background: %(surface)s; border: 1px solid %(border)s; border-radius: 8px; padding: 6px; }
QMenu::item { padding: 7px 24px; border-radius: 6px; }
QMenu::item:selected { background: %(select_bg)s; color: %(primary)s; }
QMenu::separator { height: 1px; background: %(border)s; margin: 5px 8px; }

QTableWidget { background: %(surface)s; alternate-background-color: %(alt_row)s; border: none; selection-background-color: %(select_bg)s; outline: none; }
QTableWidget::item { padding: 0 8px; border-bottom: 1px solid %(border)s; }
QHeaderView::section { background: %(header_bg)s; color: %(subtext)s; border: none; border-bottom: 1px solid %(border)s; padding: 10px 8px; font-weight: bold; }
QTableCornerButton::section { background: %(header_bg)s; border: none; }

QListWidget { background: transparent; border: none; outline: none; }
QListWidget::item { padding: 9px 4px; border-bottom: 1px solid %(border)s; }
QListWidget::item:selected { background: transparent; color: %(text)s; }

QScrollArea { background: %(bg)s; border: none; }
#dashInner { background: %(bg)s; }
QScrollBar:vertical { background: transparent; width: 10px; margin: 2px; }
QScrollBar::handle:vertical { background: %(border)s; border-radius: 4px; min-height: 30px; }
QScrollBar::handle:vertical:hover { background: %(subtext)s; }
QScrollBar:horizontal { background: transparent; height: 10px; margin: 2px; }
QScrollBar::handle:horizontal { background: %(border)s; border-radius: 4px; min-width: 30px; }
QScrollBar::add-line, QScrollBar::sub-line { width: 0; height: 0; }
QScrollBar::add-page, QScrollBar::sub-page { background: transparent; }

QStatusBar { background: %(surface)s; border-top: 1px solid %(border)s; color: %(subtext)s; }
QStatusBar::item { border: none; }

#loginLeft { background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #3B6CF6, stop:1 #7C3AED); }
#loginBrand { color: #FFFFFF; font-size: 30px; font-weight: bold; }
#loginSlogan { color: rgba(255,255,255,200); font-size: 14px; }
#loginErr { color: %(danger)s; }
"""


def apply_theme(name):
    PAL.clear()
    PAL.update(THEMES[name])
    QApplication.instance().setStyleSheet(QSS % PAL)


# ============================================================== 示例数据 DSL
TODAY = datetime.date.today()
CUSTOMERS = ["华东机电有限公司", "深圳睿达科技", "北京云帆信息", "杭州启明新材料", "成都锦程贸易",
             "上海恒信实业", "广州天泽电子", "苏州博远精密", "武汉光谷智造", "西安长风能源"]
SUPPLIERS = ["宁波金属材料厂", "东莞精工五金", "无锡鑫达塑业", "佛山华美包装", "天津鼎盛化工",
             "青岛海力电机", "常州新锐电子", "厦门嘉禾轴承"]
STAFF = ["张伟", "王芳", "李娜", "刘洋", "陈静", "杨磊", "赵敏", "黄杰", "周婷", "吴强", "徐丽", "孙浩"]
DEPTS = ["销售部", "采购部", "仓储部", "生产部", "财务部", "人事部", "研发部", "行政部"]
WAREHOUSES = ["上海主仓", "深圳分仓", "成都分仓", "原料仓", "成品仓"]
PRODUCTS = ["不锈钢螺栓 M8", "铝合金型材 6063", "伺服电机 750W", "工业传感器 PS-12", "控制主板 V3",
            "液压油缸 50mm", "轴承 6204", "PLC 模块 FX3U", "变频器 2.2kW", "电源适配器 24V"]
CITIES = ["上海", "深圳", "北京", "杭州", "成都", "广州", "苏州", "武汉", "西安", "南京"]
CATEGORIES = ["五金配件", "电子元器件", "机电设备", "包装材料", "原辅材料", "办公用品"]
UNITS = ["个", "件", "套", "箱", "台", "米"]
EXPENSE_TYPES = ["差旅费", "招待费", "办公费", "交通费", "培训费", "通讯费"]
MODULES_LOG = ["销售管理", "采购管理", "库存管理", "财务管理", "系统管理", "人力资源"]
ACTIONS_LOG = ["登录系统", "新增单据", "修改单据", "审核单据", "导出数据", "删除记录"]

S_ORDER = ["已完成", "待发货", "处理中", "待审核", "已取消"]
S_DOC = ["已审核", "待审核", "草稿", "已驳回"]
S_QUOTE = ["草稿", "已发送", "已确认", "已过期"]
S_PO = ["已入库", "运输中", "部分到货", "待审核", "已取消"]
S_ACTIVE = ["正常", "停用"]
S_STOCKDOC = ["已完成", "待审核", "处理中"]
S_STOCK = ["正常", "预警", "缺货"]
S_PROD = ["进行中", "已完成", "待开工", "已暂停"]
S_AR = ["已收款", "部分收款", "未收款", "逾期"]
S_AP = ["已付款", "部分付款", "未付款", "逾期"]
S_EXP = ["已报销", "待审批", "已驳回"]
S_EMP = ["在职", "试用期", "离职"]
S_PAY = ["已发放", "待发放"]
S_RES = ["成功", "失败"]

GOOD = {"已完成", "已审核", "已入库", "已收款", "已付款", "正常", "在职", "已发放", "成功", "已确认", "已报销"}
WARN = {"待审核", "处理中", "待发货", "运输中", "部分到货", "部分收款", "部分付款", "待审批", "试用期",
        "草稿", "预警", "待开工", "已发送", "待发放", "未收款", "未付款"}
BAD = {"已取消", "已驳回", "停用", "逾期", "缺货", "离职", "失败", "已过期", "已暂停"}
STATUS_NAMES = {"状态", "库存状态", "结果"}


def status_color(text):
    if text in GOOD:
        return "#16A34A"
    if text in WARN:
        return "#D97706"
    if text in BAD:
        return "#DC2626"
    return "#2563EB"


def seq(prefix, width=5, start=1):
    return lambda i, r: f"{prefix}{start + i:0{width}d}"


def pick(pool):
    return lambda i, r: r.choice(pool)


def choice(opts):
    f = lambda i, r: r.choice(opts)
    f.options = list(opts)
    return f


def date(back=90, fwd=0):
    return lambda i, r: (TODAY + datetime.timedelta(days=r.randint(-back, fwd))).isoformat()


def stamp():
    return lambda i, r: (f"{(TODAY - datetime.timedelta(days=r.randint(0, 20))).isoformat()} "
                         f"{r.randint(8, 21):02d}:{r.randint(0, 59):02d}:{r.randint(0, 59):02d}")


def month():
    return lambda i, r: (TODAY.replace(day=1) - datetime.timedelta(days=r.randint(0, 60))).strftime("%Y-%m")


def money(lo, hi):
    f = lambda i, r: f"{r.uniform(lo, hi):,.2f}"
    f.right = True
    return f


def integer(lo, hi):
    f = lambda i, r: str(r.randint(lo, hi))
    f.right = True
    return f


def phone():
    return lambda i, r: f"1{r.choice('358')}{r.randint(0, 10 ** 9 - 1):09d}"


def ip():
    return lambda i, r: f"192.168.{r.randint(0, 9)}.{r.randint(2, 250)}"


def page(title, cols, n=28):
    return {"title": title, "cols": cols, "n": n}


# ============================================================== 业务页面配置
PAGES = {
    # ---- 销售
    "sales_order": page("销售订单", [
        ("订单编号", seq("SO2026", 5)), ("客户", pick(CUSTOMERS)), ("订单日期", date()),
        ("交货日期", date(0, 30)), ("销售员", pick(STAFF)), ("订单金额", money(2000, 180000)),
        ("状态", choice(S_ORDER))]),
    "sales_quote": page("销售报价", [
        ("报价单号", seq("QT2026", 5)), ("客户", pick(CUSTOMERS)), ("报价日期", date()),
        ("有效期至", date(0, 45)), ("报价金额", money(3000, 150000)), ("业务员", pick(STAFF)),
        ("状态", choice(S_QUOTE))]),
    "customer": page("客户管理", [
        ("客户编号", seq("C", 4)), ("客户名称", lambda i, r: CUSTOMERS[i % len(CUSTOMERS)] + ("" if i < len(CUSTOMERS) else f"（{i // len(CUSTOMERS) + 1}部）")),
        ("联系人", pick(STAFF)), ("联系电话", phone()), ("所在城市", pick(CITIES)),
        ("客户等级", choice(["VIP", "A", "B", "C"])), ("信用额度", money(50000, 800000)),
        ("状态", choice(S_ACTIVE))]),
    "sales_return": page("销售退货", [
        ("退货单号", seq("SR2026", 5)), ("原订单号", lambda i, r: f"SO2026{r.randint(1, 60):05d}"),
        ("客户", pick(CUSTOMERS)), ("退货日期", date(60)), ("退货金额", money(500, 20000)),
        ("退货原因", choice(["质量问题", "发错货", "客户取消", "规格不符"])), ("状态", choice(S_DOC))], 18),
    # ---- 采购
    "purchase_request": page("采购申请", [
        ("申请单号", seq("PR2026", 5)), ("申请部门", pick(DEPTS)), ("申请人", pick(STAFF)),
        ("申请日期", date(60)), ("物料", pick(PRODUCTS)), ("数量", integer(10, 2000)),
        ("状态", choice(S_DOC))]),
    "purchase_order": page("采购订单", [
        ("采购单号", seq("PO2026", 5)), ("供应商", pick(SUPPLIERS)), ("采购日期", date()),
        ("预计到货", date(0, 30)), ("采购员", pick(STAFF)), ("采购金额", money(3000, 220000)),
        ("状态", choice(S_PO))]),
    "supplier": page("供应商管理", [
        ("供应商编号", seq("S", 4)), ("供应商名称", lambda i, r: SUPPLIERS[i % len(SUPPLIERS)] + ("" if i < len(SUPPLIERS) else f"（{i // len(SUPPLIERS) + 1}厂）")),
        ("联系人", pick(STAFF)), ("联系电话", phone()), ("主营品类", pick(CATEGORIES)),
        ("合作等级", choice(["战略", "优选", "合格"])), ("状态", choice(S_ACTIVE))], 20),
    "purchase_in": page("采购入库", [
        ("入库单号", seq("PI2026", 5)), ("采购单号", lambda i, r: f"PO2026{r.randint(1, 60):05d}"),
        ("仓库", pick(WAREHOUSES)), ("入库日期", date(60)), ("数量合计", integer(50, 5000)),
        ("经办人", pick(STAFF)), ("状态", choice(S_STOCKDOC))]),
    # ---- 库存
    "inventory": page("库存查询", [
        ("物料编码", seq("M", 5)), ("物料名称", lambda i, r: PRODUCTS[i % len(PRODUCTS)]),
        ("规格型号", lambda i, r: f"{r.choice(['A', 'B', 'C'])}-{r.randint(10, 99)}"),
        ("仓库", pick(WAREHOUSES)), ("库存数量", integer(0, 8000)), ("单位", pick(UNITS)),
        ("安全库存", integer(100, 1000)), ("库存状态", choice(S_STOCK))], 40),
    "stock_in": page("入库单", [
        ("入库单号", seq("IN2026", 5)), ("入库类型", choice(["采购入库", "生产入库", "退货入库", "其他入库"])),
        ("仓库", pick(WAREHOUSES)), ("入库日期", date(60)), ("数量合计", integer(20, 3000)),
        ("经办人", pick(STAFF)), ("状态", choice(S_STOCKDOC))]),
    "stock_out": page("出库单", [
        ("出库单号", seq("OUT2026", 5)), ("出库类型", choice(["销售出库", "领料出库", "报废出库", "其他出库"])),
        ("仓库", pick(WAREHOUSES)), ("出库日期", date(60)), ("数量合计", integer(20, 3000)),
        ("经办人", pick(STAFF)), ("状态", choice(S_STOCKDOC))]),
    "stock_check": page("库存盘点", [
        ("盘点单号", seq("CK2026", 4)), ("仓库", pick(WAREHOUSES)), ("盘点日期", date(90)),
        ("盘点人", pick(STAFF)), ("盘盈数量", integer(0, 30)), ("盘亏数量", integer(0, 40)),
        ("状态", choice(S_STOCKDOC))], 14),
    "stock_transfer": page("库存调拨", [
        ("调拨单号", seq("TR2026", 4)), ("调出仓库", pick(WAREHOUSES)), ("调入仓库", pick(WAREHOUSES)),
        ("调拨日期", date(60)), ("数量合计", integer(10, 1500)), ("经办人", pick(STAFF)),
        ("状态", choice(S_STOCKDOC))], 16),
    # ---- 生产
    "production_plan": page("生产计划", [
        ("计划编号", seq("PP2026", 4)), ("产品名称", pick(PRODUCTS)), ("计划数量", integer(100, 5000)),
        ("开始日期", date(20, 10)), ("结束日期", date(0, 45)), ("负责人", pick(STAFF)),
        ("状态", choice(S_PROD))], 18),
    "production_order": page("生产工单", [
        ("工单号", seq("MO2026", 5)), ("产品名称", pick(PRODUCTS)), ("计划数量", integer(100, 5000)),
        ("已完成数量", integer(0, 3000)), ("开工日期", date(30)), ("车间", choice(["一车间", "二车间", "装配车间"])),
        ("状态", choice(S_PROD))]),
    "bom": page("BOM 物料清单", [
        ("BOM编号", seq("BOM", 4)), ("产品名称", lambda i, r: PRODUCTS[i % len(PRODUCTS)]),
        ("版本", lambda i, r: f"V{r.randint(1, 3)}.{r.randint(0, 9)}"), ("物料数量", integer(3, 40)),
        ("单位成本", money(20, 3000)), ("创建人", pick(STAFF)), ("状态", choice(S_ACTIVE))], 20),
    # ---- 财务
    "receivable": page("应收账款", [
        ("单据号", seq("AR2026", 5)), ("客户", pick(CUSTOMERS)), ("应收金额", money(5000, 200000)),
        ("已收金额", money(0, 100000)), ("到期日", date(30, 60)), ("负责人", pick(STAFF)),
        ("状态", choice(S_AR))]),
    "payable": page("应付账款", [
        ("单据号", seq("AP2026", 5)), ("供应商", pick(SUPPLIERS)), ("应付金额", money(5000, 200000)),
        ("已付金额", money(0, 100000)), ("到期日", date(30, 60)), ("负责人", pick(STAFF)),
        ("状态", choice(S_AP))]),
    "expense": page("费用报销", [
        ("报销单号", seq("EX2026", 5)), ("申请人", pick(STAFF)), ("部门", pick(DEPTS)),
        ("费用类型", pick(EXPENSE_TYPES)), ("报销金额", money(50, 8000)), ("申请日期", date(45)),
        ("状态", choice(S_EXP))]),
    "voucher": page("会计凭证", [
        ("凭证号", seq("记-", 4)), ("凭证日期", date(60)),
        ("摘要", choice(["销售收入", "采购入库", "计提工资", "支付货款", "报销费用", "收到货款"])),
        ("借方金额", money(1000, 150000)), ("贷方金额", money(1000, 150000)), ("制单人", pick(STAFF)),
        ("状态", choice(S_DOC))]),
    # ---- 人事
    "employee": page("员工档案", [
        ("工号", seq("E", 4, 1001)), ("姓名", lambda i, r: STAFF[i % len(STAFF)] + ("" if i < len(STAFF) else str(i // len(STAFF) + 1))),
        ("部门", pick(DEPTS)), ("岗位", choice(["专员", "主管", "经理", "工程师", "助理"])),
        ("入职日期", date(1500)), ("联系电话", phone()), ("学历", choice(["本科", "硕士", "大专", "博士"])),
        ("状态", choice(S_EMP))], 30),
    "attendance": page("考勤管理", [
        ("工号", seq("E", 4, 1001)), ("姓名", lambda i, r: STAFF[i % len(STAFF)]), ("部门", pick(DEPTS)),
        ("月份", month()), ("出勤天数", integer(17, 23)), ("迟到次数", integer(0, 5)),
        ("请假天数", integer(0, 4)), ("加班小时", integer(0, 36))], 24),
    "payroll": page("薪资管理", [
        ("工资单号", seq("PY2026", 4)), ("姓名", pick(STAFF)), ("部门", pick(DEPTS)),
        ("基本工资", money(5000, 18000)), ("绩效奖金", money(500, 6000)), ("扣款", money(0, 800)),
        ("实发工资", money(5000, 22000)), ("状态", choice(S_PAY))], 24),
    # ---- 基础资料
    "product": page("商品档案", [
        ("商品编码", seq("G", 5)), ("商品名称", lambda i, r: PRODUCTS[i % len(PRODUCTS)]),
        ("分类", pick(CATEGORIES)), ("规格", lambda i, r: f"{r.choice(['标准', '加强', '精密'])}型"),
        ("单位", pick(UNITS)), ("参考单价", money(5, 5000)), ("状态", choice(S_ACTIVE))], 36),
    "warehouse": page("仓库管理", [
        ("仓库编码", seq("WH", 3)), ("仓库名称", lambda i, r: WAREHOUSES[i % len(WAREHOUSES)]),
        ("负责人", pick(STAFF)), ("所在城市", pick(CITIES)), ("面积(㎡)", integer(300, 6000)),
        ("状态", choice(S_ACTIVE))], 5),
    "department": page("部门管理", [
        ("部门编码", seq("D", 3)), ("部门名称", lambda i, r: DEPTS[i % len(DEPTS)]),
        ("负责人", pick(STAFF)), ("人数", integer(3, 60)), ("上级部门", choice(["总经办", "运营中心", "研发中心"])),
        ("状态", choice(S_ACTIVE))], 8),
    # ---- 系统
    "user": page("用户管理", [
        ("账号", lambda i, r: f"user{i + 1:03d}"), ("姓名", pick(STAFF)),
        ("角色", choice(["管理员", "销售", "采购", "仓管", "财务", "人事"])), ("部门", pick(DEPTS)),
        ("最后登录", stamp()), ("状态", choice(S_ACTIVE))], 20),
    "role": page("角色权限", [
        ("角色编码", seq("R", 3)), ("角色名称", lambda i, r: ["管理员", "销售", "采购", "仓管", "财务", "人事"][i % 6]),
        ("用户数", integer(1, 30)), ("描述", choice(["拥有该模块全部权限", "仅查看与新增", "只读权限"])),
        ("创建时间", date(400)), ("状态", choice(S_ACTIVE))], 6),
    "log": page("操作日志", [
        ("时间", stamp()), ("用户", pick(STAFF)), ("模块", pick(MODULES_LOG)),
        ("操作", pick(ACTIONS_LOG)), ("IP地址", ip()), ("结果", choice(S_RES))], 60),
}

# 尚未实现、仅放占位页的菜单
PLACEHOLDERS = {"rpt_sales": "📈", "rpt_stock": "📊", "rpt_finance": "💹"}

# 左侧菜单：(图标, 分组名, [(页面key, 页面标题), ...])
MENU = [
    ("🛒", "销售管理", [("sales_order", "销售订单"), ("sales_quote", "销售报价"),
                       ("customer", "客户管理"), ("sales_return", "销售退货")]),
    ("📥", "采购管理", [("purchase_request", "采购申请"), ("purchase_order", "采购订单"),
                       ("supplier", "供应商管理"), ("purchase_in", "采购入库")]),
    ("📦", "库存管理", [("inventory", "库存查询"), ("stock_in", "入库单"), ("stock_out", "出库单"),
                       ("stock_check", "库存盘点"), ("stock_transfer", "库存调拨")]),
    ("🏭", "生产管理", [("production_plan", "生产计划"), ("production_order", "生产工单"),
                       ("bom", "BOM 物料清单")]),
    ("💰", "财务管理", [("receivable", "应收账款"), ("payable", "应付账款"),
                       ("expense", "费用报销"), ("voucher", "会计凭证")]),
    ("👥", "人力资源", [("employee", "员工档案"), ("attendance", "考勤管理"), ("payroll", "薪资管理")]),
    ("🗂", "基础资料", [("product", "商品档案"), ("warehouse", "仓库管理"), ("department", "部门管理")]),
    ("📑", "报表中心", [("rpt_sales", "销售报表"), ("rpt_stock", "库存报表"), ("rpt_finance", "财务报表")]),
    ("⚙", "系统管理", [("user", "用户管理"), ("role", "角色权限"), ("log", "操作日志"),
                       ("settings", "系统设置")]),
]


def gen_rows(key):
    spec = PAGES[key]
    rnd = random.Random(key)
    return [[str(gen(i, rnd)) for _, gen in spec["cols"]] for i in range(spec["n"])]


def next_code(codes):
    best = None
    for c in codes:
        m = re.match(r"^(\D*)(\d+)$", c)
        if m and (best is None or int(m.group(2)) > best[1]):
            best = (m.group(1), int(m.group(2)), len(m.group(2)))
    return f"{best[0]}{best[1] + 1:0{best[2]}d}" if best else ""


# ============================================================== 通用小组件
def make_card(parent=None):
    f = QFrame(parent)
    f.setObjectName("card")
    return f


class SortItem(QTableWidgetItem):
    """数字按数值排序，其余按文本排序。"""

    def __lt__(self, other):
        a, b = self.text(), other.text()
        try:
            return float(a.replace(",", "")) < float(b.replace(",", ""))
        except ValueError:
            return a < b


class BarChart(QWidget):
    def __init__(self, labels, values):
        super().__init__()
        self.labels, self.values = labels, values
        self.setMinimumHeight(230)

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        w, h = self.width(), self.height()
        left, right, top, bottom = 44, 12, 16, 30
        cw, ch = w - left - right, h - top - bottom
        vmax = max(self.values) * 1.2
        grid, sub = QColor(PAL["border"]), QColor(PAL["subtext"])
        AR = Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        AC = Qt.AlignmentFlag.AlignCenter
        for k in range(5):
            y = top + ch * k / 4
            p.setPen(QPen(grid, 1))
            p.drawLine(left, int(y), int(w - right), int(y))
            p.setPen(sub)
            p.drawText(QRectF(0, y - 8, left - 8, 16), AR, f"{vmax * (4 - k) / 4:.0f}")
        n = len(self.values)
        slot = cw / n
        bw = min(38, slot * 0.5)
        for i, v in enumerate(self.values):
            x = left + slot * i + (slot - bw) / 2
            bh = ch * v / vmax
            y = top + ch - bh
            g = QLinearGradient(0, y, 0, y + bh)
            g.setColorAt(0, QColor(PAL["primary"]))
            g.setColorAt(1, QColor(PAL["primary"]).lighter(160))
            path = QPainterPath()
            path.addRoundedRect(QRectF(x, y, bw, bh), 5, 5)
            p.fillPath(path, QBrush(g))
            p.setPen(QColor(PAL["text"]))
            p.drawText(QRectF(x - 12, y - 19, bw + 24, 16), AC, f"{v:.0f}")
            p.setPen(sub)
            p.drawText(QRectF(left + slot * i, h - bottom + 8, slot, 16), AC, self.labels[i])
        p.end()


class DonutChart(QWidget):
    def __init__(self, data):
        super().__init__()
        self.data = data  # [(label, value, color)]
        self.setMinimumHeight(230)

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        w, h = self.width(), self.height()
        s = min(h - 24, w * 0.5)
        pw = s * 0.17
        rect = QRectF(12 + pw / 2, (h - s) / 2 + pw / 2, s - pw, s - pw)
        total = sum(v for _, v, _ in self.data)
        start = 90 * 16
        for _, v, c in self.data:
            span = -v / total * 360 * 16
            pen = QPen(QColor(c), pw)
            pen.setCapStyle(Qt.PenCapStyle.FlatCap)
            p.setPen(pen)
            p.drawArc(rect, int(start), int(span))
            start += span
        AC = Qt.AlignmentFlag.AlignCenter
        p.setPen(QColor(PAL["text"]))
        f = p.font()
        f.setPointSize(17)
        f.setBold(True)
        p.setFont(f)
        p.drawText(rect, AC, str(total))
        f.setPointSize(9)
        f.setBold(False)
        p.setFont(f)
        p.setPen(QColor(PAL["subtext"]))
        p.drawText(QRectF(rect.x(), rect.center().y() + 14, rect.width(), 18), AC, "订单总数")
        x0 = 12 + s + 22
        y0 = (h - len(self.data) * 28) / 2
        for i, (label, v, c) in enumerate(self.data):
            y = y0 + i * 28
            p.setBrush(QColor(c))
            p.setPen(Qt.PenStyle.NoPen)
            p.drawRoundedRect(QRectF(x0, y + 5, 11, 11), 3, 3)
            p.setPen(QColor(PAL["text"]))
            p.drawText(QRectF(x0 + 18, y, w - x0 - 18, 22),
                       Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                       f"{label}  {v}（{v / total * 100:.0f}%）")
        p.end()


# ============================================================== 弹窗
class RecordDialog(QDialog):
    """新增 / 编辑通用表单（按列自动生成）。"""

    def __init__(self, title, columns, status_idx, status_opts, values=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setMinimumWidth(660)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(24, 22, 24, 20)
        lay.setSpacing(16)
        head = QLabel(title)
        head.setObjectName("pageTitle")
        lay.addWidget(head)
        body = QHBoxLayout()
        body.setSpacing(30)
        forms = [QFormLayout(), QFormLayout()]
        for f in forms:
            f.setSpacing(14)
            body.addLayout(f, 1)
        self.editors = []
        for i, c in enumerate(columns):
            if i == status_idx and status_opts:
                ed = QComboBox()
                ed.addItems(status_opts)
                if values:
                    ed.setCurrentText(values[i])
            else:
                ed = QLineEdit(values[i] if values else "")
            self.editors.append(ed)
            forms[i % 2].addRow(c, ed)
        lay.addLayout(body)
        btns = QHBoxLayout()
        btns.addStretch(1)
        cancel, ok = QPushButton("取消"), QPushButton("保存")
        ok.setObjectName("primary")
        cancel.clicked.connect(self.reject)
        ok.clicked.connect(self.accept)
        btns.addWidget(cancel)
        btns.addWidget(ok)
        lay.addLayout(btns)

    def values(self):
        return [e.currentText() if isinstance(e, QComboBox) else e.text().strip() for e in self.editors]


class DetailDialog(QDialog):
    """详情弹窗骨架：基本信息 / 明细 / 操作记录（后两者待开发）。"""

    def __init__(self, title, columns, values, parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"{title}详情")
        self.resize(760, 520)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(24, 22, 24, 20)
        lay.setSpacing(14)
        head = QLabel(f"{title}详情 · {values[0]}")
        head.setObjectName("pageTitle")
        lay.addWidget(head)
        tabs = QTabWidget()
        info = make_card()
        grid = QGridLayout(info)
        grid.setContentsMargins(24, 20, 24, 20)
        grid.setHorizontalSpacing(30)
        grid.setVerticalSpacing(18)
        for i, (c, v) in enumerate(zip(columns, values)):
            k = QLabel(c)
            k.setObjectName("hint")
            val = QLabel(v or "-")
            val.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            col = (i % 2) * 2
            grid.addWidget(k, i // 2 * 2, col)
            grid.addWidget(val, i // 2 * 2 + 1, col)
        grid.setRowStretch(grid.rowCount(), 1)
        tabs.addTab(info, "基本信息")
        for name in ("明细", "操作记录"):
            ph = make_card()
            pl = QVBoxLayout(ph)
            t = QLabel(f"「{name}」内容待开发")
            t.setObjectName("hint")
            t.setAlignment(Qt.AlignmentFlag.AlignCenter)
            pl.addWidget(t)
            tabs.addTab(ph, name)
        lay.addWidget(tabs, 1)
        row = QHBoxLayout()
        row.addStretch(1)
        close = QPushButton("关闭")
        close.clicked.connect(self.accept)
        row.addWidget(close)
        lay.addLayout(row)


class LoginDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"登录 · {APP_NAME}")
        self.setFixedSize(760, 440)
        self.username = DEMO_USER
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)
        left = QFrame()
        left.setObjectName("loginLeft")
        left.setFixedWidth(300)
        ll = QVBoxLayout(left)
        ll.setContentsMargins(34, 0, 34, 0)
        ll.addStretch(1)
        b = QLabel(f"◆ {COMPANY}")
        b.setObjectName("loginBrand")
        s = QLabel("企业资源计划系统\n销售 · 采购 · 库存 · 生产 · 财务")
        s.setObjectName("loginSlogan")
        ll.addWidget(b)
        ll.addSpacing(8)
        ll.addWidget(s)
        ll.addStretch(2)
        root.addWidget(left)

        right = QWidget()
        rl = QVBoxLayout(right)
        rl.setContentsMargins(52, 0, 52, 0)
        rl.setSpacing(12)
        rl.addStretch(1)
        t = QLabel("欢迎登录")
        t.setObjectName("pageTitle")
        rl.addWidget(t)
        h = QLabel(f"演示账号：{DEMO_USER} / {DEMO_PWD}")
        h.setObjectName("hint")
        rl.addWidget(h)
        rl.addSpacing(8)
        self.user = QLineEdit(DEMO_USER)
        self.user.setPlaceholderText("账号")
        self.pwd = QLineEdit(DEMO_PWD)
        self.pwd.setPlaceholderText("密码")
        self.pwd.setEchoMode(QLineEdit.EchoMode.Password)
        for e in (self.user, self.pwd):
            e.setMinimumHeight(38)
            e.returnPressed.connect(self.try_login)
            rl.addWidget(e)
        self.err = QLabel("")
        self.err.setObjectName("loginErr")
        rl.addWidget(self.err)
        btn = QPushButton("登 录")
        btn.setObjectName("primary")
        btn.setMinimumHeight(40)
        btn.clicked.connect(self.try_login)
        rl.addWidget(btn)
        rl.addStretch(2)
        root.addWidget(right, 1)

    def try_login(self):
        if self.user.text().strip() == DEMO_USER and self.pwd.text() == DEMO_PWD:
            self.username = self.user.text().strip()
            self.accept()
        else:
            self.err.setText("账号或密码错误")


# ============================================================== 通用列表页
class ModulePage(QWidget):
    def __init__(self, key, spec, main):
        super().__init__()
        self.main, self.spec = main, spec
        self.title = spec["title"]
        self.columns = [c for c, _ in spec["cols"]]
        self.right_cols = {i for i, (_, g) in enumerate(spec["cols"]) if getattr(g, "right", False)}
        self.status_idx = next((i for i, c in enumerate(self.columns) if c in STATUS_NAMES), None)
        self.status_opts = getattr(spec["cols"][self.status_idx][1], "options", []) if self.status_idx is not None else []

        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 0)
        root.setSpacing(12)

        # 条件栏
        bar = make_card()
        bl = QHBoxLayout(bar)
        bl.setContentsMargins(16, 12, 16, 12)
        bl.setSpacing(10)
        self.search = QLineEdit()
        self.search.setPlaceholderText(f"搜索{self.columns[0]}、{self.columns[1]}…")
        self.search.setClearButtonEnabled(True)
        self.search.setFixedWidth(260)
        self.search.textChanged.connect(self.apply_filter)
        bl.addWidget(self.search)
        self.status_box = None
        if self.status_opts:
            self.status_box = QComboBox()
            self.status_box.addItems(["全部"] + self.status_opts)
            self.status_box.setMinimumWidth(110)
            self.status_box.currentIndexChanged.connect(self.apply_filter)
            bl.addWidget(QLabel(self.columns[self.status_idx] + "："))
            bl.addWidget(self.status_box)
        reset = QPushButton("重置")
        reset.clicked.connect(self.reset_filter)
        bl.addWidget(reset)
        bl.addStretch(1)
        for text, name, slot in (("＋ 新增", "primary", self.on_add), ("编辑", "", self.on_edit),
                                 ("删除", "danger", self.on_delete), ("导入", "", self.on_import),
                                 ("导出", "", self.on_export)):
            b = QPushButton(text)
            if name:
                b.setObjectName(name)
            b.clicked.connect(slot)
            bl.addWidget(b)
        root.addWidget(bar)

        # 表格
        box = make_card()
        vl = QVBoxLayout(box)
        vl.setContentsMargins(1, 1, 1, 0)
        vl.setSpacing(0)
        self.table = QTableWidget(0, len(self.columns))
        self.table.setHorizontalHeaderLabels(self.columns)
        self.table.setAlternatingRowColors(True)
        self.table.setShowGrid(False)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(42)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setMinimumSectionSize(80)
        self.table.setFrameShape(QFrame.Shape.NoFrame)
        self.table.cellDoubleClicked.connect(lambda r, _c: self.on_detail(r))
        self.table.itemSelectionChanged.connect(self.update_footer)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.on_menu)
        vl.addWidget(self.table, 1)
        foot = QWidget()
        fl = QHBoxLayout(foot)
        fl.setContentsMargins(16, 8, 16, 8)
        self.footer = QLabel()
        self.footer.setObjectName("hint")
        fl.addWidget(self.footer)
        fl.addStretch(1)
        tip = QLabel("双击行查看详情 · 点击表头排序 · 右键更多操作")
        tip.setObjectName("hint")
        fl.addWidget(tip)
        vl.addWidget(foot)
        root.addWidget(box, 1)

        self.table.setSortingEnabled(False)
        for row in gen_rows(self.key_of(spec)):
            self.append_row(row)
        self.table.setSortingEnabled(True)
        self.update_footer()

    @staticmethod
    def key_of(spec):
        for k, v in PAGES.items():
            if v is spec:
                return k

    # ---- 数据
    def fill_row(self, r, values):
        for c, v in enumerate(values):
            it = SortItem(("● " + v) if c == self.status_idx else v)
            if c == self.status_idx:
                it.setData(Qt.ItemDataRole.UserRole, v)
                it.setForeground(QColor(status_color(v)))
            if c in self.right_cols:
                it.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            else:
                it.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(r, c, it)

    def append_row(self, values):
        r = self.table.rowCount()
        self.table.insertRow(r)
        self.fill_row(r, values)

    def row_values(self, r):
        out = []
        for c in range(len(self.columns)):
            it = self.table.item(r, c)
            out.append(it.data(Qt.ItemDataRole.UserRole) if c == self.status_idx else it.text())
        return out

    def selected_rows(self):
        return sorted({i.row() for i in self.table.selectedIndexes()})

    # ---- 过滤
    def apply_filter(self):
        kw = self.search.text().strip().lower()
        st = self.status_box.currentText() if self.status_box else "全部"
        for r in range(self.table.rowCount()):
            txt = " ".join(self.table.item(r, c).text() for c in range(len(self.columns))).lower()
            ok = kw in txt and (st == "全部" or self.table.item(r, self.status_idx).data(Qt.ItemDataRole.UserRole) == st)
            self.table.setRowHidden(r, not ok)
        self.update_footer()

    def reset_filter(self):
        self.search.clear()
        if self.status_box:
            self.status_box.setCurrentIndex(0)

    def update_footer(self):
        total = self.table.rowCount()
        vis = sum(not self.table.isRowHidden(r) for r in range(total))
        self.footer.setText(f"共 {total} 条记录，当前显示 {vis} 条，已选 {len(self.selected_rows())} 条")

    # ---- 操作
    def on_add(self):
        codes = [self.table.item(r, 0).text() for r in range(self.table.rowCount())]
        init = [""] * len(self.columns)
        init[0] = next_code(codes)
        if self.status_idx is not None and self.status_opts:
            init[self.status_idx] = self.status_opts[0]
        dlg = RecordDialog(f"新增{self.title}", self.columns, self.status_idx, self.status_opts, init, self)
        if dlg.exec():
            self.table.setSortingEnabled(False)
            self.table.insertRow(0)
            self.fill_row(0, dlg.values())
            self.table.setSortingEnabled(True)
            self.apply_filter()
            self.main.toast(f"已新增一条{self.title}记录")

    def on_edit(self):
        rows = self.selected_rows()
        if len(rows) != 1:
            QMessageBox.information(self, "提示", "请先选中一条要编辑的记录")
            return
        r = rows[0]
        dlg = RecordDialog(f"编辑{self.title}", self.columns, self.status_idx, self.status_opts, self.row_values(r), self)
        if dlg.exec():
            self.table.setSortingEnabled(False)
            self.fill_row(r, dlg.values())
            self.table.setSortingEnabled(True)
            self.apply_filter()
            self.main.toast("修改已保存")

    def on_delete(self):
        rows = self.selected_rows()
        if not rows:
            QMessageBox.information(self, "提示", "请先选中要删除的记录")
            return
        if QMessageBox.question(self, "确认删除", f"确定删除选中的 {len(rows)} 条记录吗？") == QMessageBox.StandardButton.Yes:
            for r in reversed(rows):
                self.table.removeRow(r)
            self.update_footer()
            self.main.toast(f"已删除 {len(rows)} 条记录")

    def on_detail(self, r):
        DetailDialog(self.title, self.columns, self.row_values(r), self).exec()

    def on_import(self):
        QMessageBox.information(self, "导入", "导入功能待开发。")

    def on_export(self):
        path, _ = QFileDialog.getSaveFileName(self, "导出 CSV", f"{self.title}.csv", "CSV 文件 (*.csv)")
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow(self.columns)
                for r in range(self.table.rowCount()):
                    if not self.table.isRowHidden(r):
                        w.writerow(self.row_values(r))
            self.main.toast(f"已导出到 {path}")
        except OSError as e:
            QMessageBox.warning(self, "导出失败", str(e))

    def on_menu(self, pos):
        r = self.table.rowAt(pos.y())
        if r < 0:
            return
        self.table.selectRow(r)
        m = QMenu(self)
        m.addAction("查看详情", lambda: self.on_detail(r))
        m.addAction("编辑", self.on_edit)
        m.addSeparator()
        m.addAction("删除", self.on_delete)
        m.exec(self.table.viewport().mapToGlobal(pos))


# ============================================================== 工作台
class KpiCard(QFrame):
    def __init__(self, icon, title, value, delta, good, color):
        super().__init__()
        self.setObjectName("card")
        lay = QHBoxLayout(self)
        lay.setContentsMargins(18, 16, 18, 16)
        lay.setSpacing(14)
        c = QColor(color)
        ic = QLabel(icon)
        ic.setFixedSize(52, 52)
        ic.setAlignment(Qt.AlignmentFlag.AlignCenter)
        ic.setStyleSheet(f"background: rgba({c.red()},{c.green()},{c.blue()},38); border-radius: 12px; font-size: 24px;")
        lay.addWidget(ic)
        col = QVBoxLayout()
        col.setSpacing(2)
        t = QLabel(title)
        t.setObjectName("hint")
        v = QLabel(value)
        v.setObjectName("kpiValue")
        d = QLabel(delta)
        d.setStyleSheet(f"color: {'#16A34A' if good else '#DC2626'}; font-size: 12px;")
        for w in (t, v, d):
            col.addWidget(w)
        lay.addLayout(col, 1)


def titled_card(title, widget, right_widget=None):
    card = make_card()
    lay = QVBoxLayout(card)
    lay.setContentsMargins(18, 14, 18, 14)
    lay.setSpacing(8)
    head = QHBoxLayout()
    t = QLabel(title)
    t.setObjectName("cardTitle")
    head.addWidget(t)
    head.addStretch(1)
    if right_widget:
        head.addWidget(right_widget)
    lay.addLayout(head)
    lay.addWidget(widget, 1)
    return card


class Dashboard(QScrollArea):
    def __init__(self, main, user):
        super().__init__()
        self.setWidgetResizable(True)
        self.setFrameShape(QFrame.Shape.NoFrame)
        inner = QWidget()
        inner.setObjectName("dashInner")
        self.setWidget(inner)
        root = QVBoxLayout(inner)
        root.setContentsMargins(6, 6, 14, 16)
        root.setSpacing(16)

        hour = datetime.datetime.now().hour
        hello = "夜深了" if hour < 6 else "早上好" if hour < 11 else "中午好" if hour < 14 else "下午好" if hour < 18 else "晚上好"
        wk = "一二三四五六日"[TODAY.weekday()]
        title = QLabel(f"{hello}，{user}")
        title.setObjectName("pageTitle")
        sub = QLabel(f"{TODAY.year} 年 {TODAY.month} 月 {TODAY.day} 日 · 星期{wk} · 今天也要元气满满")
        sub.setObjectName("hint")
        root.addWidget(title)
        root.addWidget(sub)

        kp = QHBoxLayout()
        kp.setSpacing(16)
        for args in (("💰", "今日销售额", "¥ 128,460", "较昨日 ▲ 12.5%", True, "#3B6CF6"),
                     ("🧾", "待处理订单", "37", "较昨日 ▼ 3.2%", True, "#F59E0B"),
                     ("⚠", "库存预警物料", "12", "较昨日 ▲ 2 项", False, "#EF4444"),
                     ("💳", "本月应收账款", "¥ 856,200", "较上月 ▲ 6.8%", True, "#10B981")):
            kp.addWidget(KpiCard(*args))
        root.addLayout(kp)

        tiles = QWidget()
        tl = QHBoxLayout(tiles)
        tl.setContentsMargins(0, 0, 0, 0)
        tl.setSpacing(12)
        for icon, text, key in (("🛒", "销售订单", "sales_order"), ("📥", "采购订单", "purchase_order"),
                                ("📦", "库存查询", "inventory"), ("🏭", "生产工单", "production_order"),
                                ("💰", "应收账款", "receivable"), ("👥", "员工档案", "employee"),
                                ("⚙", "系统设置", "settings")):
            b = QPushButton(f"{icon}\n{text}")
            b.setObjectName("tile")
            b.setCursor(Qt.CursorShape.PointingHandCursor)
            b.clicked.connect(lambda _=False, k=key: main.open_page(k))
            tl.addWidget(b)
        root.addWidget(titled_card("快捷入口", tiles))

        days = [(TODAY - datetime.timedelta(days=6 - i)).strftime("%m-%d") for i in range(7)]
        rnd = random.Random(7)
        vals = [rnd.randint(18, 46) for _ in range(7)]
        mid = QHBoxLayout()
        mid.setSpacing(16)
        mid.addWidget(titled_card("近 7 日销售额（万元）", BarChart(days, vals)), 2)
        mid.addWidget(titled_card("订单状态分布", DonutChart(
            [("已完成", 128, "#16A34A"), ("处理中", 46, "#3B6CF6"), ("待发货", 31, "#F59E0B"),
             ("已取消", 9, "#EF4444")])), 1)
        root.addLayout(mid)

        bot = QHBoxLayout()
        bot.setSpacing(16)
        t = QTableWidget(6, 4)
        t.setHorizontalHeaderLabels(["订单编号", "客户", "金额", "状态"])
        t.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        t.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        t.setShowGrid(False)
        t.verticalHeader().setVisible(False)
        t.verticalHeader().setDefaultSectionSize(40)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        t.setFrameShape(QFrame.Shape.NoFrame)
        t.setMinimumHeight(6 * 40 + 42)
        for r, row in enumerate(gen_rows("sales_order")[:6]):
            for c, idx in enumerate((0, 1, 5, 6)):
                it = QTableWidgetItem(("● " if c == 3 else "") + row[idx])
                if c == 3:
                    it.setForeground(QColor(status_color(row[idx])))
                if c == 2:
                    it.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                t.setItem(r, c, it)
        more = QPushButton("查看全部")
        more.clicked.connect(lambda: main.open_page("sales_order"))
        bot.addWidget(titled_card("最新销售订单", t, more), 2)

        todo = QListWidget()
        for text, done in (("审核 3 张待审核销售订单", False), ("跟进逾期应收账款（2 家客户）", False),
                           ("确认本周生产计划排期", False), ("处理库存预警：补货申请", True),
                           ("提交本月考勤汇总", True)):
            it = QListWidgetItem(text)
            it.setFlags(it.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            it.setCheckState(Qt.CheckState.Checked if done else Qt.CheckState.Unchecked)
            todo.addItem(it)
        bot.addWidget(titled_card("待办事项", todo), 1)
        root.addLayout(bot)
        root.addStretch(1)


class PlaceholderPage(QWidget):
    def __init__(self, icon, title):
        super().__init__()
        lay = QVBoxLayout(self)
        lay.addStretch(1)
        for text, name in ((icon, "bigIcon"), (title, "pageTitle"), ("该模块页面待开发，可在 MainWindow._create_page 中接入自定义页面", "hint")):
            lb = QLabel(text)
            lb.setObjectName(name)
            lb.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lay.addWidget(lb)
        lay.addStretch(2)


class SettingsPage(QWidget):
    def __init__(self, main):
        super().__init__()
        self.main = main
        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(12)
        t = QLabel("系统设置")
        t.setObjectName("pageTitle")
        root.addWidget(t)
        card = make_card()
        form = QFormLayout(card)
        form.setContentsMargins(28, 24, 28, 24)
        form.setSpacing(16)
        self.company = QLineEdit(COMPANY)
        self.company.setMinimumWidth(320)
        self.theme = QComboBox()
        self.theme.addItems(["浅色", "深色"])
        self.theme.setCurrentIndex(0 if main.theme == "light" else 1)
        self.theme.currentIndexChanged.connect(lambda i: main.apply_theme("light" if i == 0 else "dark"))
        lang = QComboBox()
        lang.addItems(["简体中文"])
        self.keep = QCheckBox("启动时自动展开侧边栏")
        self.keep.setChecked(True)
        form.addRow("公司名称", self.company)
        form.addRow("界面主题", self.theme)
        form.addRow("界面语言", lang)
        form.addRow("", self.keep)
        save = QPushButton("保存设置")
        save.setObjectName("primary")
        save.clicked.connect(self.save)
        form.addRow("", save)
        root.addWidget(card)
        hint = QLabel("提示：这里只是设置页骨架，后续可接入数据库连接、编号规则、打印模板等配置。")
        hint.setObjectName("hint")
        root.addWidget(hint)
        root.addStretch(1)

    def save(self):
        self.main.brand.setText(f"◆ {self.company.text().strip() or COMPANY}")
        self.main.toast("设置已保存")


# ============================================================== 主窗口
ROLE_KEY = Qt.ItemDataRole.UserRole
ROLE_BASE = Qt.ItemDataRole.UserRole + 1


class MainWindow(QMainWindow):
    def __init__(self, user):
        super().__init__()
        self.user = user
        self.theme = "light"
        self.pages, self.items, self.meta = {}, {}, {"dashboard": ("", "工作台"), "settings": ("系统管理", "系统设置")}
        for icon, group, subs in MENU:
            for k, t in subs:
                self.meta[k] = (group, t)
        self.setWindowTitle(f"{APP_NAME} - 企业资源计划系统")
        self.resize(1400, 840)
        self.setMinimumSize(1100, 680)

        central = QWidget()
        self.setCentralWidget(central)
        h = QHBoxLayout(central)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(0)
        h.addWidget(self._build_sidebar())

        right = QWidget()
        rv = QVBoxLayout(right)
        rv.setContentsMargins(0, 0, 0, 0)
        rv.setSpacing(0)
        rv.addWidget(self._build_topbar())
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        self.tabs.setTabsClosable(True)
        self.tabs.setMovable(True)
        self.tabs.tabCloseRequested.connect(self.close_tab)
        self.tabs.currentChanged.connect(self.on_tab_changed)
        self.tabs.tabBar().setContextPolicy = None
        self.tabs.tabBar().setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tabs.tabBar().customContextMenuRequested.connect(self.tab_menu)
        wrap = QWidget()
        wl = QVBoxLayout(wrap)
        wl.setContentsMargins(14, 10, 8, 4)
        wl.addWidget(self.tabs)
        rv.addWidget(wrap, 1)
        h.addWidget(right, 1)

        sb = QStatusBar()
        self.setStatusBar(sb)
        self.clock = QLabel()
        sb.addPermanentWidget(QLabel(f"当前用户：{user}    "))
        sb.addPermanentWidget(self.clock)
        t = QTimer(self)
        t.timeout.connect(self.tick)
        t.start(1000)
        self.tick()
        sb.showMessage(f"欢迎使用 {APP_NAME} v{VERSION}")

        QShortcut(QKeySequence("Ctrl+W"), self, activated=lambda: self.close_tab(self.tabs.currentIndex()))
        self.open_page("dashboard")

    # ---- 侧边栏
    def _build_sidebar(self):
        self.sidebar = QFrame()
        self.sidebar.setObjectName("sidebar")
        self.sidebar.setFixedWidth(SIDEBAR_W)
        lay = QVBoxLayout(self.sidebar)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        self.brand = QLabel(f"◆ {COMPANY}")
        self.brand.setObjectName("brand")
        sub = QLabel("Enterprise Resource Planning")
        sub.setObjectName("brandSub")
        lay.addWidget(self.brand)
        lay.addWidget(sub)

        tree = self.tree = QTreeWidget()
        tree.setObjectName("menu")
        tree.setHeaderHidden(True)
        tree.setIndentation(14)
        tree.setRootIsDecorated(False)
        tree.setAnimated(True)
        tree.setFrameShape(QFrame.Shape.NoFrame)
        tree.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        tree.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        dash = QTreeWidgetItem(tree, ["🏠  工作台"])
        dash.setData(0, ROLE_KEY, "dashboard")
        self.items["dashboard"] = dash
        for icon, group, subs in MENU:
            g = QTreeWidgetItem(tree)
            g.setData(0, ROLE_BASE, f"{icon}  {group}")
            g.setText(0, f"{icon}  {group}    ▸")
            for k, title in subs:
                it = QTreeWidgetItem(g, [title])
                it.setData(0, ROLE_KEY, k)
                self.items[k] = it
        tree.itemClicked.connect(self.on_menu_click)
        tree.itemExpanded.connect(lambda it: self._arrow(it, True))
        tree.itemCollapsed.connect(lambda it: self._arrow(it, False))
        tree.expandItem(tree.topLevelItem(1))
        lay.addWidget(tree, 1)
        ver = QLabel(f"v{VERSION}  ·  Ctrl+W 关闭标签")
        ver.setObjectName("version")
        lay.addWidget(ver)
        return self.sidebar

    @staticmethod
    def _arrow(item, expanded):
        base = item.data(0, ROLE_BASE)
        if base:
            item.setText(0, f"{base}    {'▾' if expanded else '▸'}")

    def on_menu_click(self, item, _col):
        key = item.data(0, ROLE_KEY)
        if key:
            self.open_page(key)
        else:
            item.setExpanded(not item.isExpanded())
            self.sync_menu()

    def sync_menu(self):
        w = self.tabs.currentWidget()
        key = w.property("page_key") if w else None
        self.tree.blockSignals(True)
        self.tree.clearSelection()
        it = self.items.get(key)
        if it:
            if it.parent():
                it.parent().setExpanded(True)
            self.tree.setCurrentItem(it)
        self.tree.blockSignals(False)

    def toggle_sidebar(self):
        start = self.sidebar.width()
        end = 0 if start > 0 else SIDEBAR_W
        self.anim = QVariantAnimation(self)
        self.anim.setDuration(220)
        self.anim.setStartValue(start)
        self.anim.setEndValue(end)
        self.anim.setEasingCurve(QEasingCurve.Type.InOutCubic)
        self.anim.valueChanged.connect(lambda v: self.sidebar.setFixedWidth(int(v)))
        self.anim.start()

    # ---- 顶栏
    def _build_topbar(self):
        bar = QFrame()
        bar.setObjectName("topbar")
        bar.setFixedHeight(56)
        h = QHBoxLayout(bar)
        h.setContentsMargins(12, 0, 16, 0)
        h.setSpacing(6)
        fold = QToolButton()
        fold.setObjectName("iconBtn")
        fold.setText("☰")
        fold.setToolTip("收起/展开菜单")
        fold.clicked.connect(self.toggle_sidebar)
        h.addWidget(fold)
        self.crumb = QLabel()
        self.crumb.setObjectName("crumb")
        h.addWidget(self.crumb)
        h.addStretch(1)
        self.gsearch = QLineEdit()
        self.gsearch.setPlaceholderText("🔍 搜索功能菜单，回车跳转")
        self.gsearch.setFixedWidth(250)
        self.gsearch.returnPressed.connect(self.search_menu)
        h.addWidget(self.gsearch)
        h.addSpacing(8)

        self.theme_btn = QToolButton()
        self.theme_btn.setObjectName("iconBtn")
        self.theme_btn.setText("🌙")
        self.theme_btn.setToolTip("切换浅色 / 深色主题")
        self.theme_btn.clicked.connect(lambda: self.apply_theme("dark" if self.theme == "light" else "light"))
        h.addWidget(self.theme_btn)

        bell = QToolButton()
        bell.setObjectName("iconBtn")
        bell.setText("🔔")
        bell.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        m = QMenu(bell)
        for s in ("3 张销售订单待审核", "库存预警：控制主板 V3 低于安全库存", "2 笔应收账款已逾期"):
            m.addAction(s)
        bell.setMenu(m)
        h.addWidget(bell)

        ub = QToolButton()
        ub.setObjectName("userBtn")
        ub.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        ul = QHBoxLayout(ub)
        ul.setContentsMargins(6, 0, 6, 0)
        av = QLabel(self.user[:1].upper())
        av.setObjectName("avatar")
        av.setFixedSize(32, 32)
        av.setAlignment(Qt.AlignmentFlag.AlignCenter)
        av.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)
        nm = QLabel(f"{self.user}  ▾")
        nm.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)
        ul.addWidget(av)
        ul.addWidget(nm)
        ub.setFixedWidth(140)
        um = QMenu(ub)
        um.addAction("个人中心", lambda: self.toast("个人中心待开发"))
        um.addAction("修改密码", lambda: self.toast("修改密码待开发"))
        um.addAction("关于系统", lambda: QMessageBox.about(self, "关于", f"{APP_NAME} v{VERSION}\nPyQt6 单文件骨架示例"))
        um.addSeparator()
        um.addAction("退出系统", self.close)
        ub.setMenu(um)
        h.addWidget(ub)
        return bar

    def update_crumb(self):
        w = self.tabs.currentWidget()
        key = w.property("page_key") if w else "dashboard"
        group, title = self.meta.get(key, ("", ""))
        trail = "首页" + (f" / {group}" if group else "")
        self.crumb.setText(f"<span style='color:{PAL['subtext']}'>{trail} / </span><b>{title}</b>")

    def search_menu(self):
        kw = self.gsearch.text().strip()
        if not kw:
            return
        for k, (g, t) in self.meta.items():
            if kw in t or kw in g:
                self.open_page(k)
                self.gsearch.clear()
                return
        self.toast(f"没有找到与「{kw}」相关的功能")

    # ---- 标签页
    def _create_page(self, key):
        if key == "dashboard":
            return Dashboard(self, self.user)
        if key == "settings":
            return SettingsPage(self)
        if key in PLACEHOLDERS:
            return PlaceholderPage(PLACEHOLDERS[key], self.meta[key][1])
        return ModulePage(key, PAGES[key], self)

    def open_page(self, key):
        if key in self.pages:
            self.tabs.setCurrentWidget(self.pages[key])
            return
        w = self._create_page(key)
        w.setProperty("page_key", key)
        self.pages[key] = w
        idx = self.tabs.addTab(w, self.meta[key][1])
        if key == "dashboard":
            self.tabs.tabBar().setTabButton(idx, QTabBar.ButtonPosition.RightSide, None)
        self.tabs.setCurrentIndex(idx)

    def close_tab(self, idx):
        w = self.tabs.widget(idx)
        if w is None:
            return
        key = w.property("page_key")
        if key == "dashboard":
            return
        self.tabs.removeTab(idx)
        self.pages.pop(key, None)
        w.deleteLater()

    def tab_menu(self, pos):
        idx = self.tabs.tabBar().tabAt(pos)
        if idx < 0:
            return
        m = QMenu(self)
        m.addAction("关闭当前", lambda: self.close_tab(idx))
        m.addAction("关闭其他", lambda: self.close_others(idx))
        m.addAction("关闭全部", lambda: self.close_others(-1))
        m.exec(self.tabs.tabBar().mapToGlobal(pos))

    def close_others(self, keep_idx):
        keep = self.tabs.widget(keep_idx) if keep_idx >= 0 else None
        for w in list(self.pages.values()):
            if w is not keep and w.property("page_key") != "dashboard":
                self.close_tab(self.tabs.indexOf(w))

    def on_tab_changed(self, _):
        self.update_crumb()
        self.sync_menu()

    # ---- 其他
    def apply_theme(self, name):
        self.theme = name
        apply_theme(name)
        self.theme_btn.setText("🌙" if name == "light" else "☀")
        self.update_crumb()
        for c in self.findChildren((BarChart, DonutChart)):
            c.update()

    def toast(self, msg):
        self.statusBar().showMessage(msg, 4000)

    def tick(self):
        self.clock.setText(QDateTime.currentDateTime().toString("yyyy-MM-dd  HH:mm:ss"))


# ============================================================== 入口
def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setApplicationName(APP_NAME)
    apply_theme("light")
    user = "管理员"
    if SHOW_LOGIN:
        dlg = LoginDialog()
        if dlg.exec() != QDialog.DialogCode.Accepted:
            sys.exit(0)
        user = "管理员" if dlg.username == DEMO_USER else dlg.username
    win = MainWindow(user)
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
