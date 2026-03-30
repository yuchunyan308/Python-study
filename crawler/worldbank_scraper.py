"""
世界银行 Open Data 爬虫
World Bank Open Data API Scraper
API文档: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392

功能模块:
  - 多领域指标采集（经济、教育、健康、环境）
  - 多国家批量请求 + 自动翻页
  - 结构化输出（JSON / CSV / Excel / 控制台报表）
  - 请求重试 + 速率限制
  - 数据清洗 + 统计摘要
"""

import time
import json
import logging
import os
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

# ─────────────────────────────────────────────
# 日志配置
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 常量 & 配置
# ─────────────────────────────────────────────
BASE_URL = "https://api.worldbank.org/v2"
DEFAULT_PER_PAGE = 100
REQUEST_DELAY = 0.3       # 礼貌延时（秒）
MAX_RETRIES = 3

# 目标国家（ISO-2 代码）
TARGET_COUNTRIES = [
    "CN", "US", "JP", "DE", "GB",
    "FR", "IN", "BR", "KR", "AU",
]

# 指标分类 & 说明
INDICATORS: dict[str, dict] = {
    # ── 经济 ──────────────────────────────────
    "NY.GDP.MKTP.CD":      {"name": "GDP（现价美元）",         "category": "经济"},
    "NY.GDP.PCAP.CD":      {"name": "人均GDP（现价美元）",     "category": "经济"},
    "FP.CPI.TOTL.ZG":      {"name": "CPI通货膨胀率（%）",     "category": "经济"},
    "NE.EXP.GNFS.ZS":      {"name": "出口占GDP比例（%）",     "category": "经济"},
    # ── 教育 ──────────────────────────────────
    "SE.ADT.LITR.ZS":      {"name": "成人识字率（%）",        "category": "教育"},
    "SE.PRM.ENRR":         {"name": "小学入学率（%）",        "category": "教育"},
    "SE.TER.ENRR":         {"name": "高等教育入学率（%）",    "category": "教育"},
    "SE.XPD.TOTL.GD.ZS":  {"name": "教育支出占GDP（%）",     "category": "教育"},
    # ── 健康 ──────────────────────────────────
    "SP.DYN.LE00.IN":      {"name": "预期寿命（年）",         "category": "健康"},
    "SH.DYN.MORT":         {"name": "5岁以下死亡率（‰）",    "category": "健康"},
    "SH.XPD.CHEX.GD.ZS":  {"name": "卫生支出占GDP（%）",     "category": "健康"},
    "SP.DYN.TFRT.IN":      {"name": "生育率（每女性）",       "category": "健康"},
    # ── 环境 ──────────────────────────────────
    "EN.ATM.CO2E.PC":      {"name": "人均CO₂排放（吨）",     "category": "环境"},
    "EG.USE.PCAP.KG.OE":  {"name": "人均能源使用（千克油当量）","category": "环境"},
    "AG.LND.FRST.ZS":     {"name": "森林覆盖率（%）",        "category": "环境"},
    "ER.H2O.FWTL.ZS":     {"name": "淡水提取占可用量（%）",  "category": "环境"},
    # ── 人口 ──────────────────────────────────
    "SP.POP.TOTL":         {"name": "总人口",                 "category": "人口"},
    "SP.URB.TOTL.IN.ZS":  {"name": "城镇化率（%）",          "category": "人口"},
}

# 年份范围
YEAR_START = 2000
YEAR_END   = 2023


# ─────────────────────────────────────────────
# 数据类
# ─────────────────────────────────────────────
@dataclass
class IndicatorRecord:
    country_code:   str
    country_name:   str
    indicator_code: str
    indicator_name: str
    category:       str
    year:           int
    value:          Optional[float]


@dataclass
class CountryInfo:
    code:         str
    name:         str
    region:       str
    income_level: str
    capital:      str
    longitude:    float = 0.0
    latitude:     float = 0.0


@dataclass
class ScraperResult:
    records:       list[IndicatorRecord] = field(default_factory=list)
    countries:     list[CountryInfo]     = field(default_factory=list)
    scrape_time:   str = ""
    total_records: int = 0
    errors:        list[str] = field(default_factory=list)


# ─────────────────────────────────────────────
# HTTP 工具
# ─────────────────────────────────────────────
def build_session() -> requests.Session:
    """构建带重试策略的 Session"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({"Accept": "application/json"})
    return session


def fetch_all_pages(session: requests.Session, url: str, params: dict) -> list[dict]:
    """自动翻页，合并所有结果"""
    params = {**params, "format": "json", "per_page": DEFAULT_PER_PAGE, "page": 1}
    all_data: list[dict] = []

    while True:
        try:
            resp = session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.warning("请求失败: %s | URL: %s", exc, url)
            break

        # World Bank API 返回 [meta, data] 列表
        if not isinstance(payload, list) or len(payload) < 2:
            break

        meta, data = payload[0], payload[1]
        if not data:
            break

        all_data.extend(data)

        total_pages = int(meta.get("pages", 1))
        current_page = int(meta.get("page", 1))
        logger.debug("  翻页 %d/%d | 已采集 %d 条", current_page, total_pages, len(all_data))

        if current_page >= total_pages:
            break

        params["page"] += 1
        time.sleep(REQUEST_DELAY)

    return all_data


# ─────────────────────────────────────────────
# 数据采集
# ─────────────────────────────────────────────
class WorldBankScraper:

    def __init__(self, countries: list[str], indicators: dict[str, dict],
                 year_start: int, year_end: int):
        self.countries    = countries
        self.indicators   = indicators
        self.year_start   = year_start
        self.year_end     = year_end
        self.session      = build_session()
        self.result       = ScraperResult(scrape_time=datetime.now().isoformat())

    # ── 1. 国家元数据 ──────────────────────────
    def fetch_country_info(self) -> list[CountryInfo]:
        logger.info("=== 采集国家基本信息 ===")
        codes_str = ";".join(self.countries)
        url  = f"{BASE_URL}/country/{codes_str}"
        data = fetch_all_pages(self.session, url, {})

        infos: list[CountryInfo] = []
        for item in data:
            try:
                infos.append(CountryInfo(
                    code         = item["id"],
                    name         = item["name"],
                    region       = item.get("region", {}).get("value", "N/A"),
                    income_level = item.get("incomeLevel", {}).get("value", "N/A"),
                    capital      = item.get("capitalCity", "N/A"),
                    longitude    = float(item.get("longitude") or 0),
                    latitude     = float(item.get("latitude")  or 0),
                ))
            except Exception as exc:
                self.result.errors.append(f"国家解析错误: {exc}")

        logger.info("  获得 %d 个国家信息", len(infos))
        return infos

    # ── 2. 单指标 × 多国家 ────────────────────
    def fetch_indicator(self, indicator_code: str, meta: dict) -> list[IndicatorRecord]:
        codes_str = ";".join(self.countries)
        url = f"{BASE_URL}/country/{codes_str}/indicator/{indicator_code}"
        params = {"mrv": self.year_end - self.year_start + 1,   # 最近N年
                  "date": f"{self.year_start}:{self.year_end}"}

        raw = fetch_all_pages(self.session, url, params)
        records: list[IndicatorRecord] = []

        for item in raw:
            try:
                value = item.get("value")
                records.append(IndicatorRecord(
                    country_code   = item["countryiso3code"] or item["country"]["id"],
                    country_name   = item["country"]["value"],
                    indicator_code = indicator_code,
                    indicator_name = meta["name"],
                    category       = meta["category"],
                    year           = int(item["date"]),
                    value          = float(value) if value is not None else None,
                ))
            except Exception as exc:
                self.result.errors.append(f"记录解析错误[{indicator_code}]: {exc}")

        return records

    # ── 3. 主流程 ─────────────────────────────
    def run(self) -> ScraperResult:
        # 国家信息
        self.result.countries = self.fetch_country_info()
        time.sleep(REQUEST_DELAY)

        # 各指标
        total = len(self.indicators)
        for idx, (code, meta) in enumerate(self.indicators.items(), 1):
            logger.info("[%02d/%02d] %s — %s", idx, total, code, meta["name"])
            recs = self.fetch_indicator(code, meta)
            self.result.records.extend(recs)
            logger.info("  → 获得 %d 条记录（含空值）", len(recs))
            time.sleep(REQUEST_DELAY)

        self.result.total_records = len(self.result.records)
        logger.info("=== 采集完成，共 %d 条记录，%d 个错误 ===",
                    self.result.total_records, len(self.result.errors))
        return self.result


# ─────────────────────────────────────────────
# 数据清洗
# ─────────────────────────────────────────────
def generate_mock_data(result: ScraperResult) -> None:
    """网络不通时生成仿真演示数据（结构与真实API完全一致）"""
    import random, math
    random.seed(42)

    country_list = [
        CountryInfo("CHN","China","East Asia & Pacific","Upper middle income","Beijing",116.3912,39.9093),
        CountryInfo("USA","United States","North America","High income","Washington D.C.",-77.032,38.8895),
        CountryInfo("JPN","Japan","East Asia & Pacific","High income","Tokyo",139.77,35.67),
        CountryInfo("DEU","Germany","Europe & Central Asia","High income","Berlin",13.4115,52.5235),
        CountryInfo("GBR","United Kingdom","Europe & Central Asia","High income","London",-0.126236,51.5002),
        CountryInfo("FRA","France","Europe & Central Asia","High income","Paris",2.35097,48.8566),
        CountryInfo("IND","India","South Asia","Lower middle income","New Delhi",77.225,28.6353),
        CountryInfo("BRA","Brazil","Latin America & Caribbean","Upper middle income","Brasilia",-47.9292,-15.7801),
        CountryInfo("KOR","Korea, Rep.","East Asia & Pacific","High income","Seoul",126.957,37.5323),
        CountryInfo("AUS","Australia","East Asia & Pacific","High income","Canberra",149.129,-35.282),
    ]
    result.countries = country_list

    # 基准值（2000年）
    base: dict[str, dict] = {
        "GDP（现价美元）":          {"CHN":1.21e12,"USA":10.25e12,"JPN":4.97e12,"DEU":1.95e12,"GBR":1.67e12,"FRA":1.36e12,"IND":4.77e11,"BRA":6.55e11,"KOR":5.76e11,"AUS":4.14e11},
        "人均GDP（现价美元）":       {"CHN":959,"USA":36450,"JPN":39173,"DEU":23742,"GBR":28190,"FRA":22545,"IND":453,"BRA":3749,"KOR":12256,"AUS":21782},
        "CPI通货膨胀率（%）":       {"CHN":0.4,"USA":3.4,"JPN":-0.7,"DEU":1.4,"GBR":2.9,"FRA":1.8,"IND":4.0,"BRA":7.0,"KOR":2.3,"AUS":4.5},
        "出口占GDP比例（%）":        {"CHN":23,"USA":10.7,"JPN":10.8,"DEU":33.4,"GBR":28.0,"FRA":28.4,"IND":13.0,"BRA":10.0,"KOR":38.0,"AUS":18.0},
        "成人识字率（%）":           {"CHN":90.9,"USA":99.0,"JPN":99.0,"DEU":99.0,"GBR":99.0,"FRA":99.0,"IND":61.0,"BRA":86.4,"KOR":97.9,"AUS":99.0},
        "小学入学率（%）":           {"CHN":102,"USA":101,"JPN":100,"DEU":103,"GBR":105,"FRA":105,"IND":95,"BRA":129,"KOR":104,"AUS":103},
        "高等教育入学率（%）":       {"CHN":8.0,"USA":72,"JPN":49,"DEU":52,"GBR":59,"FRA":54,"IND":10,"BRA":16,"KOR":79,"AUS":66},
        "教育支出占GDP（%）":        {"CHN":2.5,"USA":5.5,"JPN":3.6,"DEU":4.5,"GBR":5.0,"FRA":5.9,"IND":4.3,"BRA":4.0,"KOR":4.2,"AUS":5.0},
        "预期寿命（年）":            {"CHN":71.4,"USA":76.8,"JPN":81.1,"DEU":78.0,"GBR":77.9,"FRA":79.2,"IND":63.0,"BRA":70.4,"KOR":76.0,"AUS":79.3},
        "5岁以下死亡率（‰）":        {"CHN":37,"USA":8.4,"JPN":5.3,"DEU":5.0,"GBR":6.5,"FRA":5.4,"IND":91,"BRA":35,"KOR":5.5,"AUS":6.5},
        "卫生支出占GDP（%）":        {"CHN":4.6,"USA":13.1,"JPN":7.6,"DEU":10.4,"GBR":7.0,"FRA":9.5,"IND":4.0,"BRA":7.2,"KOR":4.7,"AUS":8.3},
        "生育率（每女性）":          {"CHN":1.7,"USA":2.1,"JPN":1.36,"DEU":1.38,"GBR":1.64,"FRA":1.89,"IND":3.3,"BRA":2.4,"KOR":1.47,"AUS":1.75},
        "人均CO₂排放（吨）":         {"CHN":2.7,"USA":20.2,"JPN":9.7,"DEU":10.2,"GBR":9.1,"FRA":6.2,"IND":1.1,"BRA":1.9,"KOR":9.1,"AUS":17.6},
        "人均能源使用（千克油当量）": {"CHN":895,"USA":7900,"JPN":3935,"DEU":4210,"GBR":3700,"FRA":4040,"IND":490,"BRA":1170,"KOR":3720,"AUS":5600},
        "森林覆盖率（%）":           {"CHN":16.6,"USA":33.3,"JPN":68.4,"DEU":32.8,"GBR":11.6,"FRA":28.8,"IND":23.1,"BRA":62.4,"KOR":64.5,"AUS":16.2},
        "淡水提取占可用量（%）":      {"CHN":19.5,"USA":16.0,"JPN":19.6,"DEU":23.0,"GBR":11.0,"FRA":15.0,"IND":32.0,"BRA":1.4,"KOR":36.0,"AUS":4.4},
        "总人口":                    {"CHN":1.267e9,"USA":2.82e8,"JPN":1.27e8,"DEU":8.21e7,"GBR":5.89e7,"FRA":6.09e7,"IND":1.059e9,"BRA":1.75e8,"KOR":4.73e7,"AUS":1.9e7},
        "城镇化率（%）":             {"CHN":36.2,"USA":79.1,"JPN":78.7,"DEU":73.1,"GBR":78.7,"FRA":75.8,"IND":27.7,"BRA":81.2,"KOR":79.6,"AUS":87.2},
    }

    # 增长趋势参数
    trend: dict[str, float] = {
        "GDP（现价美元）":           0.09,
        "人均GDP（现价美元）":       0.08,
        "CPI通货膨胀率（%）":        0.0,
        "出口占GDP比例（%）":        0.005,
        "成人识字率（%）":           0.003,
        "小学入学率（%）":           0.001,
        "高等教育入学率（%）":       0.04,
        "教育支出占GDP（%）":        0.01,
        "预期寿命（年）":            0.003,
        "5岁以下死亡率（‰）":        -0.04,
        "卫生支出占GDP（%）":        0.02,
        "生育率（每女性）":          -0.01,
        "人均CO₂排放（吨）":         0.02,
        "人均能源使用（千克油当量）": 0.01,
        "森林覆盖率（%）":           0.002,
        "淡水提取占可用量（%）":      0.005,
        "总人口":                    0.008,
        "城镇化率（%）":             0.012,
    }

    code_map = {
        "GDP（现价美元）":           ("NY.GDP.MKTP.CD","经济"),
        "人均GDP（现价美元）":       ("NY.GDP.PCAP.CD","经济"),
        "CPI通货膨胀率（%）":        ("FP.CPI.TOTL.ZG","经济"),
        "出口占GDP比例（%）":        ("NE.EXP.GNFS.ZS","经济"),
        "成人识字率（%）":           ("SE.ADT.LITR.ZS","教育"),
        "小学入学率（%）":           ("SE.PRM.ENRR","教育"),
        "高等教育入学率（%）":       ("SE.TER.ENRR","教育"),
        "教育支出占GDP（%）":        ("SE.XPD.TOTL.GD.ZS","教育"),
        "预期寿命（年）":            ("SP.DYN.LE00.IN","健康"),
        "5岁以下死亡率（‰）":        ("SH.DYN.MORT","健康"),
        "卫生支出占GDP（%）":        ("SH.XPD.CHEX.GD.ZS","健康"),
        "生育率（每女性）":          ("SP.DYN.TFRT.IN","健康"),
        "人均CO₂排放（吨）":         ("EN.ATM.CO2E.PC","环境"),
        "人均能源使用（千克油当量）": ("EG.USE.PCAP.KG.OE","环境"),
        "森林覆盖率（%）":           ("AG.LND.FRST.ZS","环境"),
        "淡水提取占可用量（%）":      ("ER.H2O.FWTL.ZS","环境"),
        "总人口":                    ("SP.POP.TOTL","人口"),
        "城镇化率（%）":             ("SP.URB.TOTL.IN.ZS","人口"),
    }

    country_name_map = {c.code: c.name for c in country_list}

    for ind_name, (ind_code, cat) in code_map.items():
        bases = base[ind_name]
        g     = trend[ind_name]
        for iso2, c in zip(["CN","US","JP","DE","GB","FR","IN","BR","KR","AU"],
                           ["CHN","USA","JPN","DEU","GBR","FRA","IND","BRA","KOR","AUS"]):
            b0 = bases[c]
            for yr in range(YEAR_START, YEAR_END + 1):
                t = yr - YEAR_START
                noise = random.gauss(0, abs(b0) * 0.02)
                v = b0 * math.exp(g * t) + noise
                result.records.append(IndicatorRecord(
                    country_code   = c,
                    country_name   = country_name_map[c],
                    indicator_code = ind_code,
                    indicator_name = ind_name,
                    category       = cat,
                    year           = yr,
                    value          = round(v, 4) if v > 0 else None,
                ))

    result.total_records = len(result.records)
    logger.info("[演示模式] 已生成 %d 条仿真记录", result.total_records)


def clean_data(result: ScraperResult) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    返回:
      df_long  — 长格式（每行一条观测值）
      df_wide  — 宽格式（每行一个国家-年份，列为指标）
    """
    df = pd.DataFrame([asdict(r) for r in result.records])

    # 过滤空值后的有效记录
    df_valid = df.dropna(subset=["value"]).copy()
    df_valid["value"] = df_valid["value"].round(4)

    # 宽格式
    df_wide = df_valid.pivot_table(
        index   = ["country_code", "country_name", "year"],
        columns = "indicator_name",
        values  = "value",
        aggfunc = "first",
    ).reset_index()
    df_wide.columns.name = None

    logger.info("清洗后：长格式 %d 行，宽格式 %d 行 × %d 列",
                len(df_valid), *df_wide.shape)
    return df_valid, df_wide


# ─────────────────────────────────────────────
# 统计摘要
# ─────────────────────────────────────────────
def compute_summary(df_long: pd.DataFrame) -> pd.DataFrame:
    """按 category + indicator_name 计算统计摘要"""
    summary = (
        df_long.groupby(["category", "indicator_name"])["value"]
        .agg(
            有效观测数="count",
            均值="mean",
            中位数="median",
            最小值="min",
            最大值="max",
            标准差="std",
        )
        .round(3)
        .reset_index()
    )
    return summary


# ─────────────────────────────────────────────
# 输出
# ─────────────────────────────────────────────
def save_outputs(result: ScraperResult, df_long: pd.DataFrame,
                 df_wide: pd.DataFrame, out_dir: str = "worldbank_output") -> None:

    os.makedirs(out_dir, exist_ok=True)

    # 1. 原始 JSON
    json_path = os.path.join(out_dir, "raw_records.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "scrape_time":   result.scrape_time,
                "total_records": result.total_records,
                "errors":        result.errors,
                "countries":     [asdict(c) for c in result.countries],
                "records":       [asdict(r) for r in result.records],
            },
            f, ensure_ascii=False, indent=2,
        )
    logger.info("JSON 已保存: %s", json_path)

    # 2. 长格式 CSV
    csv_long = os.path.join(out_dir, "data_long.csv")
    df_long.to_csv(csv_long, index=False, encoding="utf-8-sig")
    logger.info("长格式 CSV: %s", csv_long)

    # 3. 宽格式 CSV
    csv_wide = os.path.join(out_dir, "data_wide.csv")
    df_wide.to_csv(csv_wide, index=False, encoding="utf-8-sig")
    logger.info("宽格式 CSV: %s", csv_wide)

    # 4. Excel（多 Sheet）
    excel_path = os.path.join(out_dir, "worldbank_data.xlsx")
    summary = compute_summary(df_long)
    countries_df = pd.DataFrame([asdict(c) for c in result.countries])

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        df_long.to_excel(writer, sheet_name="长格式数据",    index=False)
        df_wide.to_excel(writer, sheet_name="宽格式数据",    index=False)
        summary.to_excel(writer, sheet_name="统计摘要",      index=False)
        countries_df.to_excel(writer, sheet_name="国家信息", index=False)

    logger.info("Excel 已保存: %s", excel_path)

    # 5. 各国最新值快照 CSV
    latest = (
        df_long.sort_values("year", ascending=False)
        .groupby(["country_code", "indicator_name"])
        .first()
        .reset_index()
    )[["country_code", "country_name", "category",
       "indicator_name", "year", "value"]]
    latest_path = os.path.join(out_dir, "latest_snapshot.csv")
    latest.to_csv(latest_path, index=False, encoding="utf-8-sig")
    logger.info("最新快照 CSV: %s", latest_path)


# ─────────────────────────────────────────────
# 控制台报表
# ─────────────────────────────────────────────
def print_report(result: ScraperResult, df_long: pd.DataFrame) -> None:
    sep = "=" * 70

    print(f"\n{sep}")
    print("  世界银行 Open Data 采集报告")
    print(f"  采集时间: {result.scrape_time}")
    print(sep)

    # 国家信息
    print("\n【国家信息】")
    for c in result.countries:
        print(f"  {c.code:4s} {c.name:<20s} | 地区: {c.region:<25s} | 收入: {c.income_level}")

    # 总览
    print(f"\n【数据总览】")
    print(f"  指标数量   : {len(INDICATORS)}")
    print(f"  国家数量   : {len(result.countries)}")
    print(f"  原始记录数  : {result.total_records:,}")
    valid_cnt = df_long.shape[0]
    print(f"  有效记录数  : {valid_cnt:,}  (空值率 {(1-valid_cnt/max(result.total_records,1))*100:.1f}%)")
    print(f"  年份范围   : {YEAR_START} – {YEAR_END}")
    print(f"  采集错误数  : {len(result.errors)}")

    # 各类别汇总
    print("\n【分类统计】")
    for cat, grp in df_long.groupby("category"):
        indicators_in_cat = grp["indicator_name"].nunique()
        obs_cnt = len(grp)
        print(f"  {cat:<6s} | 指标数: {indicators_in_cat} | 有效观测: {obs_cnt:,}")

    # 最新年份 GDP 排名
    print("\n【最新年份 GDP 排名（现价美元）】")
    gdp_name = "GDP（现价美元）"
    gdp_df = (
        df_long[df_long["indicator_name"] == gdp_name]
        .sort_values("year", ascending=False)
        .groupby("country_name")
        .first()
        .reset_index()
        .sort_values("value", ascending=False)
    )
    for rank, row in enumerate(gdp_df.itertuples(), 1):
        print(f"  {rank:2d}. {row.country_name:<20s} {row.year}  "
              f"USD {row.value/1e12:>8.2f} 万亿")

    # 最新预期寿命
    print("\n【最新预期寿命（年）】")
    le_name = "预期寿命（年）"
    le_df = (
        df_long[df_long["indicator_name"] == le_name]
        .sort_values("year", ascending=False)
        .groupby("country_name")
        .first()
        .reset_index()
        .sort_values("value", ascending=False)
    )
    for row in le_df.itertuples():
        bar = "█" * int(row.value / 5)
        print(f"  {row.country_name:<20s} {row.year}  {row.value:5.1f} 岁  {bar}")

    # 人均 CO₂ 排放
    print("\n【最新人均 CO₂ 排放（吨）】")
    co2_name = "人均CO₂排放（吨）"
    co2_df = (
        df_long[df_long["indicator_name"] == co2_name]
        .sort_values("year", ascending=False)
        .groupby("country_name")
        .first()
        .reset_index()
        .sort_values("value", ascending=False)
    )
    for row in co2_df.itertuples():
        bar = "▓" * int(row.value / 2)
        print(f"  {row.country_name:<20s} {row.year}  {row.value:6.2f} 吨  {bar}")

    print(f"\n{sep}\n")


# ─────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────
def main():
    scraper = WorldBankScraper(
        countries  = TARGET_COUNTRIES,
        indicators = INDICATORS,
        year_start = YEAR_START,
        year_end   = YEAR_END,
    )

    result = scraper.run()

    if result.total_records == 0:
        logger.warning("未获取到任何数据，切换到演示模式（仿真数据）...")
        generate_mock_data(result)

    df_long, df_wide = clean_data(result)

    print_report(result, df_long)
    save_outputs(result, df_long, df_wide)


if __name__ == "__main__":
    main()
