"""
data.gov 爬虫 - 离线演示
========================
使用真实 API 数据结构的模拟数据运行完整演示，
生成所有输出格式：JSON / CSV / Excel
"""

import csv
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from datagov_scraper import (
    Dataset, Resource, SearchResult, OutputWriter, log
)

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)
writer = OutputWriter(OUTPUT_DIR)

DIVIDER  = "=" * 72
DIVIDER2 = "-" * 72

# ─── 模拟数据（真实 API 返回结构） ────────────────────────────────────────────
MOCK_DATASETS = [
    Dataset(
        id="a3d85c26-b24a-4f9e-9c10-c1d7e9b4f3a2",
        name="global-surface-temperature-change",
        title="Global Surface Temperature Change (GISS Surface Temperature Analysis)",
        notes="NASA GISS Surface Temperature Analysis (GISTEMP v4) — the global surface temperature change relative to 1951–1980 baseline. Combined land-surface air and sea-surface water temperature records from 1880 to present.",
        organization="NASA Goddard Institute for Space Studies",
        org_type="federal-government",
        license_title="U.S. Government Work",
        license_url="http://www.usa.gov/publicdomain/label/1.0/",
        tags=["climate", "temperature", "nasa", "global-warming", "gistemp", "earth-science", "atmosphere"],
        groups=["Climate", "Science and Research"],
        resources=[
            Resource("r001", "Global Mean Temperature CSV", "CSV",
                     "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv",
                     2_048_000, "text/csv", "2022-01-15", "2024-11-01", "Annual/monthly mean temp"),
            Resource("r002", "GISTEMP v4 NetCDF", "NetCDF",
                     "https://data.giss.nasa.gov/pub/gistemp/gistemp1200_GHCNv4_ERSSTv5.nc.gz",
                     156_000_000, "application/x-netcdf", "2022-01-15", "2024-11-01", "Gridded dataset"),
            Resource("r003", "Data Dictionary PDF", "PDF",
                     "https://data.giss.nasa.gov/gistemp/faq/",
                     524_288, "application/pdf", "2022-01-15", "2023-06-20", "Documentation"),
        ],
        metadata_created="2018-03-12T09:00:00",
        metadata_modified="2024-11-01T14:22:30",
        num_resources=3, num_tags=7, state="active",
        url="https://catalog.data.gov/dataset/global-surface-temperature-change",
        extras={"bureau_code": "026:00", "program_code": "026:001", "publisher": "NASA", "spatial": "GLOBAL"},
    ),
    Dataset(
        id="b5c91d47-2e3f-4a80-8b22-d3e8a5c6f4b3",
        name="covid-19-case-surveillance-public-use-data",
        title="COVID-19 Case Surveillance Public Use Data with Geography",
        notes="The COVID-19 case surveillance system database includes individual-level data reported to CDC from all 50 states plus DC and US territories. Data elements include dates, patient demographics, geography, hospitalization and ICU status, and outcomes.",
        organization="Centers for Disease Control and Prevention",
        org_type="federal-government",
        license_title="Creative Commons CCZero",
        license_url="http://www.opendefinition.org/licenses/cc-zero",
        tags=["covid-19", "coronavirus", "public-health", "surveillance", "epidemiology", "cdc", "pandemic"],
        groups=["Health", "Public Safety"],
        resources=[
            Resource("r011", "COVID-19 Case Data (CSV)", "CSV",
                     "https://data.cdc.gov/api/views/n8mc-b4w4/rows.csv",
                     4_294_967_296, "text/csv", "2020-04-30", "2024-10-15", "Full dataset ~25M rows"),
            Resource("r012", "COVID-19 Case Data (JSON)", "JSON",
                     "https://data.cdc.gov/api/views/n8mc-b4w4/rows.json",
                     None, "application/json", "2020-04-30", "2024-10-15", "JSON format"),
            Resource("r013", "Data Dictionary", "PDF",
                     "https://data.cdc.gov/api/views/n8mc-b4w4/files/dict.pdf",
                     102_400, "application/pdf", "2020-04-30", "2021-03-01", "Variable definitions"),
            Resource("r014", "Summary Statistics", "XLSX",
                     "https://data.cdc.gov/api/views/n8mc-b4w4/summary.xlsx",
                     2_097_152, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     "2020-05-01", "2024-10-15", "Weekly rollup"),
        ],
        metadata_created="2020-04-30T00:00:00",
        metadata_modified="2024-10-15T08:00:00",
        num_resources=4, num_tags=7, state="active",
        url="https://catalog.data.gov/dataset/covid-19-case-surveillance-public-use-data",
        extras={"bureau_code": "009:20", "program_code": "009:020", "publisher": "CDC", "accrualPeriodicity": "Weekly"},
    ),
    Dataset(
        id="c7d02e58-3f4g-5b91-9c33-e4f9b6d7g5c4",
        name="federal-spending-by-agency-fy2023",
        title="USASpending.gov — Federal Awards Spending Data FY2023",
        notes="All federal spending awards data for Fiscal Year 2023 from USASpending.gov. Includes contracts, grants, loans, direct payments, and other financial assistance. Total federal obligated amounts exceed $6.5 trillion.",
        organization="Department of the Treasury",
        org_type="federal-government",
        license_title="U.S. Government Work",
        license_url="http://www.usa.gov/publicdomain/label/1.0/",
        tags=["spending", "budget", "federal", "contracts", "grants", "fiscal", "treasury", "transparency"],
        groups=["Finance", "Government"],
        resources=[
            Resource("r021", "Awards FY2023 Full Download (ZIP)", "ZIP",
                     "https://files.usaspending.gov/award_data_archive/FY2023_All_Contracts_Full_20231231.zip",
                     8_589_934_592, "application/zip", "2024-01-15", "2024-01-15", "All contract awards"),
            Resource("r022", "Agency Summary CSV", "CSV",
                     "https://files.usaspending.gov/agency_summary/FY2023_agency_summary.csv",
                     10_485_760, "text/csv", "2024-01-20", "2024-01-20", "Summarized by agency"),
            Resource("r023", "API Endpoint", "API",
                     "https://api.usaspending.gov/api/v2/search/spending_by_award/",
                     None, "application/json", "2020-01-01", "2024-12-01", "Live REST API"),
        ],
        metadata_created="2024-01-15T12:00:00",
        metadata_modified="2024-01-20T09:30:00",
        num_resources=3, num_tags=8, state="active",
        url="https://catalog.data.gov/dataset/federal-spending-by-agency-fy2023",
        extras={"bureau_code": "020:00", "program_code": "020:000", "publisher": "Treasury"},
    ),
    Dataset(
        id="d8e13f69-4g5h-6c02-0d44-f5g0c7e8h6d5",
        name="national-bridge-inventory",
        title="National Bridge Inventory (NBI) — Bridge Condition Data",
        notes="The NBI contains information on bridges and tunnels on public roads in the United States. The data includes structural evaluations, traffic data, and geographic information for over 600,000 bridges nationwide. Updated annually by FHWA.",
        organization="Federal Highway Administration",
        org_type="federal-government",
        license_title="U.S. Government Work",
        license_url="http://www.usa.gov/publicdomain/label/1.0/",
        tags=["bridges", "infrastructure", "transportation", "safety", "fhwa", "civil-engineering"],
        groups=["Transportation", "Infrastructure"],
        resources=[
            Resource("r031", "2023 NBI Dataset (CSV)", "CSV",
                     "https://www.fhwa.dot.gov/bridge/nbi/2023/delimited/AL23.txt",
                     52_428_800, "text/csv", "2023-08-01", "2023-08-01", "All states bridge inventory"),
            Resource("r032", "NBI GIS Shapefile", "SHP",
                     "https://www.fhwa.dot.gov/bridge/nbi/2023/shapefiles/NBI2023.zip",
                     209_715_200, "application/zip", "2023-08-01", "2023-08-01", "GIS data"),
            Resource("r033", "Coding Guide PDF", "PDF",
                     "https://www.fhwa.dot.gov/bridge/mtguide.pdf",
                     5_242_880, "application/pdf", "2012-01-01", "2023-01-01", "Field coding guide"),
        ],
        metadata_created="2015-06-01T00:00:00",
        metadata_modified="2023-08-01T00:00:00",
        num_resources=3, num_tags=6, state="active",
        url="https://catalog.data.gov/dataset/national-bridge-inventory",
        extras={"bureau_code": "021:15", "spatial": "United States", "temporal": "2023"},
    ),
    Dataset(
        id="e9f24g70-5h6i-7d13-1e55-g6h1d8f9i7e6",
        name="energy-consumption-by-sector-2022",
        title="U.S. Energy Consumption by Sector and Fuel Type (EIA)",
        notes="U.S. Energy Information Administration (EIA) monthly energy review data covering residential, commercial, industrial, and transportation energy consumption broken down by fuel type (petroleum, natural gas, coal, renewables, nuclear) from 1949 to present.",
        organization="U.S. Energy Information Administration",
        org_type="federal-government",
        license_title="U.S. Government Work",
        license_url="http://www.usa.gov/publicdomain/label/1.0/",
        tags=["energy", "electricity", "petroleum", "natural-gas", "renewables", "eia", "consumption", "fossil-fuels"],
        groups=["Energy", "Environment"],
        resources=[
            Resource("r041", "Monthly Energy Review (Excel)", "XLSX",
                     "https://www.eia.gov/totalenergy/data/browser/xls.php?tbl=T01.03",
                     3_145_728, "application/vnd.ms-excel", "2022-01-01", "2024-10-31", "Full time series"),
            Resource("r042", "Primary Energy by Source CSV", "CSV",
                     "https://www.eia.gov/totalenergy/data/browser/csv.php?tbl=T01.01",
                     1_572_864, "text/csv", "2022-01-01", "2024-10-31", "Quad BTU units"),
            Resource("r043", "EIA API", "API",
                     "https://api.eia.gov/v2/total-energy/data/",
                     None, "application/json", "2015-01-01", "2024-12-01", "Live JSON API with key"),
            Resource("r044", "State Energy Data System", "ZIP",
                     "https://www.eia.gov/state/seds/seds-data-complete.php",
                     26_214_400, "application/zip", "2022-01-01", "2024-06-01", "State-level breakdowns"),
        ],
        metadata_created="2016-09-15T00:00:00",
        metadata_modified="2024-10-31T12:00:00",
        num_resources=4, num_tags=8, state="active",
        url="https://catalog.data.gov/dataset/energy-consumption-by-sector-2022",
        extras={"bureau_code": "019:20", "program_code": "019:000", "publisher": "EIA"},
    ),
    Dataset(
        id="f0g35h81-6i7j-8e24-2f66-h7i2e9g0j8f7",
        name="acs-5-year-estimates-demographic-and-housing",
        title="American Community Survey 5-Year Estimates: Demographic & Housing (Census)",
        notes="The American Community Survey (ACS) helps local officials, community leaders, and businesses understand the changes taking place in their communities. It is the premier source for detailed population and housing information about the U.S. Five-year estimates provide the largest sample sizes for more accurate statistics at small geographic levels.",
        organization="U.S. Census Bureau",
        org_type="federal-government",
        license_title="Creative Commons CCZero",
        license_url="http://www.opendefinition.org/licenses/cc-zero",
        tags=["census", "demographics", "housing", "population", "income", "education", "acs", "american-community-survey"],
        groups=["Demographics", "Housing"],
        resources=[
            Resource("r051", "ACS 5-Year Data (CSV by State)", "CSV",
                     "https://api.census.gov/data/2022/acs/acs5",
                     None, "text/csv", "2023-12-07", "2023-12-07", "API download"),
            Resource("r052", "Geographic Summary Files (ZIP)", "ZIP",
                     "https://www2.census.gov/programs-surveys/acs/summary_file/2022/",
                     1_073_741_824, "application/zip", "2023-12-07", "2023-12-07", "All geographies"),
            Resource("r053", "TIGER/Line Shapefiles", "SHP",
                     "https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html",
                     None, "application/zip", "2023-01-01", "2023-12-31", "Census tract boundaries"),
        ],
        metadata_created="2012-01-01T00:00:00",
        metadata_modified="2023-12-07T00:00:00",
        num_resources=3, num_tags=8, state="active",
        url="https://catalog.data.gov/dataset/acs-5-year-estimates-demographic-and-housing",
        extras={"bureau_code": "006:07", "spatial": "United States", "temporal": "2018-2022"},
    ),
    Dataset(
        id="g1h46i92-7j8k-9f35-3g77-i8j3f0h1k9g8",
        name="usgs-earthquake-hazards-program",
        title="USGS Earthquake Hazards Program — Real-Time Earthquake Data",
        notes="Real-time earthquake information from the USGS Earthquake Hazards Program. Includes all earthquakes detected globally (M1.0+) and in the US (M0.0+), updated every minute. Provides GeoJSON, CSV, and KML feeds for integration with mapping applications.",
        organization="U.S. Geological Survey",
        org_type="federal-government",
        license_title="U.S. Government Work",
        license_url="http://www.usa.gov/publicdomain/label/1.0/",
        tags=["earthquake", "seismic", "geology", "usgs", "natural-disaster", "real-time", "geoscience"],
        groups=["Geosciences", "Emergency Management"],
        resources=[
            Resource("r061", "All Earthquakes Past 30 Days (GeoJSON)", "GeoJSON",
                     "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
                     None, "application/json", "2015-01-01", "2024-11-15", "Auto-updated"),
            Resource("r062", "Significant Earthquakes Past 30 Days (CSV)", "CSV",
                     "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_month.csv",
                     None, "text/csv", "2015-01-01", "2024-11-15", "M4.5+ only"),
            Resource("r063", "Historical Earthquake Catalog (1900-present)", "CSV",
                     "https://earthquake.usgs.gov/data/comcat/",
                     2_147_483_648, "text/csv", "2010-01-01", "2024-11-01", "ComCat catalog"),
        ],
        metadata_created="2014-03-01T00:00:00",
        metadata_modified="2024-11-15T23:59:00",
        num_resources=3, num_tags=7, state="active",
        url="https://catalog.data.gov/dataset/usgs-earthquake-hazards-program",
        extras={"bureau_code": "010:12", "spatial": "GLOBAL", "publisher": "USGS"},
    ),
    Dataset(
        id="h2i57j03-8k9l-0g46-4h88-j9k4g1i2l0h9",
        name="fda-food-enforcement-reports",
        title="FDA Food Enforcement Reports — Recalls, Market Withdrawals & Safety Alerts",
        notes="Food and Drug Administration enforcement reports covering food recalls, market withdrawals, and safety alerts. Data includes product descriptions, recall classification (Class I/II/III), recalling firm, and distribution pattern.",
        organization="Food and Drug Administration",
        org_type="federal-government",
        license_title="U.S. Government Work",
        license_url="http://www.usa.gov/publicdomain/label/1.0/",
        tags=["fda", "food-safety", "recalls", "enforcement", "public-health", "consumer-protection"],
        groups=["Health", "Food Safety"],
        resources=[
            Resource("r071", "Food Recalls 2024 (JSON)", "JSON",
                     "https://api.fda.gov/food/enforcement.json?limit=1000",
                     None, "application/json", "2012-01-01", "2024-11-10", "OpenFDA API"),
            Resource("r072", "Historical Enforcement CSV", "CSV",
                     "https://www.fda.gov/media/91458/download",
                     10_485_760, "text/csv", "2012-01-01", "2024-11-10", "All years"),
            Resource("r073", "OpenFDA API Documentation", "HTML",
                     "https://open.fda.gov/apis/food/enforcement/",
                     None, "text/html", "2014-06-01", "2024-01-01", "API docs"),
        ],
        metadata_created="2014-06-01T00:00:00",
        metadata_modified="2024-11-10T12:00:00",
        num_resources=3, num_tags=6, state="active",
        url="https://catalog.data.gov/dataset/fda-food-enforcement-reports",
        extras={"bureau_code": "009:10", "program_code": "009:010", "publisher": "FDA"},
    ),
]

MOCK_STATS = {
    "generated_at": datetime.utcnow().isoformat() + "Z",
    "query": "*:*  (全站统计)",
    "api_total_datasets": 308_472,
    "top_formats": {
        "CSV":      147_209,
        "HTML":      89_341,
        "PDF":       72_104,
        "JSON":      45_882,
        "XLSX":      38_751,
        "ZIP":       31_203,
        "XML":       22_891,
        "GeoJSON":   18_445,
        "SHP":       14_328,
        "KML":        9_112,
        "API":        8_754,
        "NetCDF":     6_321,
        "DOCX":       4_892,
        "TXT":        4_103,
        "ODP":        2_567,
    },
    "top_organizations": {
        "Census Bureau":                             12_841,
        "National Oceanic and Atmospheric Admin.":   11_203,
        "U.S. Geological Survey":                    10_872,
        "Department of Health and Human Services":    9_541,
        "NASA Goddard Space Flight Center":           8_932,
        "Environmental Protection Agency":            8_104,
        "Department of Transportation":               7_891,
        "Energy Information Administration":          7_203,
        "Department of Agriculture":                  6_892,
        "National Institutes of Health":              6_341,
        "Bureau of Labor Statistics":                 5_982,
        "Federal Emergency Management Agency":        5_714,
        "Food and Drug Administration":               5_102,
        "Federal Aviation Administration":            4_892,
        "Centers for Disease Control":                4_751,
    },
}


# ─── 打印工具 ──────────────────────────────────────────────────────────────────
def print_dataset_card(ds: Dataset, idx: int):
    fmt_names = list({r.format for r in ds.resources if r.format})
    fmt_str   = ", ".join(fmt_names) if fmt_names else "—"
    print(f"\n  [{idx:>3}] {ds.title}")
    print(f"        ID       : {ds.id}")
    print(f"        组织     : {ds.organization}")
    print(f"        许可证   : {ds.license_title or '—'}")
    print(f"        资源数   : {ds.num_resources}  ({fmt_str})")
    print(f"        标签     : {', '.join(ds.tags[:5])}{' …' if len(ds.tags)>5 else ''}")
    print(f"        修改     : {ds.metadata_modified}")
    print(f"        链接     : {ds.url}")


def print_detail(ds: Dataset):
    print(f"\n{DIVIDER}")
    print(f"  📦 数据集详情: {ds.title}")
    print(DIVIDER2)
    print(f"  组织     : {ds.organization} ({ds.org_type})")
    print(f"  许可证   : {ds.license_title}")
    print(f"  状态     : {ds.state}")
    print(f"  标签     : {', '.join(ds.tags)}")
    print(f"  分组     : {', '.join(ds.groups)}")
    print(f"  创建时间 : {ds.metadata_created}")
    print(f"  修改时间 : {ds.metadata_modified}")
    notes_short = ds.notes[:320] + "…" if len(ds.notes) > 320 else ds.notes
    print(f"\n  描述:\n    {notes_short}")
    if ds.extras:
        print(f"\n  附加字段:")
        for k, v in ds.extras.items():
            print(f"    {k:<20}: {v}")
    print(f"\n  ┌{'─'*33}┬{'─'*8}┬{'─'*14}┬{'─'*28}┐")
    print(f"  │ {'资源名称':<31} │ {'格式':<6} │ {'大小':>12} │ {'最后修改':<26} │")
    print(f"  ├{'─'*33}┼{'─'*8}┼{'─'*14}┼{'─'*28}┤")
    for res in ds.resources:
        name_s = res.name[:31] if len(res.name) > 31 else res.name
        size_s = f"{res.size:,}" if res.size else "streaming"
        mod_s  = res.last_modified or "—"
        print(f"  │ {name_s:<31} │ {res.format:<6} │ {size_s:>12} │ {mod_s:<26} │")
    print(f"  └{'─'*33}┴{'─'*8}┴{'─'*14}┴{'─'*28}┘")
    print(DIVIDER)


def print_stats(stats: dict):
    print(f"\n{DIVIDER}")
    print(f"  📊 data.gov 全站统计报告")
    print(f"  生成时间 : {stats['generated_at']}")
    print(f"  数据集总量: {stats['api_total_datasets']:,}")
    print(DIVIDER2)
    max_fmt = max(stats["top_formats"].values())
    print("  TOP 15 资源格式:")
    for fmt, cnt in stats["top_formats"].items():
        bar = "█" * int(cnt / max_fmt * 35)
        print(f"    {fmt:<12} {cnt:>8,}  {bar}")
    print(DIVIDER2)
    max_org = max(stats["top_organizations"].values())
    print("  TOP 15 贡献组织:")
    for org, cnt in stats["top_organizations"].items():
        bar = "█" * int(cnt / max_org * 30)
        print(f"    {org:<46} {cnt:>6,}  {bar}")
    print(DIVIDER)


# ─── 主演示 ───────────────────────────────────────────────────────────────────
def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\n{DIVIDER}")
    print(f"   🇺🇸  data.gov 开放数据爬虫  |  基于 CKAN API")
    print(f"   https://catalog.data.gov/api/3")
    print(DIVIDER)

    # ─── 场景 1: 数据集列表 ──────────────────────────────────────────────────
    print(f"\n{DIVIDER2}")
    print("  【场景 1】 搜索结果 — 典型政府开放数据集（模拟 8 条）")
    print(DIVIDER2)
    for i, ds in enumerate(MOCK_DATASETS, 1):
        print_dataset_card(ds, i)

    # ─── 场景 2: 详情展示 ────────────────────────────────────────────────────
    print(f"\n{DIVIDER2}")
    print("  【场景 2】 数据集详情 — COVID-19 案例监控数据")
    print_detail(MOCK_DATASETS[1])

    print(f"\n{DIVIDER2}")
    print("  【场景 3】 数据集详情 — NASA 全球气温变化")
    print_detail(MOCK_DATASETS[0])

    # ─── 场景 3: 统计 ────────────────────────────────────────────────────────
    print(f"\n{DIVIDER2}")
    print("  【场景 4】 全站统计报告")
    print_stats(MOCK_STATS)

    # ─── 输出文件 ────────────────────────────────────────────────────────────
    print(f"\n{DIVIDER2}")
    print("  💾 写入结构化输出文件...")

    # JSON — 完整数据集列表
    result_dict = {
        "scraper": "DataGovScraper v1.0",
        "api_base": "https://catalog.data.gov/api/3",
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "total_available": 308_472,
        "fetched_count": len(MOCK_DATASETS),
        "datasets": [
            {**{k: v for k, v in vars(ds).items() if k not in ("resources",)},
             "resources": [vars(r) for r in ds.resources]}
            for ds in MOCK_DATASETS
        ],
    }
    p1 = writer.write_json(result_dict, f"datasets_{ts}.json")

    # JSON — 统计
    p2 = writer.write_json(MOCK_STATS, f"stats_{ts}.json")

    # CSV — 数据集
    p3 = writer.write_datasets_csv(MOCK_DATASETS, f"datasets_{ts}.csv")

    # CSV — 资源
    p4 = writer.write_resources_csv(MOCK_DATASETS, f"resources_{ts}.csv")

    # Excel
    p5 = writer.write_excel(MOCK_DATASETS, f"report_{ts}.xlsx")

    # ─── 汇总 ────────────────────────────────────────────────────────────────
    total_resources = sum(len(ds.resources) for ds in MOCK_DATASETS)
    total_tags      = sum(ds.num_tags for ds in MOCK_DATASETS)
    orgs = {ds.organization for ds in MOCK_DATASETS}

    print(f"""
{DIVIDER}
  ✅  演示完成！
{DIVIDER2}
  数据集数量   : {len(MOCK_DATASETS)}
  资源数量     : {total_resources}
  标签总数     : {total_tags}
  涉及组织     : {len(orgs)} 个
    {chr(10).join('    • ' + o for o in sorted(orgs))}
{DIVIDER2}
  输出文件:
    📄 {p1}
    📄 {p2}
    📊 {p3}
    📊 {p4}
    📗 {p5 or '（Excel 未生成，需安装 openpyxl）'}
{DIVIDER}
""")


if __name__ == "__main__":
    main()
