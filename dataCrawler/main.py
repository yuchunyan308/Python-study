"""
data.gov 爬虫 - 主入口
======================
运行示例:
    python main.py search --query "climate change" --max 50
    python main.py search --query "covid" --format CSV --max 100
    python main.py detail --id "7fed2a3b-1678-4be5-8df0-b4e20b11c1f8"
    python main.py stats --query "energy"
    python main.py demo           # 运行完整演示（多场景）
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List

# 将当前目录加入路径
sys.path.insert(0, str(Path(__file__).parent))

from datagov_scraper import (
    DataGovScraper,
    Dataset,
    OutputWriter,
    SearchResult,
    log,
)

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)
writer = OutputWriter(OUTPUT_DIR)


# ─── 格式化打印 ───────────────────────────────────────────────────────────────
DIVIDER  = "=" * 72
DIVIDER2 = "-" * 72


def print_banner():
    print(f"""
{DIVIDER}
   🇺🇸  data.gov 开放数据爬虫  |  基于 CKAN API
   https://catalog.data.gov/api/3
{DIVIDER}
""")


def print_dataset_card(ds: Dataset, index: int = 0):
    """在终端漂亮地打印单个数据集摘要。"""
    print(f"\n  [{index:>3}] {ds.title}")
    print(f"        ID        : {ds.id}")
    print(f"        组织      : {ds.organization or '—'}")
    print(f"        许可证    : {ds.license_title or '—'}")
    print(f"        资源数    : {ds.num_resources}  |  标签数: {ds.num_tags}")
    print(f"        修改时间  : {ds.metadata_modified or '—'}")
    if ds.tags:
        tag_str = ", ".join(ds.tags[:6]) + ("…" if len(ds.tags) > 6 else "")
        print(f"        标签      : {tag_str}")
    print(f"        链接      : {ds.url}")


def print_search_summary(result: SearchResult):
    print(f"\n{DIVIDER}")
    print(f"  搜索完成")
    print(f"  查询       : {result.query}")
    print(f"  API 总命中 : {result.total_count:,}")
    print(f"  已获取     : {result.fetched_count}")
    print(f"  耗时       : {result.fetch_time_sec}s")
    print(f"{DIVIDER}")


def print_stats(stats: dict):
    print(f"\n{DIVIDER}")
    print(f"  📊 统计报告 — {stats['query']}")
    print(f"  生成时间: {stats['generated_at']}")
    print(f"{DIVIDER2}")
    print("  TOP 资源格式:")
    for fmt, cnt in list(stats["top_formats"].items())[:15]:
        bar = "█" * min(int(cnt / max(stats["top_formats"].values()) * 40), 40)
        print(f"    {fmt:<15} {cnt:>8,}  {bar}")
    print(f"{DIVIDER2}")
    print("  TOP 贡献组织:")
    for org, cnt in list(stats["top_organizations"].items())[:15]:
        bar = "█" * min(int(cnt / max(stats["top_organizations"].values()) * 40), 40)
        print(f"    {org:<35} {cnt:>6,}  {bar}")
    print(DIVIDER)


def print_dataset_detail(ds: Dataset):
    print(f"\n{DIVIDER}")
    print(f"  📦 数据集详情")
    print(DIVIDER2)
    print(f"  标题   : {ds.title}")
    print(f"  ID     : {ds.id}")
    print(f"  Slug   : {ds.name}")
    print(f"  组织   : {ds.organization} ({ds.org_type})")
    print(f"  许可证 : {ds.license_title}")
    print(f"  状态   : {ds.state}")
    print(f"  创建   : {ds.metadata_created}")
    print(f"  修改   : {ds.metadata_modified}")
    print(f"  链接   : {ds.url}")
    if ds.notes:
        notes_short = ds.notes[:300] + "…" if len(ds.notes) > 300 else ds.notes
        print(f"\n  描述:\n    {notes_short}")
    print(f"\n  标签 ({ds.num_tags}): {', '.join(ds.tags) or '—'}")
    print(f"  分组: {', '.join(ds.groups) or '—'}")
    if ds.extras:
        print(f"\n  附加字段:")
        for k, v in list(ds.extras.items())[:10]:
            print(f"    {k}: {v}")
    print(f"\n  资源 ({ds.num_resources}):")
    print(f"  {'名称':<35} {'格式':<10} {'大小':>12}  URL")
    print(f"  {'-'*35} {'-'*10} {'-'*12}  {'-'*30}")
    for res in ds.resources[:20]:
        size_str = f"{res.size:,}" if res.size else "—"
        name_short = res.name[:33] if len(res.name) > 33 else res.name
        url_short  = res.url[:50] if len(res.url) > 50 else res.url
        print(f"  {name_short:<35} {res.format:<10} {size_str:>12}  {url_short}")
    if ds.num_resources > 20:
        print(f"  … 还有 {ds.num_resources - 20} 个资源")
    print(DIVIDER)


# ─── 场景函数 ─────────────────────────────────────────────────────────────────
def run_search(
    query: str,
    fq: str = "",
    max_results: int = 50,
    output_fmt: str = "all",
    tag: str = "",
):
    scraper = DataGovScraper(rate_limit_delay=0.4)
    result  = scraper.search(query=query, fq=fq, max_results=max_results)

    print_search_summary(result)
    for i, ds in enumerate(result.datasets, 1):
        print_dataset_card(ds, i)

    # 时间戳用于文件名
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = query.replace(" ", "_")[:30]
    prefix = f"{slug}_{ts}"

    # 构建可序列化的字典
    result_dict = {
        "query":         result.query,
        "total_count":   result.total_count,
        "fetched_count": result.fetched_count,
        "fetch_time_sec":result.fetch_time_sec,
        "datasets": [
            {**{k: v for k, v in vars(ds).items() if k != "resources"},
             "resources": [vars(r) for r in ds.resources]}
            for ds in result.datasets
        ],
    }

    paths = []
    if output_fmt in ("all", "json"):
        paths.append(writer.write_json(result_dict, f"{prefix}_datasets.json"))
    if output_fmt in ("all", "csv"):
        paths.append(writer.write_datasets_csv(result.datasets, f"{prefix}_datasets.csv"))
        paths.append(writer.write_resources_csv(result.datasets, f"{prefix}_resources.csv"))
    if output_fmt in ("all", "excel"):
        p = writer.write_excel(result.datasets, f"{prefix}_report.xlsx")
        if p:
            paths.append(p)

    print(f"\n  💾 输出文件:")
    for p in paths:
        if p:
            print(f"    {p}")
    return result


def run_detail(dataset_id: str):
    scraper = DataGovScraper()
    ds = scraper.get_dataset_detail(dataset_id)
    if not ds:
        print(f"  ❌ 未找到数据集: {dataset_id}")
        return

    print_dataset_detail(ds)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = writer.write_json(
        {**{k: v for k, v in vars(ds).items() if k != "resources"},
         "resources": [vars(r) for r in ds.resources]},
        f"detail_{ds.name}_{ts}.json",
    )
    print(f"\n  💾 已保存: {path}")


def run_stats(query: str = "*:*"):
    scraper = DataGovScraper()
    stats   = scraper.generate_stats(query)
    print_stats(stats)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = writer.write_json(stats, f"stats_{ts}.json")
    print(f"\n  💾 统计 JSON 已保存: {path}")


def run_demo():
    """
    完整演示：运行 4 个搜索场景 + 统计报告。
    （网络受限时可能部分失败，属正常现象。）
    """
    print_banner()

    # ─── 场景 1: 气候变化数据 ────────────────────────────────────────────
    print("\n【场景 1】 关键字搜索: climate change（最多 20 条）")
    r1 = run_search("climate change", max_results=20, output_fmt="all")

    # ─── 场景 2: NASA 组织数据 ───────────────────────────────────────────
    print("\n【场景 2】 组织过滤: NASA（最多 15 条）")
    r2 = run_search("*:*", fq="organization:nasa-gov", max_results=15, output_fmt="csv")

    # ─── 场景 3: CSV 格式资源 ────────────────────────────────────────────
    print("\n【场景 3】 资源格式过滤: CSV 格式数据集（最多 15 条）")
    r3 = run_search("*:*", fq="res_format:CSV", max_results=15, output_fmt="json")

    # ─── 场景 4: 新冠疫情数据 ────────────────────────────────────────────
    print("\n【场景 4】 关键字搜索: COVID-19（最多 20 条）")
    r4 = run_search("COVID-19", max_results=20, output_fmt="all")

    # ─── 场景 5: 第一个数据集的详情 ─────────────────────────────────────
    if r1.datasets:
        print(f"\n【场景 5】 获取详情: {r1.datasets[0].name}")
        run_detail(r1.datasets[0].name)

    # ─── 场景 6: 全站统计 ────────────────────────────────────────────────
    print("\n【场景 6】 全站统计报告")
    run_stats("*:*")

    # ─── 汇总 ────────────────────────────────────────────────────────────
    total_ds = sum(r.fetched_count for r in [r1, r2, r3, r4])
    print(f"""
{DIVIDER}
  ✅  演示完成！
  共获取数据集: {total_ds}
  输出目录    : {OUTPUT_DIR.resolve()}
{DIVIDER}
""")


# ─── CLI ──────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="data.gov 开放数据爬虫",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py demo
  python main.py search --query "climate" --max 30 --format all
  python main.py search --query "*:*" --fq "organization:nasa-gov" --max 20
  python main.py detail --id "some-dataset-name-or-id"
  python main.py stats --query "energy"
""",
    )
    sub = p.add_subparsers(dest="command", required=True)

    # search
    ps = sub.add_parser("search", help="搜索数据集")
    ps.add_argument("--query",  "-q", default="*:*",  help="Solr 查询字符串 (默认: *:*)")
    ps.add_argument("--fq",           default="",     help="过滤查询，如 'organization:nasa-gov'")
    ps.add_argument("--max",    "-n", type=int, default=50, help="最大获取条数 (默认: 50)")
    ps.add_argument("--format", "-f", default="all",
                    choices=["all", "json", "csv", "excel"], help="输出格式")

    # detail
    pd = sub.add_parser("detail", help="获取单个数据集详情")
    pd.add_argument("--id", required=True, help="数据集 name 或 UUID")

    # stats
    pst = sub.add_parser("stats", help="生成统计报告")
    pst.add_argument("--query", "-q", default="*:*", help="过滤范围")

    # demo
    sub.add_parser("demo", help="运行完整演示（多场景）")

    return p


def main():
    parser = build_parser()
    args   = parser.parse_args()
    print_banner()

    if args.command == "demo":
        run_demo()
    elif args.command == "search":
        run_search(
            query=args.query,
            fq=args.fq,
            max_results=args.max,
            output_fmt=args.format,
        )
    elif args.command == "detail":
        run_detail(args.id)
    elif args.command == "stats":
        run_stats(args.query)


if __name__ == "__main__":
    main()
