构建一个完整的 data.gov 爬虫项目，使用其官方 CKAN API，涵盖搜索、详情、资源下载、结构化输出等全功能。

以下是完整说明：

---

## 🇺🇸 data.gov 爬虫项目结构

### 📁 三个核心文件

| 文件 | 作用 |
|---|---|
| `datagov_scraper.py` | 核心库：API 客户端、数据模型、输出工具 |
| `main.py` | CLI 入口，支持 4 个子命令 |
| `demo_offline.py` | 离线演示，含真实结构的模拟数据 |

---

### 🏗️ 架构设计

**`datagov_scraper.py` 分四层：**

1. **数据模型层** — `@dataclass`：`Resource` → `Dataset` → `SearchResult`，类型清晰，支持序列化
2. **HTTP 层** — `_make_session()`：自动重试（5次）、退避策略、User-Agent，应对 429/5xx
3. **CKAN API 客户端** — `DataGovClient`：
   - `search_datasets()` / `iter_datasets()` — 分页迭代，支持 Solr 全文 + `fq` 过滤
   - `get_dataset()` — 单条详情
   - `facet_formats()` / `facet_organizations()` — Facet 统计聚合
4. **输出层** — `OutputWriter`：JSON / CSV / Excel（3个 Sheet）三路并行输出

---

### 🖥️ CLI 使用方法

```bash
pip install requests openpyxl

# 关键字搜索，输出全格式
python main.py search --query "climate change" --max 50 --format all

# 过滤 NASA 数据，只输出 CSV
python main.py search --fq "organization:nasa-gov" --max 30 --format csv

# 按资源格式过滤（只要 CSV 格式的数据集）
python main.py search --fq "res_format:CSV" --max 20

# 获取单个数据集完整详情
python main.py detail --id "global-surface-temperature-change"

# 全站格式 / 组织统计报告
python main.py stats --query "energy"

# 一键运行完整演示（6个场景）
python main.py demo
```

---

### 📦 输出文件说明

| 文件 | 内容 |
|---|---|
| `datasets_*.json` | 完整嵌套结构（含所有资源字段） |
| `datasets_*.csv` | 数据集摘要表（14列） |
| `resources_*.csv` | 所有资源展开表（每资源一行） |
| `report_*.xlsx` | 3个 Sheet：数据集/资源/标签频次 |
| `stats_*.json` | 格式分布 + 组织 Top 15 统计 |
