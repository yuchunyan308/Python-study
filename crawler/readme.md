
## 🌐 世界银行 Open Data 爬虫 — 完整说明

### 📦 依赖安装
```bash
pip install requests pandas openpyxl
```

---

### 🏗️ 代码架构

```
worldbank_scraper.py
├── 常量配置区              ← API地址、目标国家、18个指标分类、年份范围
├── 数据类（dataclass）
│   ├── IndicatorRecord    ← 单条观测（国家/指标/年份/数值）
│   ├── CountryInfo        ← 国家元数据（区域、收入等级、首都）
│   └── ScraperResult      ← 采集结果容器
├── HTTP工具层
│   ├── build_session()    ← 带重试策略（3次，指数退避）的Session
│   └── fetch_all_pages()  ← 自动翻页合并
├── WorldBankScraper 类
│   ├── fetch_country_info()   ← 采集国家元数据
│   ├── fetch_indicator()      ← 单指标 × 多国家批量拉取
│   └── run()                  ← 主调度流程
├── generate_mock_data()   ← 网络不通时自动切换演示模式
├── clean_data()           ← 长格式 + 宽格式 DataFrame 清洗
├── compute_summary()      ← 按分类统计摘要
├── save_outputs()         ← 多格式输出（JSON/CSV×2/Excel×4Sheet/快照）
└── print_report()         ← 控制台可视化报表
```

---

### 📊 采集指标（18项，5大领域）

| 领域 | 指标举例 |
|------|---------|
| 经济 | GDP、人均GDP、CPI通胀率、出口占比 |
| 教育 | 识字率、小学/高等入学率、教育支出占GDP |
| 健康 | 预期寿命、儿童死亡率、卫生支出、生育率 |
| 环境 | 人均CO₂、能源使用、森林覆盖率、淡水提取 |
| 人口 | 总人口、城镇化率 |

---

### 📁 输出文件（5种）

| 文件 | 说明 |
|------|------|
| `raw_records.json` | 原始完整数据 + 元信息 |
| `data_long.csv` | 长格式（每行一条观测） |
| `data_wide.csv` | 宽格式（每行一个国家-年份，列为指标） |
| `worldbank_data.xlsx` | Excel（4个Sheet：长/宽格式、统计摘要、国家信息） |
| `latest_snapshot.csv` | 各国各指标最新年份快照 |

---

### ⚙️ 关键工程特性

- **自动翻页**：解析 `meta.pages` 字段，循环直至最后一页
- **礼貌延时**：每次请求间隔 0.3s，避免触发限流
- **重试策略**：429/5xx 自动退避重试，最多 3 次
- **网络离线降级**：API 不通时自动生成结构一致的仿真演示数据
- **中文注释 + 类型注解**：全面类型标注，可直接二次开发
