"""
=============================================================
  Excel 透视表规则 → Python 完整解析教程
  作者: Claude  |  日期: 2024
=============================================================

【透视表核心概念速查】

  行字段 (Rows)    → groupby() 的第一批 key，决定"竖向分组"
  列字段 (Columns) → pivot_table() 的 columns 参数，决定"横向展开"
  值字段 (Values)  → pivot_table() 的 values + aggfunc 参数
  筛选字段 (Filter)→ 提前用 df[condition] 过滤数据
  排序             → sort_values() / reindex()
  汇总(小计/总计)  → groupby().sum()  +  df.sum()

=============================================================
"""

import pandas as pd
import numpy as np

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 0: 读取原始数据
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# df = pd.read_excel('透视表案例教程.xlsx', sheet_name='原始数据')
# 此处用代码重建数据，与Excel文件一致
np.random.seed(42)
n = 200
data = {
    '订单ID':   [f'ORD-{i:04d}' for i in range(1, n+1)],
    '月份':     np.random.choice([f'2024-{m:02d}' for m in range(1, 13)], n),
    '大区':     np.random.choice(['华北', '华南', '华东', '西南'], n),
    '产品类别': np.random.choice(['电子产品', '服装', '食品', '家居'], n),
    '销售渠道': np.random.choice(['线上', '线下'], n),
    '销售额':   np.random.randint(500, 50000, n),
    '销售量':   np.random.randint(1, 200, n),
    '销售员':   np.random.choice(['张三','李四','王五','赵六','陈七'], n),
}
df = pd.DataFrame(data)

print("=" * 60)
print("STEP 0: 原始数据预览")
print("=" * 60)
print(df.head(3).to_string())
print(f"\n数据维度: {df.shape[0]} 行 × {df.shape[1]} 列")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 1: 筛选字段（对应 Excel 透视表的"报表筛选"区域）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 规则: 月份 → 只保留 2024年上半年 (1月~6月)

h1_months = [f'2024-{m:02d}' for m in range(1, 7)]   # ['2024-01', ..., '2024-06']
df_h1 = df[df['月份'].isin(h1_months)].copy()

print("\n" + "=" * 60)
print("STEP 1: 筛选字段 → 上半年数据")
print("=" * 60)
print(f"筛选前: {len(df)} 行  →  筛选后: {len(df_h1)} 行")
print(f"保留的月份: {sorted(df_h1['月份'].unique())}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 2: 构建透视表（核心）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 规则:
#   行字段  → 大区, 产品类别（多级行）
#   列字段  → 销售渠道（线上 / 线下）
#   值字段1 → 销售额, 聚合方式: sum（求和）
#   值字段2 → 销售量, 聚合方式: mean（均值）

# ── 2a. 销售额透视表（SUM）──
pivot_sales = df_h1.pivot_table(
    values   = '销售额',        # 值字段
    index    = ['大区', '产品类别'],  # 行字段（多级）
    columns  = '销售渠道',      # 列字段
    aggfunc  = 'sum',           # 聚合方式
    fill_value = 0              # 空值填0（没有该组合的数据）
)

# ── 2b. 销售量透视表（MEAN）──
pivot_qty = df_h1.pivot_table(
    values   = '销售量',
    index    = ['大区', '产品类别'],
    columns  = '销售渠道',
    aggfunc  = 'mean',
    fill_value = 0
)

# 补全缺失的列（防御性编程）
for col in ['线上', '线下']:
    if col not in pivot_sales.columns:
        pivot_sales[col] = 0
    if col not in pivot_qty.columns:
        pivot_qty[col]   = 0

print("\n" + "=" * 60)
print("STEP 2: 透视表（基础结构）")
print("=" * 60)
print("\n── 销售额(SUM) 前6行 ──")
print(pivot_sales.head(6).to_string())
print("\n── 销售量(MEAN) 前6行（保留1位小数）──")
print(pivot_qty.round(1).head(6).to_string())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 3: 新增合计列（行方向汇总）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 规则: 销售额合计 = 线上 + 线下（对每一行做横向求和）

pivot_sales['合计'] = pivot_sales['线上'] + pivot_sales['线下']

print("\n" + "=" * 60)
print("STEP 3: 新增销售额合计列")
print("=" * 60)
print(pivot_sales.head(6).to_string())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 4: 计算大区小计（分组汇总）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 规则: 每个大区，对下属所有产品类别做小计
#   销售额小计 → sum
#   销售量小计 → mean（均值的均值，实际业务中可按需调整）

region_subtotals_sales = pivot_sales.groupby(level='大区').sum()
region_subtotals_qty   = pivot_qty.groupby(level='大区').mean()

print("\n" + "=" * 60)
print("STEP 4: 大区小计")
print("=" * 60)
print("\n── 大区销售额小计 ──")
print(region_subtotals_sales.to_string())
print("\n── 大区销售量均值小计 ──")
print(region_subtotals_qty.round(1).to_string())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 5: 排序（按大区销售额合计降序）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 规则: 大区按"销售额合计"从高到低排列

region_order = (
    region_subtotals_sales['合计']
    .sort_values(ascending=False)
    .index.tolist()
)
print("\n" + "=" * 60)
print("STEP 5: 大区排序（销售额合计降序）")
print("=" * 60)
print("排序结果:", region_order)
print("\n各大区销售额合计:")
print(region_subtotals_sales['合计'].sort_values(ascending=False).to_string())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 6: 计算总计行
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

grand_sales = pivot_sales.sum()
grand_qty   = pivot_qty.mean()

print("\n" + "=" * 60)
print("STEP 6: 全部总计")
print("=" * 60)
print("销售额总计:")
print(grand_sales.to_string())
print("\n销售量均值总计:")
print(grand_qty.round(1).to_string())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 7: 组装最终报表（按排序后的顺序拼接）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 最终表结构：
#   [大区] [产品类别] [线上_销售额] [线下_销售额] [销售额合计] [线上_均销售量] [线下_均销售量]

rows = []
for region in region_order:
    cats = pivot_sales.loc[region].index.tolist()
    for cat in cats:
        rows.append({
            '大区':      region,
            '产品类别':  cat,
            '线上_销售额': int(pivot_sales.loc[(region, cat), '线上']),
            '线下_销售额': int(pivot_sales.loc[(region, cat), '线下']),
            '销售额合计':  int(pivot_sales.loc[(region, cat), '合计']),
            '线上_均销售量': round(float(pivot_qty.loc[(region, cat), '线上']), 1),
            '线下_均销售量': round(float(pivot_qty.loc[(region, cat), '线下']), 1),
            '行类型': '明细',
        })
    # 小计行
    st = region_subtotals_sales.loc[region]
    sq = region_subtotals_qty.loc[region]
    rows.append({
        '大区':      f'【{region} 小计】',
        '产品类别':  '',
        '线上_销售额': int(st['线上']),
        '线下_销售额': int(st['线下']),
        '销售额合计':  int(st['合计']),
        '线上_均销售量': round(float(sq['线上']), 1),
        '线下_均销售量': round(float(sq['线下']), 1),
        '行类型': '小计',
    })

# 总计行
rows.append({
    '大区':      '【全部总计】',
    '产品类别':  '',
    '线上_销售额': int(grand_sales['线上']),
    '线下_销售额': int(grand_sales['线下']),
    '销售额合计':  int(grand_sales['合计']),
    '线上_均销售量': round(float(grand_qty['线上']), 1),
    '线下_均销售量': round(float(grand_qty['线下']), 1),
    '行类型': '总计',
})

final_df = pd.DataFrame(rows)

print("\n" + "=" * 60)
print("STEP 7: 最终透视报表")
print("=" * 60)
display_cols = ['大区','产品类别','线上_销售额','线下_销售额','销售额合计','线上_均销售量','线下_均销售量']
pd.set_option('display.max_rows', 50)
pd.set_option('display.width', 120)
print(final_df[display_cols].to_string(index=False))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STEP 8: 导出到 Excel（带格式）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# （此步骤已在 create_pivot_demo2.py 中完成，此处演示简洁版）

output_path = 'pivot_output_simple.xlsx'
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    final_df[display_cols].to_excel(writer, sheet_name='透视结果', index=False)
print(f"\n✅ 简洁版已导出: {output_path}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 【附录】透视表规则解析速查模板
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
拿到同事的透视表时，按以下步骤逐一翻译：

1. 看【筛选区域】
      Excel:  报表筛选 → 选择了哪些值
      Python: df = df[df['字段'].isin([...])]

2. 看【行标签区域】
      Excel:  拖入了哪些字段（从上到下）
      Python: index = ['字段1', '字段2', ...]  （多级行用列表）

3. 看【列标签区域】
      Excel:  拖入了哪些字段
      Python: columns = '字段'

4. 看【值区域】
      Excel:  字段名 + 值汇总方式（求和/计数/平均值/最大值…）
      Python: aggfunc = 'sum' / 'count' / 'mean' / 'max' / 'min'
      多个值: aggfunc = {'销售额': 'sum', '销售量': 'mean'}

5. 看【排序】
      Excel:  列标题旁的排序箭头
      Python: .sort_values('列名', ascending=False)

6. 看【汇总行/列】
      Excel:  "总计"行/列是否显示
      Python: .groupby(level=0).sum()  # 组内小计
              df.sum()                 # 全局总计

常用 aggfunc 对照表:
   Excel 求和    → aggfunc='sum'
   Excel 计数    → aggfunc='count'
   Excel 平均值  → aggfunc='mean'
   Excel 最大值  → aggfunc='max'
   Excel 最小值  → aggfunc='min'
   Excel 乘积    → aggfunc=np.prod
   Excel 非空计数→ aggfunc='count'（pandas默认忽略NaN）
"""
