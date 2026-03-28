#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================
  通用电影数据爬虫脚本
  目标：以 books.toscrape.com（合法练习站点）为示例，
        演示完整的爬虫工程实践。
  同时提供豆瓣 Top250 的选择器模板，可按需切换。
========================================================
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import logging
from datetime import datetime

# ──────────────────────────────────────────────────────
# 1. 日志配置
#    使用 logging 模块替代 print，便于记录运行状态
# ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────
# 2. 全局配置区（按需修改）
# ──────────────────────────────────────────────────────
CONFIG = {
    # ---- 示例站点：books.toscrape.com（专为爬虫练习设计，合法免费）----
    "base_url": "https://books.toscrape.com/catalogue/",
    "start_url": "https://books.toscrape.com/catalogue/page-1.html",
    "next_page_selector": "li.next a",          # 下一页按钮的 CSS 选择器
    "item_selector": "article.product_pod",     # 每条数据条目的 CSS 选择器
    "max_pages": 5,                             # 最多爬取页数（0 = 不限制）
    "output_file": "books_data.csv",            # 输出 CSV 文件名
    "request_delay": (1.0, 2.5),               # 随机延迟范围（秒），降低服务器压力
    "timeout": 10,                             # 请求超时时间（秒）

    # ---- 豆瓣 Top250 模板（注释状态，使用时取消注释并替换上方配置）----
    # "base_url":           "https://movie.douban.com/top250",
    # "start_url":          "https://movie.douban.com/top250?start=0",
    # "next_page_selector": "span.next a",
    # "item_selector":      "div.item",
    # "max_pages":          10,
    # "output_file":        "douban_top250.csv",
}

# ──────────────────────────────────────────────────────
# 3. 请求头配置
#    模拟真实浏览器，减少被反爬拦截的概率
# ──────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}


# ──────────────────────────────────────────────────────
# 4. 网络请求函数
# ──────────────────────────────────────────────────────
def fetch_page(url: str, retries: int = 3) -> BeautifulSoup | None:
    """
    发送 GET 请求并返回解析后的 BeautifulSoup 对象。

    Args:
        url:     目标页面 URL
        retries: 失败时最大重试次数

    Returns:
        BeautifulSoup 对象；若全部失败则返回 None
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"请求页面（第 {attempt} 次）：{url}")

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=CONFIG["timeout"],
            )
            # 若状态码为 4xx / 5xx，主动抛出 HTTPError
            response.raise_for_status()

            # 指定编码，防止中文乱码
            response.encoding = response.apparent_encoding

            # 使用 lxml 解析器（速度更快），fallback 至 html.parser
            try:
                soup = BeautifulSoup(response.text, "lxml")
            except Exception:
                soup = BeautifulSoup(response.text, "html.parser")

            return soup

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP 错误：{e}（状态码 {response.status_code}）")
            if response.status_code in (403, 404):
                # 4xx 客户端错误，无需重试
                return None

        except requests.exceptions.ConnectionError:
            logger.warning("网络连接失败，等待后重试…")

        except requests.exceptions.Timeout:
            logger.warning(f"请求超时（{CONFIG['timeout']}s），等待后重试…")

        except requests.exceptions.RequestException as e:
            logger.error(f"未知请求错误：{e}")
            return None

        # 指数退避：每次重试等待时间翻倍
        wait_time = 2 ** attempt
        logger.info(f"等待 {wait_time}s 后重试…")
        time.sleep(wait_time)

    logger.error(f"已达最大重试次数，跳过：{url}")
    return None


# ──────────────────────────────────────────────────────
# 5. 数据解析函数（books.toscrape.com 版本）
# ──────────────────────────────────────────────────────
def parse_books(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """
    从页面 BeautifulSoup 对象中提取书籍信息。

    Args:
        soup:     已解析的页面对象
        base_url: 用于拼接相对链接的基础 URL

    Returns:
        包含书籍字典的列表，每本书含：title / price / rating / url
    """
    items = []

    # 找到所有书籍条目节点
    articles = soup.select(CONFIG["item_selector"])
    logger.info(f"本页找到 {len(articles)} 条数据")

    # 星级映射：将英文单词转为数字
    rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}

    for article in articles:
        try:
            # ---- 标题：从 <h3><a title="..."> 获取 ----
            title_tag = article.select_one("h3 a")
            title = title_tag["title"].strip() if title_tag else "N/A"

            # ---- 价格：提取文本并去除多余空白 ----
            price_tag = article.select_one("p.price_color")
            price = price_tag.get_text(strip=True) if price_tag else "N/A"

            # ---- 评分：从 class 属性中读取文字星级，转换为数字 ----
            rating_tag = article.select_one("p.star-rating")
            rating_word = rating_tag["class"][1] if rating_tag else "Zero"
            rating = rating_map.get(rating_word, 0)

            # ---- 详情页链接：将相对路径拼接为完整 URL ----
            relative_url = title_tag["href"] if title_tag else ""
            # 去掉多余的 "../" 前缀
            clean_relative = relative_url.replace("../", "")
            detail_url = base_url + clean_relative

            items.append({
                "title":  title,
                "price":  price,
                "rating": rating,
                "url":    detail_url,
            })

        except Exception as e:
            # 单条数据解析失败时，记录日志并继续处理其他条目
            logger.warning(f"解析单条数据时出错，已跳过：{e}")
            continue

    return items


# ──────────────────────────────────────────────────────
# 6. 豆瓣解析函数模板（注释状态）
#    使用豆瓣时，将 parse_books 替换为此函数即可
# ──────────────────────────────────────────────────────
# def parse_douban(soup: BeautifulSoup, base_url: str) -> list[dict]:
#     items = []
#     for item in soup.select(CONFIG["item_selector"]):
#         try:
#             title  = item.select_one("span.title").get_text(strip=True)
#             score  = item.select_one("span.rating_num").get_text(strip=True)
#             quote  = item.select_one("span.inq")
#             quote  = quote.get_text(strip=True) if quote else ""
#             url    = item.select_one("div.hd a")["href"]
#             items.append({"title": title, "score": score, "quote": quote, "url": url})
#         except Exception as e:
#             logger.warning(f"解析失败：{e}")
#     return items


# ──────────────────────────────────────────────────────
# 7. 翻页逻辑
# ──────────────────────────────────────────────────────
def get_next_url(soup: BeautifulSoup, current_url: str) -> str | None:
    """
    从当前页面中提取下一页 URL。

    Returns:
        下一页完整 URL；若已是最后一页则返回 None
    """
    next_tag = soup.select_one(CONFIG["next_page_selector"])
    if not next_tag:
        return None

    next_href = next_tag.get("href", "")

    # 处理相对路径：若不以 http 开头，则拼接当前目录
    if next_href.startswith("http"):
        return next_href
    else:
        # 取当前 URL 的目录部分再拼接
        base = current_url.rsplit("/", 1)[0]
        return f"{base}/{next_href}"


# ──────────────────────────────────────────────────────
# 8. CSV 写入函数
# ──────────────────────────────────────────────────────
def save_to_csv(data: list[dict], filename: str) -> None:
    """
    将爬取数据写入 CSV 文件。

    Args:
        data:     包含字典的列表
        filename: 输出文件路径
    """
    if not data:
        logger.warning("数据为空，未生成 CSV 文件")
        return

    fieldnames = list(data[0].keys())   # 以第一条数据的 key 作为表头

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        # utf-8-sig：写入 BOM 头，确保 Excel 正确显示中文
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()            # 写入表头行
        writer.writerows(data)          # 批量写入数据行

    logger.info(f"✅ 数据已保存至：{filename}（共 {len(data)} 条）")


# ──────────────────────────────────────────────────────
# 9. 主爬取流程
# ──────────────────────────────────────────────────────
def main():
    logger.info("=" * 50)
    logger.info("爬虫启动")
    logger.info(f"目标：{CONFIG['start_url']}")
    logger.info("=" * 50)

    all_data   = []            # 汇总所有页面数据
    current_url = CONFIG["start_url"]
    page_count  = 0

    while current_url:
        page_count += 1

        # 检查是否超过最大页数限制
        if CONFIG["max_pages"] > 0 and page_count > CONFIG["max_pages"]:
            logger.info(f"已达最大爬取页数 {CONFIG['max_pages']}，停止")
            break

        logger.info(f"── 第 {page_count} 页 ──")

        # 获取页面
        soup = fetch_page(current_url)
        if soup is None:
            logger.error("页面获取失败，终止爬取")
            break

        # 解析数据（切换站点时替换此函数名）
        page_data = parse_books(soup, CONFIG["base_url"])
        all_data.extend(page_data)
        logger.info(f"累计已抓取：{len(all_data)} 条")

        # 获取下一页 URL
        current_url = get_next_url(soup, current_url)
        if current_url:
            # 随机延迟：礼貌性访问，避免对服务器造成压力
            delay = random.uniform(*CONFIG["request_delay"])
            logger.info(f"等待 {delay:.1f}s 后抓取下一页…")
            time.sleep(delay)

    # 保存结果
    save_to_csv(all_data, CONFIG["output_file"])

    logger.info("爬虫运行完毕")
    logger.info(f"总计抓取页数：{page_count}  总计数据条数：{len(all_data)}")


# ──────────────────────────────────────────────────────
# 10. 程序入口
# ──────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
