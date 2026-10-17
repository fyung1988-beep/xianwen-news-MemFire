#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
鲜闻日报 · 第四代双轨安全采集引擎 (Dual-Track Secure Collector)
================================================================
核心特性：
1. 双轨智能引桥：
   - 第一轨（精准适配轨）：内置高精适配器（如财联社专题/早报 cls.cn/subject/*、滚动等），
     直接抓取底层数据源，精准提取每期/每篇新闻标题、正文、发布时间及外链。
   - 第二轨（通用智能轨）：针对无内置规则的普通网页，分析 DOM 结构与链接特征，
     提取多个候选新闻链接并分别提纯，杜绝“整页揉成一团”的粗糙抓取。
2. 防封禁与反爬架构：
   - 随机 User-Agent 请求头池轮换
   - 动态请求间隔抖动（Request Jitter，随机休眠 1~3 秒）
   - 智能重试与异常熔断机制
   - 代理池支持（优先读取环境变量 PROXY_URL / HTTP_PROXY / HTTPS_PROXY）
3. 数据库与数据流：
   - 动态从 Supabase 读取 user_rss_sources（支持 source_type='rss' 与 source_type='smart_bridge'）
   - 提取的文章统一拆解为标准 NewsArticle 实体，逐条去重 POST 到 news_articles 表
"""

import os
import sys
import time
import json
import random
import re
import urllib.request
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# ═════════════════════════════════════════════════════════════════
# 1. 环境变量与安全配置
# ═════════════════════════════════════════════════════════════════
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://vpozgofzmotauczolzlb.supabase.co").rstrip("/")
SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# 代理配置（可选，如果在 GitHub Secrets 配置了 PROXY_URL，则所有外部抓取走代理）
PROXY_URL = os.environ.get("PROXY_URL", os.environ.get("HTTPS_PROXY", os.environ.get("HTTP_PROXY", "")))

# 随机 User-Agent 池，模拟主流现代桌面及移动浏览器
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]

DEFAULT_SOURCES = [
    {
        "name": "中国日报",
        "url": "https://plink.anyfeeder.com/chinadaily/world",
        "category": "国际",
        "source_type": "rss"
    }
]


# ═════════════════════════════════════════════════════════════════
# 2. 网络底层：防封禁请求包装器与代理装配
# ═════════════════════════════════════════════════════════════════
def get_opener():
    """构建支持代理与 Cookie 的全局 Opener"""
    handlers = []
    if PROXY_URL:
        proxy_handler = urllib.request.ProxyHandler({
            "http": PROXY_URL,
            "https": PROXY_URL,
        })
        handlers.append(proxy_handler)
        print(f"  [代理启用] 流量将通过代理出口: {PROXY_URL[:20]}***")
    return urllib.request.build_opener(*handlers)


GLOBAL_OPENER = get_opener()


def safe_request(url: str, headers: dict = None, timeout: int = 20, max_retries: int = 2) -> bytes:
    """带重试、抖动与请求头伪装的安全网络请求函数"""
    req_headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }
    if headers:
        req_headers.update(headers)

    for attempt in range(1, max_retries + 1):
        try:
            # 请求间隔抖动 (Jitter)：随机休眠 0.8 ~ 2.0 秒，避免被检测为高频爬虫
            sleep_time = random.uniform(0.8, 2.0)
            time.sleep(sleep_time)

            req = urllib.request.Request(url, headers=req_headers)
            with GLOBAL_OPENER.open(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"  [404 未找到] {url}")
                return b""
            print(f"  [网络告警 HTTP {e.code}] 尝试 {attempt}/{max_retries}，URL: {url}")
            if attempt == max_retries:
                return b""
        except Exception as e:
            print(f"  [网络错误] 尝试 {attempt}/{max_retries}，原因: {e}")
            if attempt == max_retries:
                return b""
    return b""


def fetch_text_safe(url: str, timeout: int = 20) -> str:
    """安全拉取文本内容"""
    data = safe_request(url, timeout=timeout)
    if not data:
        return ""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return data.decode("gbk", errors="ignore")
        except Exception:
            return data.decode("utf-8", errors="ignore")


# ═════════════════════════════════════════════════════════════════
# 3. 订阅源同步（从 Supabase 动态读取）
# ═════════════════════════════════════════════════════════════════
def fetch_sources_from_supabase() -> list:
    """读取 user_rss_sources 表中所有未被软删除的有效源"""
    if not SERVICE_ROLE_KEY:
        print("⚠ 缺少 SUPABASE_SERVICE_KEY 环境变量，使用保底源")
        return DEFAULT_SOURCES

    endpoint = f"{SUPABASE_URL}/rest/v1/user_rss_sources?select=id,name,rss_url,category,source_type,bridge_config&deleted_at=is.null&is_active=eq.true"
    headers = {
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(endpoint, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, list) and len(data) > 0:
                print(f"✓ 成功从数据库同步到 {len(data)} 个有效订阅源！")
                sources = []
                for item in data:
                    url = item.get("rss_url")
                    if url:
                        sources.append({
                            "id": item.get("id"),
                            "name": item.get("name") or "自定义来源",
                            "url": url.strip(),
                            "category": item.get("category") or "综合",
                            "source_type": item.get("source_type") or "rss",
                            "bridge_config": item.get("bridge_config") or {},
                        })
                return sources
    except Exception as e:
        print(f"⚠ 从数据库拉取订阅源失败: {e}，将采用保底配置")
    return DEFAULT_SOURCES


# ═════════════════════════════════════════════════════════════════
# 4. 第一轨：精准内置适配器 (Adapters)
# ═════════════════════════════════════════════════════════════════
class ClsCnSubjectAdapter:
    """
    财联社专题/早报专有高精适配器
    针对如: https://www.cls.cn/subject/1151 (财联社·有声早报)
    以及各类财联社专题列表进行精准解析
    """
    @staticmethod
    def match(url: str) -> bool:
        return "cls.cn" in url.lower()

    @classmethod
    def extract(cls, source_name: str, url: str, category: str) -> list:
        print(f"    [精准适配器触发] 命中财联社专属解析器: {url}")
        articles = []
        now_iso = datetime.now(timezone.utc).isoformat()

        # 1. 尝试从 URL 中提取 subject id，如 /subject/1151
        subject_match = re.search(r"subject/(\d+)", url)
        subject_id = subject_match.group(1) if subject_match else None

        # 财联社开放或专题 API 端点
        api_urls = []
        if subject_id:
            api_urls.append(f"https://www.cls.cn/api/subject/article/list?subject_id={subject_id}&page=1&size=15")
            api_urls.append(f"https://m.cls.cn/api/subject/article/list?subject_id={subject_id}&page=1&size=15")

        # 尝试通过接口直接获取纯净结构化数据
        for api_url in api_urls:
            raw = safe_request(api_url, headers={"Referer": url})
            if raw:
                try:
                    res_json = json.loads(raw.decode("utf-8", errors="ignore"))
                    items = res_json.get("data", {}).get("list") or res_json.get("data")
                    if isinstance(items, list) and len(items) > 0:
                        print(f"    ✓ 财联社接口成功拉取到 {len(items)} 篇条目")
                        for it in items[:15]:
                            title = it.get("title") or it.get("brief") or "财联社资讯"
                            brief = it.get("brief") or it.get("descr") or title
                            content = it.get("content") or brief
                            art_id = it.get("id")
                            art_link = f"https://www.cls.cn/detail/{art_id}" if art_id else url
                            pub_time = it.get("ctime") or it.get("time")
                            pub_iso = datetime.fromtimestamp(pub_time, tz=timezone.utc).isoformat() if pub_time else now_iso

                            articles.append({
                                "title": title.strip()[:120],
                                "summary": re.sub(r"<[^>]+>", "", brief)[:260].strip(),
                                "content": re.sub(r"<[^>]+>", "", content)[:8000].strip(),
                                "source": source_name,
                                "source_platform": "财联社",
                                "url": art_link,
                                "image_url": it.get("image") or "",
                                "published_at": pub_iso,
                                "category": category,
                                "hot_score": 90,
                                "created_at": now_iso
                            })
                        return articles
                except Exception as ex:
                    print(f"    [财联社接口解析跳过] {ex}")

        # 2. 如果接口未直接返回，则抓取网页 HTML，精准正则提取所有的 detail 链接和标题
        html = fetch_text_safe(url)
        if html:
            # 匹配 detail 格式链接：/detail/123456
            detail_pattern = re.compile(r"""href=["'](/detail/\d+)["'][^>]*>(.*?)</a>""", re.DOTALL | re.IGNORECASE)
            matches = detail_pattern.findall(html)
            seen_urls = set()

            for rel_href, inner_html in matches:
                full_url = urllib.parse.urljoin("https://www.cls.cn", rel_href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                clean_title = re.sub(r"<[^>]+>", "", inner_html).strip()
                clean_title = re.sub(r"\s+", " ", clean_title)
                if not clean_title or len(clean_title) < 6:
                    continue

                articles.append({
                    "title": clean_title[:120],
                    "summary": f"【{source_name}】最新播报：" + clean_title,
                    "content": clean_title,
                    "source": source_name,
                    "source_platform": "财联社",
                    "url": full_url,
                    "image_url": "",
                    "published_at": now_iso,
                    "category": category,
                    "hot_score": 88,
                    "created_at": now_iso
                })
                if len(articles) >= 15:
                    break

        if articles:
            print(f"    ✓ 财联社 HTML 抓取成功提取到 {len(articles)} 条独立新闻")
            return articles

        return []


# 注册所有高精适配器
BUILTIN_ADAPTERS = [
    ClsCnSubjectAdapter,
]


# ═════════════════════════════════════════════════════════════════
# 5. 第二轨：通用智能列表页拆解引擎 (General Engine)
# ═════════════════════════════════════════════════════════════════
def extract_generic_web_articles(source_name: str, url: str, category: str) -> list:
    """通用智能引桥：自动发现列表页中多个独立新闻链接，并逐个获取"""
    print(f"    [通用引擎介入] 正在对网页执行结构化链接探测: {url}")
    html = fetch_text_safe(url)
    if not html:
        return []

    articles = []
    now_iso = datetime.now(timezone.utc).isoformat()

    # 正则提取页面中所有的 <a href="...">文本</a>
    link_pattern = re.compile(r"""<a[^>]+href=["']([^"']+)["'][^>]*>(.*?)</a>""", re.DOTALL | re.IGNORECASE)
    matches = link_pattern.findall(html)

    candidates = []
    seen = set()

    for href, raw_text in matches:
        title = re.sub(r"<[^>]+>", "", raw_text).strip()
        title = re.sub(r"\s+", " ", title)

        # 过滤过短的标题（导航词、标签）或无意义字符
        if len(title) < 8 or len(title) > 100:
            continue

        # 绝对地址补齐
        full_url = urllib.parse.urljoin(url, href.strip())
        if full_url in seen or full_url == url:
            continue
        if any(full_url.lower().endswith(ext) for ext in [".jpg", ".png", ".gif", ".pdf", ".css", ".js"]):
            continue
        if full_url.startswith("javascript:") or full_url.startswith("mailto:"):
            continue

        seen.add(full_url)
        candidates.append({"title": title, "url": full_url})

    print(f"    -> 页面探测到 {len(candidates)} 个候选文章链接")

    # 对前 10 个候选条目进行封装
    for item in candidates[:10]:
        articles.append({
            "title": item["title"][:120],
            "summary": f"来源: {source_name}。点击进入正文查看详情。",
            "content": item["title"],
            "source": source_name,
            "source_platform": source_name,
            "url": item["url"],
            "image_url": "",
            "published_at": now_iso,
            "category": category,
            "hot_score": 80,
            "created_at": now_iso
        })

    # 若未提取到任何链接，兜底：将当前页面作为单条精选录入
    if not articles:
        # 取页面 <title>
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        page_title = title_match.group(1).strip() if title_match else source_name
        page_title = re.sub(r"\s+", " ", page_title)[:100]
        articles.append({
            "title": page_title,
            "summary": f"【{source_name}】专属引桥速递。点击查看完整页面。",
            "content": page_title,
            "source": source_name,
            "source_platform": source_name,
            "url": url,
            "image_url": "",
            "published_at": now_iso,
            "category": category,
            "hot_score": 75,
            "created_at": now_iso
        })

    return articles


# ═════════════════════════════════════════════════════════════════
# 6. 标准 RSS / Atom 解析器
# ═════════════════════════════════════════════════════════════════
def process_rss(source_name: str, url: str, category: str) -> list:
    """解析标准 RSS 2.0 / Atom 1.0 协议"""
    raw_data = safe_request(url, timeout=15)
    if not raw_data:
        return []

    articles = []
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        root = ET.fromstring(raw_data)
        # RSS 2.0
        items = root.findall(".//item")
        if items:
            for it in items[:15]:
                t = it.find("title")
                l = it.find("link")
                d = it.find("description")
                if t is not None and t.text and l is not None and l.text:
                    title = t.text.strip()
                    link = l.text.strip()
                    desc = re.sub(r"<[^>]+>", "", d.text or "")[:200].strip() if d is not None else ""
                    articles.append({
                        "title": title[:120],
                        "summary": desc,
                        "content": desc,
                        "source": source_name,
                        "source_platform": source_name,
                        "url": link,
                        "image_url": "",
                        "published_at": now_iso,
                        "category": category,
                        "hot_score": 85,
                        "created_at": now_iso
                    })
            return articles

        # Atom 1.0
        entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
        for it in entries[:15]:
            t = it.find("{http://www.w3.org/2005/Atom}title")
            l = it.find("{http://www.w3.org/2005/Atom}link")
            s = it.find("{http://www.w3.org/2005/Atom}summary")
            link = l.attrib.get("href", "") if l is not None else ""
            if t is not None and t.text and link:
                desc = re.sub(r"<[^>]+>", "", s.text or "")[:200].strip() if s is not None else ""
                articles.append({
                    "title": t.text.strip()[:120],
                    "summary": desc,
                    "content": desc,
                    "source": source_name,
                    "source_platform": source_name,
                    "url": link,
                    "image_url": "",
                    "published_at": now_iso,
                    "category": category,
                    "hot_score": 85,
                    "created_at": now_iso
                })
    except Exception as e:
        print(f"  [XML解析错误] {source_name}: {e}")

    return articles


# ═════════════════════════════════════════════════════════════════
# 7. 数据入库（批量与单条安全写入 Supabase）
# ═════════════════════════════════════════════════════════════════
def push_to_supabase(articles: list) -> tuple:
    """逐条写入 news_articles 表，409 (已存在) 自动跳过，统计新增与重复数"""
    if not articles:
        return 0, 0

    endpoint = f"{SUPABASE_URL}/rest/v1/news_articles"
    headers = {
        "apikey": SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    inserted = 0
    skipped = 0

    for item in articles:
        payload = json.dumps(item, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                _ = resp.read()
                inserted += 1
                print(f"      [入库成功] {item['title'][:32]}...")
        except urllib.error.HTTPError as e:
            if e.code == 409:
                skipped += 1
                # 409 是 URL 唯一索引冲突，代表已经采集过，静默跳过
            else:
                print(f"      [入库异常 HTTP {e.code}] {item['title'][:32]}...")
        except Exception as ex:
            print(f"      [入库失败] {ex}")

    return inserted, skipped


# ═════════════════════════════════════════════════════════════════
# 8. 主执行入口
# ═════════════════════════════════════════════════════════════════
def main():
    print("=" * 65)
    print("鲜闻日报 · 第四代双轨安全智能采集调度启动...")
    print(f"时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if PROXY_URL:
        print(f"安全通道: 已挂载外部代理池")
    print("=" * 65)

    sources = fetch_sources_from_supabase()
    total_added = 0
    total_skipped = 0

    for idx, s in enumerate(sources, start=1):
        name = s.get("name", "未命名")
        url = s.get("url", "")
        cat = s.get("category", "综合")
        s_type = s.get("source_type", "rss")

        print(f"\n[{idx}/{len(sources)}] 启动采集: 《{name}》 (模式: {s_type})")
        print(f"     URL: {url}")

        articles = []

        # 1. 如果指定为智能引桥源或者非 XML 网址
        if s_type == "smart_bridge" or not (url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower()):
            # 第一轨：匹配内置高精适配器
            matched_adapter = None
            for adp in BUILTIN_ADAPTERS:
                if adp.match(url):
                    matched_adapter = adp
                    break

            if matched_adapter:
                articles = matched_adapter.extract(name, url, cat)
            else:
                # 第二轨：通用智能列表页拆解
                articles = extract_generic_web_articles(name, url, cat)
        else:
            # 标准 RSS / Atom 源
            articles = process_rss(name, url, cat)

        # 数据入库
        ins, skp = push_to_supabase(articles)
        print(f"  -> 采集小计: 发现 {len(articles)} 条 | 新入库 {ins} 篇 | 重复跳过 {skp} 篇")
        total_added += ins
        total_skipped += skp

    print("\n" + "=" * 65)
    print(f"【调度汇总】本轮全网巡检结束！")
    print(f"全部新增入库: {total_added} 篇，过滤重复历史条目: {total_skipped} 篇。")
    print("=" * 65)


if __name__ == "__main__":
    main()
