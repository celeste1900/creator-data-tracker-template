#!/usr/bin/env python3
"""
创作者数据采集工具
支持：抖音、小红书、视频号、公众号

使用方法：
  python collect_all.py              # 采集所有平台
  python collect_all.py --platform douyin  # 采集指定平台
"""
import json
import os
import subprocess
import argparse
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

# ============================================================
# 配置
# ============================================================
ROOT_DIR = Path(__file__).parent
CONFIG_FILE = ROOT_DIR / "config.json"
DATA_FILE = ROOT_DIR / "data" / "all_data.json"
LOG_FILE = ROOT_DIR / "logs" / "collect.log"

# 导入数据库模块
import sys
sys.path.insert(0, str(ROOT_DIR / "data"))
from database import (
    init_db, save_daily_account, save_works,
    export_for_frontend, get_latest_account, backup_db
)


# ============================================================
# 工具函数
# ============================================================
def log(msg):
    """记录日志（自动轮转，单文件超过 1MB 时归档）"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    # 日志轮转：超过 1MB 时归档旧日志
    if LOG_FILE.exists() and LOG_FILE.stat().st_size > 1_000_000:
        archive = LOG_FILE.with_suffix(f".{datetime.now().strftime('%Y%m%d')}.log")
        if not archive.exists():
            LOG_FILE.rename(archive)
        # 清理 30 天前的归档
        for old in LOG_FILE.parent.glob("collect.*.log"):
            if old.stem.split(".")[-1] < (datetime.now().strftime('%Y%m%d')[:6] + "01"):
                old.unlink()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def safe_int(val):
    """安全转换为整数"""
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def load_config():
    """加载配置"""
    if not CONFIG_FILE.exists():
        log("错误: config.json 不存在")
        return None
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_frontend_json():
    """生成前端需要的 JSON 文件（含数据完整性校验）"""
    import shutil
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

    # 读取旧数据（用于比较和保护）
    old_data = {}
    old_count = 0
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                old_data = json.load(f)
            old_count = len(old_data.get("daily_snapshots", []))
        except Exception:
            pass

    data = export_for_frontend()
    new_count = len(data.get("daily_snapshots", []))

    # 防线：数据量暴跌检测
    if old_count >= 10 and new_count < old_count * 0.5:
        log(f"⛔ 数据异常：快照从 {old_count} 条降到 {new_count} 条，拒绝覆写 all_data.json")
        return

    # 合并 orders_data.json（权威来源），转换为前端格式（累计值）
    orders_file = DATA_FILE.parent / "orders_data.json"
    if orders_file.exists():
        try:
            with open(orders_file, "r", encoding="utf-8") as f:
                orders_src = json.load(f)
            daily_orders = orders_src.get("daily", [])
            if daily_orders:
                converted = []
                cumulative_orders = 0
                cumulative_amount = 0.0
                for entry in daily_orders:
                    if "cumulative_orders" in entry:
                        cumulative_orders = entry["cumulative_orders"]
                        cumulative_amount = float(entry["cumulative_revenue_usd"])
                    else:
                        # 没有累计字段时，从上一条累计值加上日增量
                        cumulative_orders += entry.get("orders", entry.get("paid_orders", 0))
                        cumulative_amount += float(entry.get("revenue_usd", entry.get("revenue_cents", 0) / 100))
                    converted.append({
                        "date": entry["date"],
                        "order_count": cumulative_orders,
                        "order_amount": cumulative_amount
                    })
                data["orders"] = converted
                log(f"订单数据已从 orders_data.json 合并（{len(converted)} 条）")
        except Exception as e:
            log(f"⚠️ 合并 orders_data.json 失败: {e}")

    # 保护平台 account 数据：若旧 JSON 里的 last_updated 比 DB 更新，保留旧数据
    for platform in ["douyin", "xiaohongshu", "shipinhao", "gongzhonghao"]:
        old_platform = old_data.get(platform, {})
        new_platform = data.get(platform, {})
        if not old_platform or not new_platform:
            continue
        old_ts = old_platform.get("account", {}).get("last_updated", "")
        new_ts = new_platform.get("account", {}).get("last_updated", "")
        if old_ts and new_ts and old_ts > new_ts:
            data[platform] = old_platform
            log(f"⚠️ {platform} DB数据({new_ts})比现有JSON({old_ts})旧，保留JSON数据")

    # 备份旧文件
    if DATA_FILE.exists():
        backup = DATA_FILE.with_suffix('.backup.json')
        shutil.copy2(DATA_FILE, backup)

    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"前端数据已更新: {DATA_FILE} ({new_count} 条快照)")


def parse_cookies(cookie_str):
    """解析 Cookie 字符串为 Playwright 格式"""
    cookies = []
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            name, value = item.split("=", 1)
            cookies.append({
                "name": name.strip(),
                "value": value.strip(),
                "path": "/",
            })
    return cookies


# ============================================================
# 数据结构：统一的账号和作品数据格式
# ============================================================
def create_empty_account(platform):
    """创建空的账号数据结构"""
    return {
        "platform": platform,
        "account_name": "",
        "account_id": "",
        "avatar_url": "",
        "followers": 0,
        "total_views": 0,
        "total_likes": 0,
        "total_comments": 0,
        "total_shares": 0,
        "total_collects": 0,
        "total_works": 0
    }


def create_work(platform, work_id="", title="", publish_time="", cover_url="", url="",
                views=0, likes=0, comments=0, shares=0, collects=0):
    """创建作品数据结构"""
    return {
        "work_id": work_id,
        "platform": platform,
        "title": title[:80] if title else "",
        "publish_time": publish_time,
        "cover_url": cover_url,
        "url": url,
        "views": safe_int(views),
        "likes": safe_int(likes),
        "comments": safe_int(comments),
        "shares": safe_int(shares),
        "collects": safe_int(collects)
    }


def scroll_to_load_all(page, get_count_fn, label="", max_scrolls=10):
    """滚动页面加载所有内容（用于无限滚动列表）"""
    prev_count = get_count_fn()
    for i in range(max_scrolls):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        current = get_count_fn()
        if current > prev_count:
            log(f"[{label}] 滚动加载: {prev_count} → {current}")
            prev_count = current
        else:
            break


def calculate_account_totals(account, works):
    """从作品列表计算账号的汇总数据"""
    account["total_views"] = sum(w["views"] for w in works)
    account["total_likes"] = sum(w["likes"] for w in works)
    account["total_comments"] = sum(w["comments"] for w in works)
    account["total_shares"] = sum(w["shares"] for w in works)
    account["total_collects"] = sum(w["collects"] for w in works)
    account["total_works"] = len(works)
    return account


# ============================================================
# 采集器：小红书
# ============================================================
def collect_xiaohongshu(page, cookie_str):
    """采集小红书数据"""
    log("[小红书] 开始采集...")

    cookies = parse_cookies(cookie_str)
    for c in cookies:
        c["domain"] = ".xiaohongshu.com"
    page.context.add_cookies(cookies)

    account = create_empty_account("xiaohongshu")
    works = []
    api_data = {"user": None, "notes": None, "overview": None}

    def handle_response(response):
        try:
            url = response.url
            if "/api/galaxy/user/info" in url:
                data = response.json()
                if data.get("code") == 0:
                    api_data["user"] = data.get("data", {})
            elif "/fans/overall" in url:
                data = response.json()
                if data.get("code") == 0 and data.get("data"):
                    api_data["overview"] = data.get("data", {})
            elif "/api/galaxy/creator/datacenter/note/analyze/list" in url:
                data = response.json()
                if data.get("code") == 0:
                    notes = data.get("data", {}).get("note_infos", [])
                    if api_data["notes"] is None:
                        api_data["notes"] = []
                    api_data["notes"].extend(notes)  # 合并多次请求的数据
            elif "/api/galaxy/creator/content/note/list" in url or "/content/note/list" in url:
                # 内容管理页面的笔记列表 API
                data = response.json()
                if data.get("code") == 0:
                    notes = data.get("data", {}).get("notes", [])
                    if api_data["notes"] is None:
                        api_data["notes"] = []
                    for note in notes:
                        # 转换格式
                        api_data["notes"].append({
                            "id": note.get("note_id", ""),
                            "title": note.get("title", ""),
                            "cover_url": note.get("cover", ""),
                            "post_time": note.get("time", 0) * 1000 if note.get("time") else 0,
                            "read_count": note.get("read_count", 0),
                            "like_count": note.get("like_count", 0),
                            "comment_count": note.get("comment_count", 0),
                            "share_count": note.get("share_count", 0),
                            "fav_count": note.get("collect_count", 0),
                        })
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            if any(kw in response.url for kw in ["/api/galaxy/", "/post/post_list", "/auth/auth_data", "/work_list"]):
                log(f"[响应拦截] 解析失败 {response.url[:80]}: {e}")

    page.on("response", handle_response)

    try:
        page.goto("https://creator.xiaohongshu.com/statistics/fans-data",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)

        page.goto("https://creator.xiaohongshu.com/statistics/data-analysis",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)

        # 翻页获取所有数据（动态检测是否有下一页，不设硬编码上限）
        page_num = 1
        while True:
            page_num += 1
            prev_count = len(api_data["notes"]) if api_data["notes"] else 0
            try:
                # 尝试点击页码数字
                clicked = False
                page_selectors = [
                    f"li:has-text('{page_num}'):not([class*='active'])",
                    f"span:text-is('{page_num}'):not([class*='active'])",
                    f"[class*='pagination'] :text-is('{page_num}')",
                    f"[class*='pager'] :text-is('{page_num}')",
                ]
                for sel in page_selectors:
                    btn = page.locator(sel)
                    if btn.count() > 0 and btn.first.is_visible():
                        log(f"[小红书] 点击第 {page_num} 页")
                        btn.first.click()
                        page.wait_for_timeout(2000)
                        clicked = True
                        break
                if not clicked:
                    # 尝试点击 > 或下一页按钮
                    next_selectors = [
                        "li:has-text('>')", "span:text-is('>')",
                        "[class*='next']", "button:has-text('下一页')",
                    ]
                    for sel in next_selectors:
                        btn = page.locator(sel)
                        if btn.count() > 0 and btn.first.is_visible():
                            log(f"[小红书] 点击下一页按钮")
                            btn.first.click()
                            page.wait_for_timeout(2000)
                            clicked = True
                            break
                if not clicked:
                    log(f"[小红书] 没有更多页了 (共 {page_num - 1} 页)")
                    break
                # 验证翻页后是否有新数据
                new_count = len(api_data["notes"]) if api_data["notes"] else 0
                if new_count <= prev_count:
                    page.wait_for_timeout(2000)  # 再等一下
                    new_count = len(api_data["notes"]) if api_data["notes"] else 0
                    if new_count <= prev_count:
                        log(f"[小红书] 第 {page_num} 页没有新数据，停止翻页")
                        break
            except Exception as e:
                log(f"[小红书] 翻页失败: {e}")
                break
            if page_num > 20:
                break

        # 解析用户信息
        if api_data["user"]:
            user = api_data["user"]
            account["account_name"] = user.get("userName", "") or user.get("name", "")
            account["account_id"] = user.get("redId", "") or user.get("userId", "")
            account["avatar_url"] = user.get("userAvatar", "") or user.get("avatar", "")

        # 从粉丝总览获取粉丝数
        if api_data["overview"]:
            seven_data = api_data["overview"].get("seven", {})
            account["followers"] = safe_int(seven_data.get("fans_count", 0))

        # 解析笔记数据（去重）
        seen_ids = set()
        unique_notes = []
        if api_data["notes"]:
            for note in api_data["notes"]:
                note_id = note.get("id", "")
                if note_id and note_id not in seen_ids:
                    seen_ids.add(note_id)
                    unique_notes.append(note)

        for note in unique_notes:
                publish_time = ""
                if note.get("post_time"):
                    try:
                        publish_time = datetime.fromtimestamp(
                            note["post_time"] / 1000
                        ).strftime("%Y-%m-%d %H:%M")
                    except (ValueError, OSError, TypeError):
                        pass

                work = create_work(
                    platform="xiaohongshu",
                    work_id=note.get("id", ""),
                    title=note.get("title", ""),
                    publish_time=publish_time,
                    cover_url=note.get("cover_url", ""),
                    url=f"https://www.xiaohongshu.com/explore/{note.get('id', '')}",
                    views=note.get("read_count", 0),
                    likes=note.get("like_count", 0),
                    comments=note.get("comment_count", 0),
                    shares=note.get("share_count", 0),
                    collects=note.get("fav_count", 0)
                )
                works.append(work)

        # 计算汇总
        calculate_account_totals(account, works)

        log(f"[小红书] 采集完成: {account['account_name']}, {len(works)} 个作品")
        return {"account": account, "works": works}

    except Exception as e:
        log(f"[小红书] 采集失败: {e}")
        return None


# ============================================================
# 采集器：抖音
# ============================================================
def collect_douyin(page, cookie_str):
    """采集抖音数据"""
    log("[抖音] 开始采集...")

    cookies = parse_cookies(cookie_str)
    for c in cookies:
        c["domain"] = ".douyin.com"
    page.context.add_cookies(cookies)

    account = create_empty_account("douyin")
    works = []
    api_data = {"works": [], "user_info": None}

    def handle_response(response):
        try:
            url = response.url
            if "/janus/douyin/creator/pc/work_list" in url or "/work_list" in url:
                data = response.json()
                if data.get("status_code") == 0:
                    aweme_list = data.get("aweme_list", [])
                    api_data["works"].extend(aweme_list)
            # 拦截创作者首页的用户信息 API（获取准确粉丝数）
            elif "/creator/user/info" in url or "/creator/pc/user/info" in url:
                if response.status == 200 and "application/json" in (response.headers.get("content-type") or ""):
                    data = response.json()
                    api_data["user_info"] = data
                    log(f"[抖音] 捕获用户信息 API: {url[:80]}")
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            if any(kw in response.url for kw in ["/api/galaxy/", "/post/post_list", "/auth/auth_data", "/work_list"]):
                log(f"[响应拦截] 解析失败 {response.url[:80]}: {e}")

    page.on("response", handle_response)

    try:
        page.goto("https://creator.douyin.com/creator-micro/home",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)

        # 尝试从首页页面直接读取粉丝数（DOM 中展示的实时数据）
        try:
            fans_text = page.locator("text=/粉丝/").first
            if fans_text.is_visible():
                # 粉丝数通常在附近的元素中
                fans_container = fans_text.locator("..").first
                all_text = fans_container.inner_text()
                log(f"[抖音] 首页粉丝区域文本: {all_text.strip()[:100]}")
        except Exception:
            pass

        # 尝试从页面中提取粉丝数数字
        try:
            page_fans = page.evaluate('''() => {
                // 查找包含"粉丝"的元素，取其附近的数字
                const els = document.querySelectorAll('*');
                for (const el of els) {
                    if (el.children.length === 0 && el.textContent.trim() === '粉丝') {
                        const parent = el.parentElement;
                        if (parent) {
                            const nums = parent.textContent.match(/[\\d,]+/);
                            if (nums) return nums[0].replace(/,/g, '');
                        }
                    }
                }
                return null;
            }''')
            if page_fans:
                fans_count = int(page_fans)
                if fans_count > 0:
                    account["followers"] = fans_count
                    log(f"[抖音] 从首页获取粉丝数: {fans_count}")
        except Exception as e:
            log(f"[抖音] 从首页提取粉丝数失败: {e}")

        # 再尝试从 API 响应中获取粉丝数（比 DOM 更可靠）
        if api_data["user_info"]:
            try:
                info = api_data["user_info"]
                # 递归查找 follower_count 字段
                def find_followers(obj, depth=0):
                    if depth > 5 or not isinstance(obj, dict):
                        return None
                    for key in ["follower_count", "followers_count", "fans_count", "mplatform_followers_count"]:
                        if key in obj and obj[key]:
                            return safe_int(obj[key])
                    for v in obj.values():
                        if isinstance(v, dict):
                            result = find_followers(v, depth + 1)
                            if result and result > 0:
                                return result
                    return None
                val = find_followers(info)
                if val and val > 0:
                    account["followers"] = val
                    log(f"[抖音] 从 API 获取粉丝数: {val}")
                else:
                    log(f"[抖音] API 响应顶层 keys: {list(info.keys())[:10]}")
            except Exception as e:
                log(f"[抖音] 从 API 提取粉丝数失败: {e}")

        page.goto("https://creator.douyin.com/creator-micro/content/manage",
                  wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(3000)
        page.evaluate("window.scrollTo(0, 500)")
        page.wait_for_timeout(2000)

        if not api_data["works"]:
            page.goto("https://creator.douyin.com/creator/content/manage",
                      wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(3000)

        # 翻页 + 滚动获取所有作品
        initial_count = len(api_data["works"])
        log(f"[抖音] 初始加载 {initial_count} 个作品，尝试加载更多...")

        # 1. 滚动加载（处理无限滚动）
        scroll_to_load_all(page, lambda: len(api_data["works"]), "抖音")

        # 2. 翻页按钮（处理分页）
        page_num = 1
        while True:
            page_num += 1
            try:
                # 尝试多种分页按钮选择器
                selectors = [
                    f"li:has-text('{page_num}'):not([class*='active'])",
                    f"span:text-is('{page_num}'):not([class*='active'])",
                    f"button:text-is('{page_num}')",
                    f"[class*='pager'] :text-is('{page_num}')",
                    f"[class*='pagination'] :text-is('{page_num}')",
                ]
                clicked = False
                for sel in selectors:
                    btn = page.locator(sel)
                    if btn.count() > 0 and btn.first.is_visible():
                        log(f"[抖音] 点击第 {page_num} 页")
                        btn.first.click()
                        page.wait_for_timeout(2000)
                        clicked = True
                        break
                if not clicked:
                    # 尝试通用下一页按钮
                    next_selectors = [
                        "button:has-text('>')", "li:has-text('>')",
                        "[class*='next']", "button:has-text('下一页')",
                    ]
                    for sel in next_selectors:
                        btn = page.locator(sel)
                        if btn.count() > 0 and btn.first.is_visible() and btn.first.is_enabled():
                            log(f"[抖音] 点击下一页 (第 {page_num} 页)")
                            btn.first.click()
                            page.wait_for_timeout(2000)
                            clicked = True
                            break
                    if not clicked:
                        break
            except Exception:
                break

        log(f"[抖音] 最终获取 {len(api_data['works'])} 个作品 (初始 {initial_count})")

        # 解析作品数据
        for item in api_data["works"]:
            # 从第一个作品获取用户信息
            if not account["account_name"] and item.get("author"):
                author = item["author"]
                account["account_name"] = author.get("nickname", "")
                account["account_id"] = str(author.get("uid", "") or author.get("unique_id", ""))
                # 仅在首页没有拿到粉丝数时，用 work_list 的 author 数据作为 fallback
                if account["followers"] == 0:
                    account["followers"] = safe_int(
                        author.get("follower_count", 0) or author.get("mplatform_followers_count", 0)
                    )
                # 头像
                for key in ["avatar_thumb", "avatar_medium", "avatar_larger"]:
                    if author.get(key, {}).get("url_list"):
                        account["avatar_url"] = author[key]["url_list"][0]
                        break

            stats = item.get("statistics", {})
            publish_time = ""
            if item.get("create_time"):
                try:
                    publish_time = datetime.fromtimestamp(item["create_time"]).strftime("%Y-%m-%d %H:%M")
                except (ValueError, OSError, TypeError):
                    pass

            cover_url = ""
            if item.get("cover", {}).get("url_list"):
                cover_url = item["cover"]["url_list"][0]

            work = create_work(
                platform="douyin",
                work_id=item.get("aweme_id", ""),
                title=item.get("desc", ""),
                publish_time=publish_time,
                cover_url=cover_url,
                url=f"https://www.douyin.com/video/{item.get('aweme_id', '')}",
                views=stats.get("play_count", 0),
                likes=stats.get("digg_count", 0),
                comments=stats.get("comment_count", 0),
                shares=stats.get("share_count", 0),
                collects=stats.get("collect_count", 0)
            )
            works.append(work)

        # 计算汇总
        calculate_account_totals(account, works)

        log(f"[抖音] 采集完成: {account['account_name']}, {len(works)} 个作品")
        return {"account": account, "works": works}

    except Exception as e:
        log(f"[抖音] 采集失败: {e}")
        return None


# ============================================================
# 采集器：视频号
# ============================================================
def collect_shipinhao(page, cookie_str, playwright_instance=None, allow_interactive_login=True):
    """采集视频号数据"""
    log("[视频号] 开始采集...")

    cookies = parse_cookies(cookie_str)
    for c in cookies:
        c["domain"] = ".weixin.qq.com"
    page.context.add_cookies(cookies)

    account = create_empty_account("shipinhao")
    works = []
    api_data = {"auth": None, "posts": [], "images": [], "need_login": False,
                "posts_has_more": False, "images_has_more": False,
                "posts_last_buffer": "", "images_last_buffer": "",
                "data_overview": None}
    collecting_images = False  # 标记当前是否在采集图文
    result_status = {"status": "success", "message": ""}

    def handle_response(response):
        nonlocal collecting_images
        try:
            url = response.url
            if "/auth/auth_data" in url:
                data = response.json()
                if data.get("errCode") == 0:
                    api_data["auth"] = data.get("data", {})
                elif data.get("errCode") == 300334:
                    api_data["need_login"] = True
            elif "/post/post_list" in url:
                data = response.json()
                if data.get("errCode") == 0:
                    resp_data = data.get("data", {})
                    items = resp_data.get("list", [])
                    has_more = resp_data.get("hasMore", False)
                    last_buffer = resp_data.get("lastBuffer", "")
                    # 根据当前采集状态决定存储位置
                    if collecting_images:
                        api_data["images"].extend(items)
                        api_data["images_has_more"] = has_more
                        api_data["images_last_buffer"] = last_buffer
                    else:
                        api_data["posts"].extend(items)
                        api_data["posts_has_more"] = has_more
                        api_data["posts_last_buffer"] = last_buffer
            # 拦截数据中心的实时互动数据 API
            elif "/statistic/new_post_total_data" in url:
                data = response.json()
                if data.get("errCode") == 0:
                    api_data["data_overview"] = data.get("data", {})
        except Exception as e:
            if any(kw in response.url for kw in ["/post/post_list", "/auth/auth_data", "/statistic/"]):
                log(f"[响应拦截] 解析失败 {response.url[:80]}: {type(e).__name__}")

    page.on("response", handle_response)
    headed_browser = None

    try:
        # 访问内容管理页面
        page.goto("https://channels.weixin.qq.com/platform/post/list",
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        need_login = "login" in page.url or api_data["need_login"] or not api_data["auth"]

        if need_login and playwright_instance and allow_interactive_login:
            log("[视频号] 需要登录，正在打开浏览器窗口...")
            page.context.close()

            # 使用持久化上下文保存登录状态
            user_data_dir = ROOT_DIR / "data" / "browser_data" / "shipinhao"
            user_data_dir.mkdir(parents=True, exist_ok=True)

            headed_context = playwright_instance.chromium.launch_persistent_context(
                user_data_dir=str(user_data_dir),
                headless=False,
                args=["--window-position=200,100", "--window-size=800,700"],
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            headed_browser = headed_context  # 用于后续关闭
            headed_page = headed_context.pages[0] if headed_context.pages else headed_context.new_page()

            api_data["auth"] = None
            api_data["posts"] = []
            api_data["images"] = []
            headed_page.on("response", handle_response)

            headed_page.goto("https://channels.weixin.qq.com/platform/post/list",
                             wait_until="domcontentloaded", timeout=120000)
            log("[视频号] 请扫码登录...")
            # 用 AppleScript 将浏览器窗口置顶 + 发通知带声音
            try:
                subprocess.run([
                    "osascript", "-e",
                    '''
                    tell application "Google Chrome for Testing" to activate
                    display notification "请扫码登录视频号（2分钟内）" with title "视频号采集" sound name "Glass"
                    '''
                ], capture_output=True, timeout=5)
            except Exception:
                try:
                    subprocess.run([
                        "osascript", "-e",
                        'display notification "请扫码登录视频号（2分钟内）" with title "视频号采集" sound name "Glass"'
                    ], capture_output=True, timeout=5)
                except Exception:
                    pass

            login_success = False
            for i in range(120):
                headed_page.wait_for_timeout(1000)
                if "login" not in headed_page.url and api_data["auth"]:
                    log("[视频号] 登录成功！")
                    login_success = True
                    break
                # 60秒时再提醒一次
                if i == 60:
                    log("[视频号] 扫码等待中，再次提醒...")
                    try:
                        subprocess.run([
                            "osascript", "-e",
                            'display notification "视频号还没扫码！还剩1分钟" with title "视频号采集" sound name "Sosumi"'
                        ], capture_output=True, timeout=5)
                    except Exception:
                        pass
            if not login_success:
                log("[视频号] 登录超时")
                headed_browser.close()
                return {
                    "status": "pending_login",
                    "message": "登录超时",
                    "account": account,
                    "works": works
                }

            # 持久化上下文会自动保存登录状态，不需要手动保存 Cookie
            log("[shipinhao] 登录状态已保存到浏览器配置")

            # 登录后重新导航到作品列表页（视频），确保触发 post_list API
            headed_page.goto("https://channels.weixin.qq.com/platform/post/list",
                             wait_until="domcontentloaded", timeout=60000)
            # 等待 post_list API 返回（最多等 10 秒）
            for _ in range(10):
                headed_page.wait_for_timeout(1000)
                if api_data["posts"]:
                    break
            log(f"[视频号] 视频列表已加载，捕获到 {len(api_data['posts'])} 个视频, hasMore={api_data['posts_has_more']}")
            # 始终尝试滚动加载更多视频（API 的 hasMore 不可靠）
            scroll_to_load_all(headed_page, lambda: len(api_data["posts"]), "视频号-视频")
            # 用 lastBuffer 手动翻页拿完剩余视频
            if api_data["posts_last_buffer"]:
                for _ in range(5):
                    prev_cnt = len(api_data["posts"])
                    headed_page.evaluate(f"""
                        fetch('/cgi-bin/mmfinderassistant-bin/post/post_list', {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/x-www-form-urlencoded'}},
                            body: 'lastBuffer={api_data["posts_last_buffer"]}&pageSize=50&scene=0'
                        }}).then(r => r.text())
                    """)
                    headed_page.wait_for_timeout(3000)
                    if len(api_data["posts"]) > prev_cnt:
                        log(f"[视频号-视频] 手动翻页: {prev_cnt} → {len(api_data['posts'])}")
                    else:
                        break

            # 在 headed browser 中点击图文 tab 采集图文
            log("[视频号] 采集图文内容...")
            collecting_images = True  # 标记开始采集图文
            try:
                # 点击左侧菜单的"图文"
                image_tab = headed_page.locator("text=图文").first
                if image_tab.is_visible():
                    image_tab.click()
                    headed_page.wait_for_timeout(3000)
                    # 等待图文 API 返回
                    for _ in range(5):
                        headed_page.wait_for_timeout(1000)
                        if api_data["images"]:
                            break
                    log(f"[视频号] 图文列表已加载，捕获到 {len(api_data['images'])} 个图文")
                    # 翻页：遍历所有页码
                    page_idx = 2
                    while True:
                        prev_cnt = len(api_data["images"])
                        # 点击下一页页码数字
                        try:
                            # 尝试多种选择器找到页码按钮
                            next_page = headed_page.locator(f"li.ant-pagination-item-{page_idx}")
                            if not next_page.count():
                                next_page = headed_page.locator(f"a:text-is('{page_idx}')").last
                            if not next_page.count():
                                next_page = headed_page.locator(f"text='{page_idx}'").last
                            if next_page.count() and next_page.is_visible():
                                next_page.click()
                                headed_page.wait_for_timeout(3000)
                                if len(api_data["images"]) > prev_cnt:
                                    log(f"[视频号-图文] 第{page_idx}页: {prev_cnt} → {len(api_data['images'])}")
                                    page_idx += 1
                                else:
                                    log(f"[视频号-图文] 第{page_idx}页点击了但没有新数据")
                                    break
                            else:
                                log(f"[视频号-图文] 找不到第{page_idx}页按钮")
                                break
                        except Exception as e:
                            log(f"[视频号-图文] 翻页失败: {e}")
                            break
                        if page_idx > 10:
                            break
            except Exception as e:
                log(f"[视频号] 点击图文 tab 失败: {e}")

            page = headed_page

        elif need_login:
            log("[视频号] Cookie无效，需要登录")
            return {
                "status": "pending_login",
                "message": "等待登录",
                "account": account,
                "works": works
            }

        # 只在非登录路径下点击图文 tab（登录路径已在 headed browser 中点击过）
        if not (need_login and headed_browser):
            page.wait_for_timeout(3000)
            log(f"[视频号] 视频列表已加载，捕获到 {len(api_data['posts'])} 个视频, hasMore={api_data['posts_has_more']}")
            # 始终尝试滚动加载更多视频（API 的 hasMore 不可靠）
            scroll_to_load_all(page, lambda: len(api_data["posts"]), "视频号-视频")

            # 点击图文 tab 采集图文内容
            log("[视频号] 采集图文内容...")
            collecting_images = True  # 标记开始采集图文
            try:
                # 点击左侧菜单的"图文"
                image_tab = page.locator("text=图文").first
                if image_tab.is_visible():
                    image_tab.click()
                    page.wait_for_timeout(3000)
                    # 等待图文 API 返回
                    for _ in range(5):
                        page.wait_for_timeout(1000)
                        if api_data["images"]:
                            break
                    log(f"[视频号] 图文列表已加载，捕获到 {len(api_data['images'])} 个图文, hasMore={api_data['images_has_more']}, lastBuffer长度={len(api_data['images_last_buffer'])}")
                    # 用 JS fetch 翻页拿完所有图文
                    last_buf = api_data["images_last_buffer"]
                    page_num = 1
                    while True:
                        page_num += 1
                        prev_cnt = len(api_data["images"])
                        # 有 lastBuffer 就用 lastBuffer，没有就用 pageNum
                        if last_buf:
                            body = f"lastBuffer={last_buf}&pageSize=20&scene=2"
                        else:
                            body = f"pageNum={page_num}&pageSize=20&scene=2"
                        try:
                            result = page.evaluate("""async (body) => {
                                const resp = await fetch('/cgi-bin/mmfinderassistant-bin/post/post_list', {
                                    method: 'POST',
                                    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                                    body: body
                                });
                                return await resp.json();
                            }""", body)
                            log(f"[视频号-图文] 翻页{page_num} 返回: errCode={result.get('errCode') if result else 'None'}")
                            if result and result.get("errCode") == 0:
                                items = result.get("data", {}).get("list", [])
                                last_buf = result.get("data", {}).get("lastBuffer", "")
                                if items:
                                    api_data["images"].extend(items)
                                    log(f"[视频号-图文] 翻页成功: {prev_cnt} → {len(api_data['images'])}")
                                    if not result.get("data", {}).get("hasMore", False) and not last_buf:
                                        break
                                else:
                                    break
                            else:
                                break
                        except Exception as e:
                            log(f"[视频号-图文] 翻页失败: {e}")
                            break
                        if page_num > 10:
                            break
            except Exception as e:
                log(f"[视频号] 点击图文 tab 失败: {e}")

        # 访问数据中心获取实时每日互动数据（post_list 返回的互动数据有延迟）
        active_page = headed_page if headed_browser else page
        try:
            log("[视频号] 获取数据中心实时互动数据...")
            active_page.goto("https://channels.weixin.qq.com/platform/datacenter/content",
                             wait_until="domcontentloaded", timeout=30000)
            active_page.wait_for_timeout(6000)
            # 拦截到的 data_overview 可能已经有了，如果没有就主动调用
            if not api_data.get("data_overview"):
                try:
                    overview_result = active_page.evaluate("""async () => {
                        try {
                            const resp = await fetch('/cgi-bin/mmfinderassistant-bin/statistic/new_post_total_data', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                                body: 'duration=2'
                            });
                            return await resp.json();
                        } catch(e) { return {error: e.message}; }
                    }""")
                    if overview_result and overview_result.get("errCode") == 0:
                        api_data["data_overview"] = overview_result.get("data", {})
                except Exception:
                    pass
            if api_data.get("data_overview"):
                td = api_data["data_overview"].get("totalData", {})
                if td:
                    # 数组索引 0 = 昨天, 1 = 今天
                    browse = [int(x) for x in td.get("browse", ["0", "0"])]
                    like = [int(x) for x in td.get("like", ["0", "0"])]
                    comment = [int(x) for x in td.get("comment", ["0", "0"])]
                    forward = [int(x) for x in td.get("forward", ["0", "0"])]
                    fav = [int(x) for x in td.get("fav", ["0", "0"])]
                    today_interactions = like[-1] + comment[-1] + forward[-1] + fav[-1]
                    yesterday_interactions = like[0] + comment[0] + forward[0] + fav[0]
                    log(f"[视频号] 数据中心 - 昨日: 浏览{browse[0]} 点赞{like[0]} 评论{comment[0]} 转发{forward[0]} 收藏{fav[0]} (互动{yesterday_interactions})")
                    log(f"[视频号] 数据中心 - 今日: 浏览{browse[-1]} 点赞{like[-1]} 评论{comment[-1]} 转发{forward[-1]} 收藏{fav[-1]} (互动{today_interactions})")
                else:
                    log("[视频号] 数据中心返回数据中缺少 totalData")
            else:
                log("[视频号] 未获取到数据中心数据")
        except Exception as e:
            log(f"[视频号] 数据中心访问失败（不影响基础采集）: {type(e).__name__}")

        # 解析用户信息
        if api_data["auth"]:
            user = api_data["auth"].get("finderUser", {})
            account["account_name"] = user.get("nickname", "")
            account["account_id"] = user.get("uniqId", "") or user.get("finderUsername", "")
            account["followers"] = safe_int(user.get("fansCount", 0))
            account["avatar_url"] = user.get("headImgUrl", "")

        # 解析作品数据（视频 + 图文），按 objectId 去重
        all_posts_raw = api_data["posts"] + api_data["images"]
        seen_ids = set()
        all_posts = []
        for item in all_posts_raw:
            oid = item.get("objectId", "")
            if oid and oid not in seen_ids:
                seen_ids.add(oid)
                all_posts.append(item)
            elif not oid:
                all_posts.append(item)
        if len(all_posts) < len(all_posts_raw):
            log(f"[视频号] 去重: {len(all_posts_raw)} → {len(all_posts)} 个作品")

        for item in all_posts:
            desc = item.get("desc", "")
            if isinstance(desc, dict):
                # desc 是字典时，尝试从中提取标题
                title = desc.get("title", "") or desc.get("description", "") or "无标题"
            else:
                title = str(desc)[:50] if desc else "无标题"

            publish_time = ""
            if item.get("createTime"):
                try:
                    publish_time = datetime.fromtimestamp(item["createTime"]).strftime("%Y-%m-%d %H:%M")
                except (ValueError, OSError, TypeError):
                    pass

            # 获取封面 URL
            cover_url = item.get("coverUrl", "")
            if not cover_url and isinstance(desc, dict):
                media = desc.get("media", [])
                if media and isinstance(media, list):
                    cover_url = media[0].get("coverUrl", "") or media[0].get("thumbUrl", "")

            work = create_work(
                platform="shipinhao",
                work_id=item.get("objectId", ""),
                title=title,
                publish_time=publish_time,
                cover_url=cover_url,
                url="",
                views=item.get("readCount", 0),
                likes=item.get("likeCount", 0),
                comments=item.get("commentCount", 0),
                shares=item.get("forwardCount", 0),
                collects=item.get("favCount", 0)
            )
            works.append(work)

        # 计算汇总
        calculate_account_totals(account, works)

        if headed_browser:
            headed_browser.close()

        video_count = len(api_data["posts"])
        image_count = len(api_data["images"])
        log(f"[视频号] 采集完成: {account['account_name']}, {len(works)} 个作品 (视频:{video_count}, 图文:{image_count})")
        return {"status": "success", "message": "", "account": account, "works": works}

    except Exception as e:
        log(f"[视频号] 采集失败: {e}")
        if headed_browser:
            headed_browser.close()
        return {"status": "error", "message": str(e), "account": account, "works": works}


def _save_cookie_to_config(platform, new_cookie):
    """保存新 Cookie 到配置文件"""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        if platform in config:
            config[platform]["cookie"] = new_cookie
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        log(f"[{platform}] Cookie 已保存")
    except Exception as e:
        log(f"[{platform}] 保存 Cookie 失败: {e}")


# ============================================================
# 保存采集结果到数据库
# ============================================================
def save_platform_data(platform, result):
    """保存平台数据到 SQLite"""
    if not result:
        return

    account = result.get("account", {})
    works = result.get("works", [])

    # 保存每日账号数据
    save_daily_account(platform, account)

    # 保存作品数据
    if works:
        save_works(platform, works)

    log(f"[{platform}] 数据已保存到数据库")


# ============================================================
# Cookie 同步
# ============================================================
def sync_browser_cookies():
    """尝试从浏览器同步视频号 Cookie"""
    try:
        sys.path.insert(0, str(ROOT_DIR / "scripts"))
        from sync_cookie_from_browser import sync_cookies
        log("正在从浏览器同步 Cookie...")
        results = sync_cookies(browser="chrome", platforms=["shipinhao"], validate=False)
        if results.get("shipinhao", {}).get("success"):
            log("[视频号] Cookie 同步成功")
            return True
        else:
            log("[视频号] Cookie 同步失败")
            return False
    except Exception as e:
        log(f"Cookie 同步跳过: {e}")
        return False




# ============================================================
# Git 推送
# ============================================================
def push_to_github():
    """推送到 GitHub（含数据大小校验）"""
    try:
        os.chdir(ROOT_DIR)

        # 防线：推送前检查 all_data.json 是否异常缩小
        new_size = DATA_FILE.stat().st_size if DATA_FILE.exists() else 0
        try:
            git_show = subprocess.run(
                ["git", "show", "HEAD:data/all_data.json"],
                capture_output=True, timeout=5
            )
            old_size = len(git_show.stdout)
            if old_size > 5000 and new_size < old_size * 0.5:
                log(f"⛔ 推送中止：all_data.json 从 {old_size} 字节缩小到 {new_size} 字节，疑似数据丢失")
                return
        except Exception:
            pass

        # 只推送 JSON 文件（SQLite 数据库保留在本地）
        subprocess.run(["git", "add", "data/all_data.json"], check=True, capture_output=True)
        result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not result.stdout.strip():
            log("没有变更需要提交")
            return
        commit_msg = f"Auto update: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True, capture_output=True)
        # 先 stash 未跟踪的修改，再 pull --rebase，避免脏工作区阻塞
        stash_result = subprocess.run(["git", "stash", "--include-untracked"], capture_output=True, text=True)
        stashed = "No local changes" not in stash_result.stdout

        rebase_result = subprocess.run(["git", "pull", "--rebase"], capture_output=True, text=True)
        if rebase_result.returncode != 0:
            log(f"Git pull --rebase 失败: {rebase_result.stderr[:200]}")
            subprocess.run(["git", "rebase", "--abort"], capture_output=True)
            if stashed:
                subprocess.run(["git", "stash", "pop"], capture_output=True)
            return

        subprocess.run(["git", "push"], check=True, capture_output=True)

        if stashed:
            subprocess.run(["git", "stash", "pop"], capture_output=True)
        log("已推送到 GitHub")
    except Exception as e:
        log(f"Git 推送失败: {e}")


# ============================================================
# 主函数
# ============================================================
def main(target_platform=None):
    """主函数"""
    log("=" * 50)
    log(f"开始采集{'所有平台' if not target_platform else target_platform}数据")
    log("=" * 50)

    # 初始化数据库并备份
    init_db()
    backup_db()

    # 同步视频号 Cookie
    sync_browser_cookies()

    config = load_config()
    if not config:
        return

    # 采集器映射
    collectors = {
        "xiaohongshu": collect_xiaohongshu,
        "douyin": collect_douyin,
        "shipinhao": collect_shipinhao,
    }

    # 自动化模式下跳过需要扫码的视频号
    auto_mode = os.environ.get("AUTO_MODE") == "1"
    if target_platform:
        platforms_to_collect = [target_platform]
    elif auto_mode:
        platforms_to_collect = ["xiaohongshu", "douyin"]
        log("自动化模式：跳过视频号（需要扫码登录）")
    else:
        platforms_to_collect = ["xiaohongshu", "douyin", "shipinhao"]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for platform in platforms_to_collect:
            platform_config = config.get(platform, {})

            if not platform_config.get("enabled", False):
                log(f"[{platform}] 已禁用，跳过")
                continue

            cookie = platform_config.get("cookie", "")
            if not cookie or cookie.startswith("在这里"):
                log(f"[{platform}] Cookie 未配置，跳过")
                continue

            # Cookie 基本有效性检测：检查关键字段是否存在
            cookie_keys = [k.split("=")[0].strip() for k in cookie.split(";") if "=" in k]
            expected_keys = {
                "xiaohongshu": ["web_session"],
                "douyin": ["sessionid"],
                "shipinhao": [],  # 视频号用持久化浏览器登录，Cookie 可能为空
            }
            missing = [k for k in expected_keys.get(platform, []) if k not in cookie_keys]
            if missing:
                log(f"[{platform}] Cookie 缺少关键字段 {missing}，可能已过期")

            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            )
            page = context.new_page()

            try:
                if platform == "shipinhao":
                    # 视频号：总是允许弹窗登录，因为 Cookie 可能随时失效
                    result = collectors[platform](page, cookie, p, allow_interactive_login=True)
                    if result:
                        status = result.get("status", "success")
                        if status == "success":
                            # 防止空数据覆盖：登录失败/超时时 works 为空，不保存
                            if not result.get("works") and not result.get("account", {}).get("account_name"):
                                log(f"[{platform}] 数据为空（可能登录失败），跳过保存")
                            else:
                                save_platform_data(platform, result)
                        else:
                            log(f"[{platform}] 状态: {status}，跳过保存")
                else:
                    result = collectors[platform](page, cookie)
                    if result:
                        save_platform_data(platform, result)

            except Exception as e:
                log(f"[{platform}] 采集异常: {e}")
                # 网络超时时重试一次
                if "Timeout" in str(e) and platform != "shipinhao":
                    log(f"[{platform}] 超时，30 秒后重试...")
                    context.close()
                    import time
                    time.sleep(30)
                    context = browser.new_context(
                        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                    )
                    page = context.new_page()
                    try:
                        result = collectors[platform](page, cookie)
                        if result:
                            save_platform_data(platform, result)
                            log(f"[{platform}] 重试成功")
                    except Exception as e2:
                        log(f"[{platform}] 重试失败: {e2}")
            finally:
                context.close()

        browser.close()

    # 生成前端 JSON
    save_frontend_json()

    # 推送 GitHub
    if config.get("settings", {}).get("auto_push_to_github", False):
        push_to_github()

    log("=" * 50)
    log("采集完成!")
    log("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="创作者数据采集工具")
    parser.add_argument(
        "--platform",
        choices=["douyin", "xiaohongshu", "shipinhao"],
        help="指定采集的平台，不指定则采集所有平台"
    )
    args = parser.parse_args()
    main(target_platform=args.platform)
