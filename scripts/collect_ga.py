#!/usr/bin/env python3
"""
Google Analytics 数据采集
"""
import json
import time
import functools
import socket
import os
from datetime import datetime, timedelta
from pathlib import Path
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)

import sys
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR / "data"))
from database import init_db, save_daily_ga, get_ga_history
CREDENTIALS_FILE = ROOT_DIR / "config" / "ga_credentials.json"
CONFIG_FILE = ROOT_DIR / "config.json"

# 从 config.json 读取 GA Property ID，回退到默认值
def _load_ga_property_id():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config.get("ga", {}).get("property_id", "YOUR_GA_PROPERTY_ID")
    except Exception:
        return "YOUR_GA_PROPERTY_ID"

PROPERTY_ID = _load_ga_property_id()

# 重试配置
MAX_RETRIES = 3
RETRY_DELAY = 5  # 初始延迟秒数
RETRY_BACKOFF = 2  # 指数退避倍数

# 默认代理配置（用于 launchd 定时任务）
DEFAULT_PROXY = "http://127.0.0.1:7890"

# 允许的域名（过滤掉 localhost、Vercel 预览等非生产环境数据）
ALLOWED_HOSTNAMES = ['example.com', 'app.example.com']

# 排除的流量来源（开发环境 referral 等）
EXCLUDED_SOURCES = ['localhost']

_HOSTNAME_ALLOW = {
    "or_group": {
        "expressions": [
            {"filter": {"field_name": "hostName", "string_filter": {"match_type": "EXACT", "value": h}}}
            for h in ALLOWED_HOSTNAMES
        ]
    }
}

_SOURCE_EXCLUDE = {
    "not_expression": {
        "or_group": {
            "expressions": [
                {"filter": {"field_name": "sessionSource", "string_filter": {"match_type": "CONTAINS", "value": s}}}
                for s in EXCLUDED_SOURCES
            ]
        }
    }
}

# 排除 bot 流量（无浏览器信息的会话）
_BOT_EXCLUDE = {
    "not_expression": {
        "filter": {
            "field_name": "browser",
            "string_filter": {"match_type": "EXACT", "value": "(not set)"}
        }
    }
}


def _with_exclusions(base_filter):
    """将所有排除过滤器与已有过滤条件组合"""
    return {
        "and_group": {
            "expressions": [base_filter, _SOURCE_EXCLUDE, _BOT_EXCLUDE]
        }
    }


HOSTNAME_FILTER = _with_exclusions(_HOSTNAME_ALLOW)


def with_hostname_filter(existing_filter):
    """将 hostname 过滤与已有过滤条件组合"""
    return {
        "and_group": {
            "expressions": [HOSTNAME_FILTER, existing_filter]
        }
    }


def check_proxy_available(proxy_addr: str, timeout: float = 3.0) -> bool:
    """检查代理是否可用"""
    try:
        # 解析代理地址
        addr = proxy_addr.replace("http://", "").replace("https://", "")
        host, port = addr.split(":")
        port = int(port)

        # 尝试连接代理端口
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def check_google_connectivity(timeout: float = 5.0) -> bool:
    """检查是否能直连 Google（用于判断是否需要代理）"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        # analyticsdata.googleapis.com 的常用 IP
        result = sock.connect_ex(("analyticsdata.googleapis.com", 443))
        sock.close()
        return result == 0
    except Exception:
        return False


def setup_proxy():
    """设置代理环境变量"""
    # 检查是否已设置代理
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")

    if not proxy:
        # 没有设置代理，检查默认代理是否可用
        if check_proxy_available(DEFAULT_PROXY):
            proxy = DEFAULT_PROXY
            os.environ["HTTPS_PROXY"] = proxy
            os.environ["HTTP_PROXY"] = proxy
            print(f"  自动检测到代理: {proxy}")
        elif check_google_connectivity():
            print("  直连 Google 可用，无需代理")
            return True
        else:
            print("  ⚠️ 无法连接 Google，且未找到可用代理")
            return False

    # 验证代理是否可用
    if not check_proxy_available(proxy):
        print(f"  ❌ 代理不可用: {proxy}")
        # 尝试直连
        if check_google_connectivity():
            print("  尝试直连 Google...")
            os.environ.pop("HTTPS_PROXY", None)
            os.environ.pop("HTTP_PROXY", None)
            return True
        return False

    # 设置 gRPC 代理
    proxy_addr = proxy.replace("http://", "").replace("https://", "")
    os.environ["grpc_proxy"] = f"http://{proxy_addr}"
    os.environ["GRPC_PROXY"] = f"http://{proxy_addr}"
    print(f"  使用代理: {proxy_addr}")
    return True


def retry_on_failure(max_retries=MAX_RETRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF):
    """重试装饰器，带指数退避"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        print(f"  ⚠️  {func.__name__} 失败 (尝试 {attempt + 1}/{max_retries + 1}): {type(e).__name__}")
                        print(f"      {current_delay} 秒后重试...")
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        print(f"  ❌ {func.__name__} 最终失败: {e}")

            raise last_exception
        return wrapper
    return decorator


GA_API_TIMEOUT = 30  # 单次 API 调用超时（秒）


class _TimeoutClient:
    """为每个 API 调用自动添加超时的包装类"""

    def __init__(self, client):
        self._client = client

    def run_report(self, request, **kwargs):
        kwargs.setdefault("timeout", GA_API_TIMEOUT)
        return self._client.run_report(request, **kwargs)

    def run_realtime_report(self, request, **kwargs):
        kwargs.setdefault("timeout", GA_API_TIMEOUT)
        return self._client.run_realtime_report(request, **kwargs)


def get_client():
    """获取 GA 客户端（带默认超时）"""
    # 验证凭据文件存在
    if not CREDENTIALS_FILE.exists():
        raise FileNotFoundError(f"GA 凭据文件不存在: {CREDENTIALS_FILE}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CREDENTIALS_FILE)
    return _TimeoutClient(BetaAnalyticsDataClient())


@retry_on_failure()
def fetch_overview(days=7):
    """获取概览数据"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        metrics=[
            Metric(name="activeUsers"),           # 活跃用户
            Metric(name="sessions"),              # 会话数
            Metric(name="screenPageViews"),       # 页面浏览量
            Metric(name="averageSessionDuration"),# 平均会话时长
            Metric(name="bounceRate"),            # 跳出率
            Metric(name="newUsers"),              # 新用户
        ],
        dimension_filter=HOSTNAME_FILTER
    )

    response = client.run_report(request)

    if response.rows:
        row = response.rows[0]
        return {
            "active_users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
            "page_views": int(row.metric_values[2].value),
            "avg_session_duration": float(row.metric_values[3].value),
            "bounce_rate": float(row.metric_values[4].value),
            "new_users": int(row.metric_values[5].value),
        }
    return None


@retry_on_failure()
def fetch_yesterday_data():
    """获取昨天的完整数据（用于保存到数据库历史记录）"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="sessions"),
            Metric(name="screenPageViews"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
            Metric(name="newUsers"),
        ],
        dimension_filter=HOSTNAME_FILTER
    )

    response = client.run_report(request)

    if response.rows:
        row = response.rows[0]
        return {
            "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            "active_users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
            "page_views": int(row.metric_values[2].value),
            "avg_session_duration": float(row.metric_values[3].value),
            "bounce_rate": float(row.metric_values[4].value),
            "new_users": int(row.metric_values[5].value),
        }
    return None


@retry_on_failure()
def fetch_daily_trend(days=30):
    """获取每日趋势"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="sessions"),
            Metric(name="screenPageViews"),
            Metric(name="newUsers"),
        ],
        dimension_filter=HOSTNAME_FILTER,
        order_bys=[{"dimension": {"dimension_name": "date"}}]
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        date_str = row.dimension_values[0].value
        # 转换日期格式 YYYYMMDD -> YYYY-MM-DD
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        result.append({
            "date": date_formatted,
            "active_users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
            "page_views": int(row.metric_values[2].value),
            "new_users": int(row.metric_values[3].value),
        })

    return result


@retry_on_failure()
def fetch_page_daily_trend(days=14, segment_filter=None):
    """获取各页面的每日浏览趋势（取 top 10 页面）

    Args:
        days: 天数
        segment_filter: 可选的域名过滤器（如 FRONTEND_FILTER / BACKEND_FILTER），
                       不传则使用全局 HOSTNAME_FILTER
    """
    client = get_client()
    base_filter = segment_filter or HOSTNAME_FILTER

    # 先获取 top 10 热门页面
    top_request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="pagePath")],
        metrics=[Metric(name="screenPageViews")],
        dimension_filter=base_filter,
        order_bys=[{"metric": {"metric_name": "screenPageViews"}, "desc": True}],
        limit=10
    )
    top_response = client.run_report(top_request)
    top_pages = [row.dimension_values[0].value for row in top_response.rows]

    if not top_pages:
        return {}

    # 获取这些页面的每日数据
    page_filter = {
        "filter": {
            "field_name": "pagePath",
            "in_list_filter": {"values": top_pages}
        }
    }
    combined_filter = {
        "and_group": {
            "expressions": [base_filter, page_filter]
        }
    }

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="pagePath"),
        ],
        metrics=[Metric(name="screenPageViews")],
        dimension_filter=combined_filter,
        order_bys=[{"dimension": {"dimension_name": "date"}}],
        limit=500  # days * pages
    )

    response = client.run_report(request)

    # 组织数据：按页面分组
    result = {page: [] for page in top_pages}
    date_set = set()

    for row in response.rows:
        date_str = row.dimension_values[0].value
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        page = row.dimension_values[1].value
        views = int(row.metric_values[0].value)
        date_set.add(date_formatted)

        if page in result:
            result[page].append({"date": date_formatted, "views": views})

    # 排序每个页面的数据
    for page in result:
        result[page].sort(key=lambda x: x["date"])

    return {
        "pages": top_pages,
        "data": result,
        "dates": sorted(list(date_set))
    }


# 前台/后台按域名分类
FRONTEND_HOSTNAMES = ['example.com']
BACKEND_HOSTNAMES = ['app.example.com']


def _segment_hostname_filter(hostnames):
    """创建指定域名列表的过滤器"""
    if len(hostnames) == 1:
        return {
            "filter": {
                "field_name": "hostName",
                "string_filter": {"match_type": "EXACT", "value": hostnames[0]}
            }
        }
    return {
        "or_group": {
            "expressions": [
                {"filter": {"field_name": "hostName", "string_filter": {"match_type": "EXACT", "value": h}}}
                for h in hostnames
            ]
        }
    }


FRONTEND_FILTER = _with_exclusions(_segment_hostname_filter(FRONTEND_HOSTNAMES))
BACKEND_FILTER = _with_exclusions(_segment_hostname_filter(BACKEND_HOSTNAMES))


@retry_on_failure()
def fetch_segmented_overview(days=7):
    """获取前台/后台分段概览数据（按域名分类，含对比周期）"""
    client = get_client()

    def fetch_segment(start_date, end_date, segment_filter):
        """获取单个分段的数据"""
        # 页面浏览量、用户、会话
        page_request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name="pagePath")],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="activeUsers"),
                Metric(name="sessions"),
            ],
            dimension_filter=segment_filter,
            order_bys=[{"metric": {"metric_name": "activeUsers"}, "desc": True}],
            limit=100
        )
        page_response = client.run_report(page_request)

        total_views = 0
        total_sessions = 0
        pages = []
        for row in page_response.rows:
            page = row.dimension_values[0].value
            views = int(row.metric_values[0].value)
            users = int(row.metric_values[1].value)
            sessions = int(row.metric_values[2].value)
            total_views += views
            total_sessions += sessions
            pages.append({"page": page, "views": views, "users": users})

        # 独立用户数和平均时长
        user_request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="averageSessionDuration"),
            ],
            dimension_filter=segment_filter
        )
        user_response = client.run_report(user_request)

        total_users = 0
        avg_duration = 0
        if user_response.rows:
            total_users = int(user_response.rows[0].metric_values[0].value)
            avg_duration = float(user_response.rows[0].metric_values[1].value)

        return {
            "views": total_views,
            "users": total_users,
            "sessions": total_sessions,
            "avg_duration": avg_duration,
            "pages": sorted(pages, key=lambda x: -x["users"])[:10]
        }

    def fetch_period_data(start_date, end_date):
        """获取指定周期的前台后台数据"""
        frontend = fetch_segment(start_date, end_date, FRONTEND_FILTER)
        backend = fetch_segment(start_date, end_date, BACKEND_FILTER)
        return frontend, backend

    # 当前周期
    current_frontend, current_backend = fetch_period_data(f"{days}daysAgo", "yesterday")

    # 上一周期（用于对比）
    prev_frontend, prev_backend = fetch_period_data(f"{days * 2}daysAgo", f"{days + 1}daysAgo")

    return {
        "frontend": {
            **current_frontend,
            "changes": {
                "views": current_frontend["views"] - prev_frontend["views"],
                "users": current_frontend["users"] - prev_frontend["users"],
                "sessions": current_frontend["sessions"] - prev_frontend["sessions"]
            }
        },
        "backend": {
            **current_backend,
            "changes": {
                "views": current_backend["views"] - prev_backend["views"],
                "users": current_backend["users"] - prev_backend["users"],
                "sessions": current_backend["sessions"] - prev_backend["sessions"]
            }
        }
    }


@retry_on_failure()
def fetch_segmented_daily_trend(days=14):
    """获取前台/后台每日趋势（按域名分类）"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="hostName"),
        ],
        metrics=[
            Metric(name="screenPageViews"),
        ],
        dimension_filter=HOSTNAME_FILTER,
        order_bys=[{"dimension": {"dimension_name": "date"}}],
        limit=5000
    )

    response = client.run_report(request)

    # 按日期汇总，根据 hostname 归类
    daily_data = {}
    for row in response.rows:
        date_str = row.dimension_values[0].value
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        hostname = row.dimension_values[1].value
        views = int(row.metric_values[0].value)

        if date_formatted not in daily_data:
            daily_data[date_formatted] = {"frontend": 0, "backend": 0}

        if hostname in BACKEND_HOSTNAMES:
            daily_data[date_formatted]["backend"] += views
        elif hostname in FRONTEND_HOSTNAMES:
            daily_data[date_formatted]["frontend"] += views

    # 转换为列表格式
    result = []
    for date in sorted(daily_data.keys()):
        result.append({
            "date": date,
            "frontend": daily_data[date]["frontend"],
            "backend": daily_data[date]["backend"]
        })

    return result


@retry_on_failure()
def fetch_signup_by_source(days=30):
    """按首次获客来源统计新访客数、注册完成数、下单数，计算转化率

    使用 firstUserSource 而非 sessionSource，确保同一用户的注册和付费
    归因到最初带来该用户的来源，避免订单数 > 注册数的归因错位问题。
    """
    client = get_client()

    event_filter = {
        "filter": {
            "field_name": "eventName",
            "in_list_filter": {"values": [
                "first_visit", "sign_up_completed",
                "topup_payment_started", "payment_success", "topup_completed",
            ]}
        }
    }
    combined_filter = {
        "and_group": {"expressions": [HOSTNAME_FILTER, event_filter]}
    }

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="firstUserSource"), Dimension(name="eventName")],
        metrics=[Metric(name="totalUsers")],
        dimension_filter=combined_filter,
        order_bys=[{"metric": {"metric_name": "totalUsers"}, "desc": True}],
        limit=100
    )

    response = client.run_report(request)

    sources = {}
    for row in response.rows:
        source = row.dimension_values[0].value
        event = row.dimension_values[1].value
        users = int(row.metric_values[0].value)
        if source not in sources:
            sources[source] = {"source": source, "new_visitors": 0, "signups": 0,
                               "orders": 0, "paid": 0}
        if event == "first_visit":
            sources[source]["new_visitors"] = users
        elif event == "sign_up_completed":
            sources[source]["signups"] = users
        elif event == "topup_payment_started":
            sources[source]["orders"] = users
        elif event in ("payment_success", "topup_completed"):
            # 取两个支付成功事件中较大的值作为实际付费数
            sources[source]["paid"] = max(sources[source]["paid"], users)

    result = []
    for s in sources.values():
        # 优先使用 payment_success/topup_completed 作为订单数，回退到 topup_payment_started
        if s["paid"] > 0:
            s["orders"] = s["paid"]
        # 防护：订单不应超过注册数
        s["orders"] = min(s["orders"], s["signups"])
        if s["new_visitors"] > 0:
            s["conversion_rate"] = round(s["signups"] / s["new_visitors"] * 100, 1)
        else:
            s["conversion_rate"] = 0
        if s["signups"] > 0:
            s["order_rate"] = round(s["orders"] / s["signups"] * 100, 1)
        else:
            s["order_rate"] = 0
        del s["paid"]  # 不暴露中间字段
        result.append(s)

    result.sort(key=lambda x: (-x["signups"], -x["conversion_rate"]))
    return result[:15]


def fetch_traffic_sources(days=30, segment_filter=None):
    """获取流量来源"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="sessionSource")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
        ],
        dimension_filter=segment_filter or HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        limit=10
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "source": row.dimension_values[0].value,
            "sessions": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        })

    return result


@retry_on_failure()
def fetch_top_pages(days=30):
    """获取热门页面"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="pagePath")],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="activeUsers"),
        ],
        dimension_filter=HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "activeUsers"}, "desc": True}],
        limit=10
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "page": row.dimension_values[0].value,
            "views": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        })

    return result


@retry_on_failure()
def fetch_geo(days=30):
    """获取地理位置分布"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="country")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="sessions"),
        ],
        dimension_filter=HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "activeUsers"}, "desc": True}],
        limit=10
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "country": row.dimension_values[0].value,
            "users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
        })

    return result


@retry_on_failure()
def fetch_devices(days=30):
    """获取设备类型分布"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="deviceCategory")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="sessions"),
        ],
        dimension_filter=HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}]
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "device": row.dimension_values[0].value,
            "users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
        })

    return result


@retry_on_failure()
def fetch_operating_systems(days=30):
    """获取操作系统分布"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="operatingSystem")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="sessions"),
        ],
        dimension_filter=HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        limit=10
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "os": row.dimension_values[0].value,
            "users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
        })

    return result


@retry_on_failure()
def fetch_languages(days=30):
    """获取语言分布"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="language")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="sessions"),
        ],
        dimension_filter=HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        limit=10
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "language": row.dimension_values[0].value,
            "users": int(row.metric_values[0].value),
            "sessions": int(row.metric_values[1].value),
        })

    return result


@retry_on_failure()
def fetch_landing_pages(days=30, segment_filter=None):
    """获取着陆页分布"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="landingPage")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
            Metric(name="bounceRate"),
        ],
        dimension_filter=segment_filter or HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        limit=10
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "page": row.dimension_values[0].value,
            "sessions": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
            "bounce_rate": float(row.metric_values[2].value),
        })

    return result


@retry_on_failure()
def fetch_exit_pages(days=30, segment_filter=None):
    """获取退出页分布"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="pagePath")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
        ],
        dimension_filter=segment_filter or HOSTNAME_FILTER,
        order_bys=[{"metric": {"metric_name": "sessions"}, "desc": True}],
        limit=10
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        result.append({
            "page": row.dimension_values[0].value,
            "sessions": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        })

    return result


@retry_on_failure()
def fetch_signups(days=30):
    """获取注册数据"""
    client = get_client()

    # 获取注册总数
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[Dimension(name="eventName")],
        metrics=[
            Metric(name="eventCount"),
            Metric(name="totalUsers"),
        ],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "string_filter": {"value": "sign_up_completed"}
            }
        })
    )

    response = client.run_report(request)

    total_signups = 0
    total_users = 0
    if response.rows:
        total_signups = int(response.rows[0].metric_values[0].value)
        total_users = int(response.rows[0].metric_values[1].value)

    # 获取按注册方式分布（如果有 method 参数）
    method_request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[
            Dimension(name="eventName"),
            Dimension(name="customEvent:method"),  # 自定义参数
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "string_filter": {"value": "sign_up_completed"}
            }
        }),
        order_bys=[{"metric": {"metric_name": "eventCount"}, "desc": True}]
    )

    by_method = []
    try:
        method_response = client.run_report(method_request)
        for row in method_response.rows:
            method = row.dimension_values[1].value
            if method and method != "(not set)":
                by_method.append({
                    "method": method,
                    "count": int(row.metric_values[0].value)
                })
    except Exception:
        pass  # 如果没有 method 参数，忽略

    return {
        "total": total_signups,
        "users": total_users,
        "by_method": by_method
    }


@retry_on_failure()
def fetch_signup_by_method_daily(days=30):
    """按注册方式 + 日期统计注册数，用于诊断 GA 注册丢失问题"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="customEvent:method"),
        ],
        metrics=[
            Metric(name="eventCount"),
            Metric(name="totalUsers"),
        ],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "string_filter": {"value": "sign_up_completed"}
            }
        }),
        order_bys=[{"dimension": {"dimension_name": "date"}}],
        limit=1000
    )

    response = client.run_report(request)

    from collections import defaultdict
    daily = defaultdict(lambda: {})
    methods_set = set()
    for row in response.rows:
        date_raw = row.dimension_values[0].value
        date = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        method = row.dimension_values[1].value or "(not set)"
        count = int(row.metric_values[0].value)
        users = int(row.metric_values[1].value)
        daily[date][method] = {"count": count, "users": users}
        methods_set.add(method)

    result = []
    for date in sorted(daily.keys()):
        entry = {"date": date, "methods": daily[date]}
        entry["total_count"] = sum(v["count"] for v in daily[date].values())
        entry["total_users"] = sum(v["users"] for v in daily[date].values())
        result.append(entry)

    return {"daily": result, "methods": sorted(methods_set)}


@retry_on_failure()
def fetch_signup_trend(days=30):
    """获取每日注册趋势"""
    client = get_client()

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=f"{days}daysAgo", end_date="yesterday")],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="eventName"),
        ],
        metrics=[Metric(name="eventCount")],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "string_filter": {"value": "sign_up_completed"}
            }
        }),
        order_bys=[{"dimension": {"dimension_name": "date"}}]
    )

    response = client.run_report(request)

    result = []
    for row in response.rows:
        date_str = row.dimension_values[0].value
        date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        result.append({
            "date": date_formatted,
            "signups": int(row.metric_values[0].value)
        })

    return result


@retry_on_failure()
def fetch_conversion_funnel(start_date="2026-02-05", end_date="yesterday"):
    """完整转化漏斗：新访客 → 注册 → API Key → 充值"""
    client = get_client()

    funnel_events = [
        "first_visit", "sign_up_started", "sign_up_completed",
        "api_key_created", "topup_payment_started", "topup_coupon_applied",
        "payment_success", "topup_completed",
    ]

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount"), Metric(name="totalUsers")],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "in_list_filter": {"values": funnel_events}
            }
        }),
    )
    response = client.run_report(request)

    event_data = {}
    for row in response.rows:
        event_data[row.dimension_values[0].value] = {
            "count": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        }

    steps = [
        ("first_visit", "新访客"),
        ("sign_up_started", "开始注册"),
        ("sign_up_completed", "注册完成"),
        ("api_key_created", "创建API Key"),
        ("topup_payment_started", "发起充值"),
        ("topup_coupon_applied", "使用优惠券"),
        ("payment_success", "支付成功"),
        ("topup_completed", "充值完成"),
    ]
    result = []
    top_users = 0
    prev_users = 0
    for event, label in steps:
        d = event_data.get(event, {"count": 0, "users": 0})
        step = {"event": event, "label": label, "users": d["users"], "count": d["count"]}
        if event == "first_visit":
            top_users = d["users"]
        if prev_users > 0:
            step["step_rate"] = round(d["users"] / prev_users * 100, 1)
        if top_users > 0:
            step["overall_rate"] = round(d["users"] / top_users * 100, 1)
        result.append(step)
        prev_users = d["users"]

    return {"steps": result, "start_date": start_date, "end_date": end_date}


@retry_on_failure()
def fetch_conversion_funnel_daily(start_date="2026-02-05", end_date="yesterday"):
    """完整转化漏斗每日趋势"""
    client = get_client()

    funnel_events = [
        "first_visit", "sign_up_started", "sign_up_completed",
        "api_key_created", "topup_payment_started", "topup_coupon_applied",
        "payment_success", "topup_completed",
    ]

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="date"), Dimension(name="eventName")],
        metrics=[Metric(name="totalUsers")],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "in_list_filter": {"values": funnel_events}
            }
        }),
        order_bys=[{"dimension": {"dimension_name": "date"}}],
        limit=500
    )
    response = client.run_report(request)

    from collections import defaultdict
    daily = defaultdict(lambda: {e: 0 for e in funnel_events})
    for row in response.rows:
        date_raw = row.dimension_values[0].value
        date = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        event = row.dimension_values[1].value
        daily[date][event] = int(row.metric_values[0].value)

    return [{"date": d, **daily[d]} for d in sorted(daily.keys())]


@retry_on_failure()
def fetch_conversion_funnel_weekly(start_date="2026-02-05", end_date="yesterday"):
    """完整转化漏斗按周汇总"""
    client = get_client()

    funnel_events = [
        "first_visit", "sign_up_started", "sign_up_completed",
        "api_key_created", "topup_payment_started", "topup_coupon_applied",
        "payment_success", "topup_completed",
    ]

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="isoWeek"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount"), Metric(name="totalUsers")],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "in_list_filter": {"values": funnel_events}
            }
        }),
        order_bys=[{"dimension": {"dimension_name": "isoWeek"}}],
        limit=500
    )
    response = client.run_report(request)

    # isoWeek 只返回周数，需要算出周起始日期
    from collections import defaultdict
    weeks = defaultdict(lambda: {e: {"count": 0, "users": 0} for e in funnel_events})
    for row in response.rows:
        week_num = int(row.dimension_values[0].value)
        event = row.dimension_values[1].value
        weeks[week_num][event] = {
            "count": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        }

    # 把周数转为日期范围
    from datetime import datetime as dt, timedelta
    result = []
    for week_num in sorted(weeks.keys()):
        # ISO week → Monday date (2026年)
        week_start = dt.strptime(f"2026-W{week_num:02d}-1", "%G-W%V-%u")
        week_end = week_start + timedelta(days=6)
        week_label = f"{week_start.strftime('%m/%d')}-{week_end.strftime('%m/%d')}"

        week_data = {"week": week_num, "week_label": week_label, "start_date": week_start.strftime("%Y-%m-%d")}
        prev_users = 0
        for event in funnel_events:
            d = weeks[week_num][event]
            week_data[event] = d["users"]
            week_data[f"{event}_count"] = d["count"]
            if event == "first_visit":
                top_users = d["users"]
            if prev_users > 0 and d["users"] > 0:
                week_data[f"{event}_rate"] = round(d["users"] / prev_users * 100, 1)
            if event != "first_visit" and top_users > 0:
                week_data[f"{event}_overall"] = round(d["users"] / top_users * 100, 1)
            prev_users = d["users"] if d["users"] > 0 else prev_users
        result.append(week_data)

    return result


@retry_on_failure()
def fetch_march_promo_funnel():
    """三月活动漏斗：活动曝光 → 点击 → 注册 → 充值 → 推荐"""
    client = get_client()

    promo_events = [
        "march_promo_banner_clicked", "march_promo_cta_clicked",
        "promo_card_clicked",
        "welcome_modal_viewed", "welcome_modal_dismissed",
        "welcome_modal_cta_clicked", "welcome_modal_endpoint_copied",
        "sns_link_clicked",
        "referral_page_viewed", "referral_program_joined",
        "referral_code_copied", "referral_poster_downloaded",
        "sign_up_completed", "topup_payment_started", "topup_coupon_applied",
        "payment_success",
    ]

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date="2026-03-03", end_date="yesterday")],
        dimensions=[Dimension(name="eventName")],
        metrics=[Metric(name="eventCount"), Metric(name="totalUsers")],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "in_list_filter": {"values": promo_events}
            }
        }),
    )
    response = client.run_report(request)

    event_data = {}
    for row in response.rows:
        event_data[row.dimension_values[0].value] = {
            "count": int(row.metric_values[0].value),
            "users": int(row.metric_values[1].value),
        }

    steps = [
        ("march_promo_banner_clicked", "活动Banner点击"),
        ("march_promo_cta_clicked", "3月活动CTA点击"),
        ("promo_card_clicked", "活动卡片点击"),
        ("welcome_modal_viewed", "欢迎弹窗展示"),
        ("welcome_modal_cta_clicked", "欢迎弹窗CTA"),
        ("welcome_modal_endpoint_copied", "复制API Endpoint"),
        ("welcome_modal_dismissed", "欢迎弹窗关闭"),
        ("sns_link_clicked", "社交链接点击"),
        ("sign_up_completed", "注册完成"),
        ("topup_payment_started", "发起充值"),
        ("topup_coupon_applied", "使用优惠券"),
        ("payment_success", "支付成功"),
        ("referral_page_viewed", "推荐页浏览"),
        ("referral_program_joined", "加入推荐计划"),
        ("referral_code_copied", "复制推荐码"),
        ("referral_poster_downloaded", "下载推荐海报"),
    ]
    result = []
    for event, label in steps:
        d = event_data.get(event, {"count": 0, "users": 0})
        result.append({"event": event, "label": label, "users": d["users"], "count": d["count"]})

    return {"steps": result, "start_date": "2026-03-03"}


@retry_on_failure()
def fetch_march_promo_daily():
    """三月活动每日趋势"""
    client = get_client()

    promo_events = [
        "march_promo_banner_clicked", "march_promo_cta_clicked",
        "promo_card_clicked",
        "welcome_modal_viewed", "welcome_modal_cta_clicked",
        "sns_link_clicked", "payment_success",
        "referral_program_joined", "topup_coupon_applied",
        "sign_up_completed", "topup_payment_started",
    ]

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        date_ranges=[DateRange(start_date="2026-03-03", end_date="yesterday")],
        dimensions=[Dimension(name="date"), Dimension(name="eventName")],
        metrics=[Metric(name="totalUsers")],
        dimension_filter=with_hostname_filter({
            "filter": {
                "field_name": "eventName",
                "in_list_filter": {"values": promo_events}
            }
        }),
        order_bys=[{"dimension": {"dimension_name": "date"}}],
        limit=500
    )
    response = client.run_report(request)

    from collections import defaultdict
    daily = defaultdict(lambda: {e: 0 for e in promo_events})
    for row in response.rows:
        date_raw = row.dimension_values[0].value
        date = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        event = row.dimension_values[1].value
        daily[date][event] = int(row.metric_values[0].value)

    return [{"date": d, **daily[d]} for d in sorted(daily.keys())]


def fetch_real_registrations():
    """获取真实注册数据

    优先从后台 API 获取，回退到本地缓存。
    GA 的 sign_up_completed 事件数据不可靠，以后台数据为准。
    """
    try:
        from collect_registrations import get_registration_data
        data = get_registration_data(days=30)
    except ImportError:
        # 回退：直接读缓存文件
        reg_file = ROOT_DIR / "data" / "registration_data.json"
        if reg_file.exists():
            with open(reg_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {"daily": [], "total": 0}

    daily = data.get("daily", [])
    total = data.get("total", sum(d.get("count", 0) for d in daily))

    today = datetime.now()
    seven_days_ago = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    daily_dict = {d["date"]: d["count"] for d in daily} if daily else {}
    last_7_days = sum(v for k, v in daily_dict.items() if k > seven_days_ago)
    yesterday_count = daily_dict.get(yesterday, 0)

    return {
        "total": total,
        "last_7_days": last_7_days,
        "last_30_days": total,
        "yesterday": yesterday_count,
        "daily": daily
    }


def fetch_realtime_users():
    """获取实时在线用户数"""
    from google.analytics.data_v1beta.types import RunRealtimeReportRequest

    client = get_client()

    # 注意：Realtime API 不支持 hostName 维度过滤
    request = RunRealtimeReportRequest(
        property=f"properties/{PROPERTY_ID}",
        metrics=[Metric(name="activeUsers")]
    )

    try:
        response = client.run_realtime_report(request)
        if response.rows:
            return int(response.rows[0].metric_values[0].value)
        return 0
    except Exception as e:
        print(f"实时数据获取失败: {e}")
        return 0


def collect_all():
    """采集所有 GA 数据"""
    print("正在采集 Google Analytics 数据...")

    # 预检查：网络连接和代理
    print("  检查网络连接...")
    if not setup_proxy():
        raise ConnectionError("无法连接 Google Analytics API，请检查网络或代理配置")

    # 预检查：验证凭据文件
    if not CREDENTIALS_FILE.exists():
        print(f"❌ 错误: GA 凭据文件不存在: {CREDENTIALS_FILE}")
        raise FileNotFoundError(f"GA 凭据文件不存在: {CREDENTIALS_FILE}")

    # 初始化数据库
    init_db()

    errors = []

    def safe_fetch(name, func, *args, **kwargs):
        """安全地执行 fetch 操作，记录错误但不中断"""
        try:
            print(f"  采集 {name}...")
            result = func(*args, **kwargs)
            print(f"  ✅ {name} 完成")
            return result
        except Exception as e:
            print(f"  ❌ {name} 失败: {e}")
            errors.append((name, str(e)))
            return None

    # 获取昨日完整数据（用于存储到数据库历史记录）
    # 注意：使用昨天的数据而不是今天的，因为今天的数据在采集时刻还不完整
    yesterday_data = safe_fetch("昨日数据", fetch_yesterday_data)

    data = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "realtime_users": safe_fetch("实时用户", fetch_realtime_users),
        "overview_1d": safe_fetch("昨天概览", fetch_overview, days=1),
        "overview_7d": safe_fetch("7天概览", fetch_overview, days=7),
        "overview_30d": safe_fetch("30天概览", fetch_overview, days=30),
        "daily_trend": safe_fetch("每日趋势", fetch_daily_trend, days=30),
        "page_daily_trend": safe_fetch("页面每日趋势", fetch_page_daily_trend, days=14),
        "page_daily_trend_frontend": safe_fetch("页面每日趋势(前台)", fetch_page_daily_trend, days=14, segment_filter=FRONTEND_FILTER),
        "page_daily_trend_backend": safe_fetch("页面每日趋势(后台)", fetch_page_daily_trend, days=14, segment_filter=BACKEND_FILTER),
        "segmented_1d": safe_fetch("前后台昨天", fetch_segmented_overview, days=1),
        "segmented_7d": safe_fetch("前后台7天", fetch_segmented_overview, days=7),
        "segmented_30d": safe_fetch("前后台30天", fetch_segmented_overview, days=30),
        "segmented_trend": safe_fetch("前后台趋势", fetch_segmented_daily_trend, days=30),
        "signup_by_source_1d": safe_fetch("注册来源昨天", fetch_signup_by_source, days=1),
        "signup_by_source_7d": safe_fetch("注册来源7天", fetch_signup_by_source, days=7),
        "signup_by_source_30d": safe_fetch("注册来源30天", fetch_signup_by_source, days=30),
        "traffic_sources": safe_fetch("流量来源", fetch_traffic_sources, days=30),
        "traffic_sources_frontend": safe_fetch("流量来源(前台)", fetch_traffic_sources, days=30, segment_filter=FRONTEND_FILTER),
        "traffic_sources_backend": safe_fetch("流量来源(后台)", fetch_traffic_sources, days=30, segment_filter=BACKEND_FILTER),
        "top_pages": safe_fetch("热门页面", fetch_top_pages, days=30),
        "geo": safe_fetch("地理分布", fetch_geo, days=30),
        "devices": safe_fetch("设备类型", fetch_devices, days=30),
        "operating_systems": safe_fetch("操作系统", fetch_operating_systems, days=30),
        "languages": safe_fetch("语言分布", fetch_languages, days=30),
        "landing_pages": safe_fetch("着陆页", fetch_landing_pages, days=30),
        "landing_pages_frontend": safe_fetch("着陆页(前台)", fetch_landing_pages, days=30, segment_filter=FRONTEND_FILTER),
        "landing_pages_backend": safe_fetch("着陆页(后台)", fetch_landing_pages, days=30, segment_filter=BACKEND_FILTER),
        "exit_pages": safe_fetch("退出页", fetch_exit_pages, days=30),
        "exit_pages_frontend": safe_fetch("退出页(前台)", fetch_exit_pages, days=30, segment_filter=FRONTEND_FILTER),
        "exit_pages_backend": safe_fetch("退出页(后台)", fetch_exit_pages, days=30, segment_filter=BACKEND_FILTER),
        "signups_1d": safe_fetch("昨日注册", fetch_signups, days=1),
        "signups_7d": safe_fetch("7天注册", fetch_signups, days=7),
        "signups_30d": safe_fetch("30天注册", fetch_signups, days=30),
        "signup_trend": safe_fetch("注册趋势", fetch_signup_trend, days=30),
        "signup_by_method_daily": safe_fetch("注册方式每日统计", fetch_signup_by_method_daily, days=30),
        "real_registrations": safe_fetch("真实注册", fetch_real_registrations),
        # 转化漏斗
        "conversion_funnel": safe_fetch("完整转化漏斗", fetch_conversion_funnel),
        "conversion_funnel_daily": safe_fetch("转化漏斗趋势", fetch_conversion_funnel_daily),
        "conversion_funnel_weekly": safe_fetch("转化漏斗周报", fetch_conversion_funnel_weekly),
        # 三月活动漏斗
        "march_promo_funnel": safe_fetch("3月活动漏斗", fetch_march_promo_funnel),
        "march_promo_daily": safe_fetch("3月活动趋势", fetch_march_promo_daily),
    }

    # 保存昨日完整数据到数据库
    if yesterday_data:
        save_daily_ga(yesterday_data)
        print(f"昨日 ({yesterday_data.get('date')}) GA 数据已保存到数据库")

    # 获取历史数据用于对比
    ga_history = get_ga_history(days=35)
    data["history"] = ga_history

    # 检查是否有关键数据
    critical_fields = ["overview_7d", "overview_30d", "daily_trend"]
    missing_critical = [f for f in critical_fields if data.get(f) is None]

    if missing_critical:
        print(f"\n❌ 关键数据缺失: {missing_critical}")
        raise RuntimeError(f"GA 数据采集失败，关键数据缺失: {missing_critical}")

    # 保存到 JSON
    output_file = ROOT_DIR / "data" / "ga_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ GA 数据已保存到 {output_file}")

    if errors:
        print(f"⚠️  有 {len(errors)} 个非关键错误:")
        for name, err in errors:
            print(f"   - {name}: {err}")

    return data


def print_summary():
    """打印数据摘要"""
    data = collect_all()

    print("\n" + "=" * 50)
    print("Google Analytics 数据摘要")
    print("=" * 50)

    if data["overview_7d"]:
        o = data["overview_7d"]
        print(f"\n【最近 7 天】")
        print(f"  活跃用户: {o['active_users']:,}")
        print(f"  新用户: {o['new_users']:,}")
        print(f"  会话数: {o['sessions']:,}")
        print(f"  页面浏览: {o['page_views']:,}")
        print(f"  平均时长: {o['avg_session_duration']:.1f} 秒")
        print(f"  跳出率: {o['bounce_rate']*100:.1f}%")

    if data["traffic_sources"]:
        print(f"\n【流量来源 TOP 5】")
        for i, src in enumerate(data["traffic_sources"][:5], 1):
            print(f"  {i}. {src['source']}: {src['sessions']} 会话")

    if data["top_pages"]:
        print(f"\n【热门页面 TOP 5】")
        for i, page in enumerate(data["top_pages"][:5], 1):
            print(f"  {i}. {page['page'][:40]}: {page['views']} 浏览")

    if data.get("signups_7d"):
        s7 = data["signups_7d"]
        s30 = data.get("signups_30d", {})
        print(f"\n【用户注册】")
        print(f"  最近 7 天: {s7.get('total', 0)} 次注册")
        print(f"  最近 30 天: {s30.get('total', 0)} 次注册")
        if s30.get("by_method"):
            print(f"  注册方式分布:")
            for m in s30["by_method"][:5]:
                print(f"    - {m['method']}: {m['count']} 次")


if __name__ == "__main__":
    print_summary()
