"""
SQLite 数据库管理模块
"""
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "tracker.db"


def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化数据库表结构"""
    conn = get_connection()
    cursor = conn.cursor()

    # 每日账号数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            platform TEXT NOT NULL,
            account_name TEXT DEFAULT '',
            account_id TEXT DEFAULT '',
            avatar_url TEXT DEFAULT '',
            followers INTEGER DEFAULT 0,
            total_views INTEGER DEFAULT 0,
            total_likes INTEGER DEFAULT 0,
            total_comments INTEGER DEFAULT 0,
            total_shares INTEGER DEFAULT 0,
            total_collects INTEGER DEFAULT 0,
            total_works INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, platform)
        )
    """)

    # 作品表（存储最新状态）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS works (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            title TEXT DEFAULT '',
            publish_time TEXT DEFAULT '',
            cover_url TEXT DEFAULT '',
            url TEXT DEFAULT '',
            views INTEGER DEFAULT 0,
            likes INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0,
            shares INTEGER DEFAULT 0,
            collects INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(work_id)
        )
    """)

    # GA 每日数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_ga (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            active_users INTEGER DEFAULT 0,
            sessions INTEGER DEFAULT 0,
            page_views INTEGER DEFAULT 0,
            avg_session_duration REAL DEFAULT 0,
            bounce_rate REAL DEFAULT 0,
            new_users INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 每日订单数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL UNIQUE,
            order_count INTEGER DEFAULT 0,
            order_amount REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_accounts(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_platform ON daily_accounts(platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_platform_date ON daily_accounts(platform, date DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_works_platform ON works(platform)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_works_platform_time ON works(platform, publish_time DESC)")

    conn.commit()
    conn.close()


def save_daily_account(platform, account_data):
    """保存每日账号数据（含异常数据校验 + 智能继承）

    采集到部分数据时，真实部分保留，异常归零的字段从历史继承。
    """
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    cursor = conn.cursor()

    # 获取最近一条历史数据用于校验和继承
    cursor.execute("""
        SELECT account_name, account_id, avatar_url,
               followers, total_views, total_likes, total_comments,
               total_shares, total_collects, total_works
        FROM daily_accounts WHERE platform = ?
        ORDER BY date DESC LIMIT 1
    """, (platform,))
    prev = cursor.fetchone()

    # 构建要写入的数据，先用采集到的新值
    merged = {
        "account_name": account_data.get("account_name", ""),
        "account_id": account_data.get("account_id", ""),
        "avatar_url": account_data.get("avatar_url", ""),
        "followers": account_data.get("followers", 0),
        "total_views": account_data.get("total_views", 0),
        "total_likes": account_data.get("total_likes", 0),
        "total_comments": account_data.get("total_comments", 0),
        "total_shares": account_data.get("total_shares", 0),
        "total_collects": account_data.get("total_collects", 0),
        "total_works": account_data.get("total_works", 0),
    }

    if prev:
        p_name, p_id, p_avatar, p_followers, p_views, p_likes, p_comments, p_shares, p_collects, p_works = prev

        # 互动指标保护：累计值不应大幅回退
        # 1) 归零 → 继承历史值
        # 2) 下降超过 20% → 视为采集不完整，取历史值
        inherit_fields = {
            "total_works": p_works,
            "total_views": p_views,
            "total_likes": p_likes,
            "total_comments": p_comments,
            "total_shares": p_shares,
            "total_collects": p_collects,
        }
        inherited = []
        for field, old_val in inherit_fields.items():
            new_val = merged[field]
            if old_val > 0 and (new_val == 0 or new_val < old_val * 0.8):
                merged[field] = old_val
                inherited.append(f"{field}({new_val}→{old_val})")
        if inherited:
            print(f"⚠️ [{platform}] 数据异常回退，已从历史继承: {', '.join(inherited)}")

        # 账号名/ID/头像：如果新值为空但历史有值，继承
        if not merged["account_name"] and p_name:
            merged["account_name"] = p_name
        if not merged["account_id"] and p_id:
            merged["account_id"] = p_id
        if not merged["avatar_url"] and p_avatar:
            merged["avatar_url"] = p_avatar

    # 用 DB 中该平台全部作品重新算总量（API 单次可能不返回所有作品）
    db_totals = cursor.execute("""
        SELECT COUNT(*) as cnt,
               COALESCE(SUM(views), 0) as views,
               COALESCE(SUM(likes), 0) as likes,
               COALESCE(SUM(comments), 0) as comments,
               COALESCE(SUM(shares), 0) as shares,
               COALESCE(SUM(collects), 0) as collects
        FROM works WHERE platform = ?
    """, (platform,)).fetchone()

    if db_totals and db_totals[0] > 0:
        db_works, db_views, db_likes, db_comments, db_shares, db_collects = db_totals
        # 取 DB 全量作品加总和当前值的较大值
        for field, db_val in [
            ("total_works", db_works), ("total_views", db_views),
            ("total_likes", db_likes), ("total_comments", db_comments),
            ("total_shares", db_shares), ("total_collects", db_collects),
        ]:
            if db_val > merged[field]:
                merged[field] = db_val

    cursor.execute("""
        INSERT OR REPLACE INTO daily_accounts
        (date, platform, account_name, account_id, avatar_url,
         followers, total_views, total_likes, total_comments,
         total_shares, total_collects, total_works, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today, platform,
        merged["account_name"], merged["account_id"], merged["avatar_url"],
        merged["followers"], merged["total_views"], merged["total_likes"],
        merged["total_comments"], merged["total_shares"], merged["total_collects"],
        merged["total_works"], now
    ))

    conn.commit()
    conn.close()
    return True


def save_works(platform, works_list):
    """保存作品数据（更新或插入）"""
    conn = get_connection()
    cursor = conn.cursor()

    for work in works_list:
        cursor.execute("""
            INSERT OR REPLACE INTO works
            (work_id, platform, title, publish_time, cover_url, url,
             views, likes, comments, shares, collects, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            work.get("work_id", ""),
            platform,
            work.get("title", ""),
            work.get("publish_time", ""),
            work.get("cover_url", ""),
            work.get("url", ""),
            work.get("views", 0),
            work.get("likes", 0),
            work.get("comments", 0),
            work.get("shares", 0),
            work.get("collects", 0)
        ))

    conn.commit()
    conn.close()


def save_daily_ga(ga_data, target_date=None):
    """保存每日 GA 数据

    Args:
        ga_data: GA 数据字典
        target_date: 指定日期，如果不指定则使用 ga_data 中的 date 字段，
                     如果都没有则使用今天
    """
    if target_date is None:
        target_date = ga_data.get("date", datetime.now().strftime("%Y-%m-%d"))

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO daily_ga
        (date, active_users, sessions, page_views, avg_session_duration, bounce_rate, new_users)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        target_date,
        ga_data.get("active_users", 0),
        ga_data.get("sessions", 0),
        ga_data.get("page_views", 0),
        ga_data.get("avg_session_duration", 0),
        ga_data.get("bounce_rate", 0),
        ga_data.get("new_users", 0)
    ))

    conn.commit()
    conn.close()


def save_daily_orders(date, order_count, order_amount):
    """保存每日订单数据"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO daily_orders (date, order_count, order_amount, created_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
    """, (date, order_count, order_amount))
    conn.commit()
    conn.close()


def get_orders_history(days=90):
    """获取最近 N 天的订单历史数据"""
    conn = get_connection()
    cursor = conn.cursor()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT date, order_count, order_amount
        FROM daily_orders WHERE date >= ?
        ORDER BY date ASC
    """, (cutoff,))
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def get_ga_by_date(target_date):
    """获取指定日期的 GA 数据"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM daily_ga WHERE date = ?
    """, (target_date,))

    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def get_ga_history(days=30):
    """获取最近 N 天的 GA 历史数据"""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    cursor.execute("""
        SELECT * FROM daily_ga
        WHERE date >= ?
        ORDER BY date DESC
    """, (cutoff,))

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return rows


def get_latest_account(platform):
    """获取平台最新账号数据"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM daily_accounts
        WHERE platform = ?
        ORDER BY date DESC
        LIMIT 1
    """, (platform,))

    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def get_previous_account(platform, before_date):
    """获取指定日期之前的最新数据（用于计算变化）"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM daily_accounts
        WHERE platform = ? AND date < ?
        ORDER BY date DESC
        LIMIT 1
    """, (platform, before_date))

    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def get_works_by_platform(platform, limit=50):
    """获取平台的作品列表"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM works
        WHERE platform = ?
        ORDER BY publish_time DESC
        LIMIT ?
    """, (platform, limit))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_daily_data(days=30):
    """获取最近 N 天的每日数据"""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    cursor.execute("""
        SELECT * FROM daily_accounts
        WHERE date >= ?
        ORDER BY date DESC, platform
    """, (cutoff,))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_platform_trend(platform, days=30):
    """获取平台的趋势数据"""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    cursor.execute("""
        SELECT date, followers, total_views, total_likes,
               total_comments, total_shares, total_collects, total_works
        FROM daily_accounts
        WHERE platform = ? AND date >= ?
        ORDER BY date ASC
    """, (platform, cutoff))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_stats_summary():
    """获取统计摘要（用于快速查询）"""
    conn = get_connection()
    cursor = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")

    result = {}

    for platform in ["douyin", "xiaohongshu", "shipinhao", "gongzhonghao"]:
        # 获取今日数据
        cursor.execute("""
            SELECT * FROM daily_accounts
            WHERE platform = ? AND date = ?
        """, (platform, today))
        current = cursor.fetchone()

        # 获取昨日数据
        cursor.execute("""
            SELECT * FROM daily_accounts
            WHERE platform = ? AND date < ?
            ORDER BY date DESC LIMIT 1
        """, (platform, today))
        previous = cursor.fetchone()

        if current:
            current = dict(current)
            if previous:
                previous = dict(previous)
                current["followers_change"] = current["followers"] - previous["followers"]
                current["views_change"] = current["total_views"] - previous["total_views"]
                current["likes_change"] = current["total_likes"] - previous["total_likes"]
                current["comments_change"] = current["total_comments"] - previous["total_comments"]
                current["shares_change"] = current["total_shares"] - previous["total_shares"]
                current["collects_change"] = current["total_collects"] - previous["total_collects"]
                current["works_change"] = current["total_works"] - previous["total_works"]
            else:
                for key in ["followers", "views", "likes", "comments", "shares", "collects", "works"]:
                    current[f"{key}_change"] = 0

            result[platform] = current

    conn.close()
    return result


def export_for_frontend():
    """导出前端需要的 JSON 数据（优化版：单次连接，批量查询）"""
    conn = get_connection()
    cursor = conn.cursor()

    data = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "daily_snapshots": []
    }

    cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    # 一次性获取所有每日数据（按平台和日期排序）
    cursor.execute("""
        SELECT * FROM daily_accounts
        WHERE date >= ?
        ORDER BY platform, date ASC
    """, (cutoff,))
    all_daily = [dict(row) for row in cursor.fetchall()]

    # 按平台分组，方便计算变化
    by_platform = {}
    for row in all_daily:
        platform = row["platform"]
        if platform not in by_platform:
            by_platform[platform] = []
        by_platform[platform].append(row)

    # 计算每日快照（带变化值）
    for platform, rows in by_platform.items():
        prev = None
        for row in rows:
            snapshot = {
                "date": row["date"],
                "platform": platform,
                "followers": row["followers"],
                "followers_change": row["followers"] - (prev["followers"] if prev else 0),
                "total_views": row["total_views"],
                "views_change": row["total_views"] - (prev["total_views"] if prev else 0),
                "total_likes": row["total_likes"],
                "likes_change": row["total_likes"] - (prev["total_likes"] if prev else 0),
                "total_comments": row["total_comments"],
                "comments_change": row["total_comments"] - (prev["total_comments"] if prev else 0),
                "total_shares": row["total_shares"],
                "shares_change": row["total_shares"] - (prev["total_shares"] if prev else 0),
                "total_collects": row["total_collects"],
                "collects_change": row["total_collects"] - (prev["total_collects"] if prev else 0),
                "total_works": row["total_works"],
                "works_change": row["total_works"] - (prev["total_works"] if prev else 0)
            }
            data["daily_snapshots"].append(snapshot)
            prev = row

    # 按日期降序排列
    data["daily_snapshots"].sort(key=lambda x: (x["date"], x["platform"]), reverse=True)

    # 一次性获取各平台最新数据
    for platform in ["douyin", "xiaohongshu", "shipinhao", "gongzhonghao"]:
        cursor.execute("""
            SELECT * FROM daily_accounts
            WHERE platform = ?
            ORDER BY date DESC LIMIT 1
        """, (platform,))
        latest = cursor.fetchone()

        cursor.execute("""
            SELECT work_id, platform, title, publish_time, cover_url, url,
                   views, likes, comments, shares, collects
            FROM works WHERE platform = ?
            ORDER BY publish_time DESC LIMIT 50
        """, (platform,))
        works = [dict(row) for row in cursor.fetchall()]

        if latest:
            latest = dict(latest)
            data[platform] = {
                "account": {
                    "platform": platform,
                    "account_name": latest["account_name"],
                    "account_id": latest["account_id"],
                    "avatar_url": latest["avatar_url"],
                    "followers": latest["followers"],
                    "total_views": latest["total_views"],
                    "total_likes": latest["total_likes"],
                    "total_comments": latest["total_comments"],
                    "total_shares": latest["total_shares"],
                    "total_collects": latest["total_collects"],
                    "total_works": latest["total_works"],
                    "last_updated": latest.get("created_at", "")
                },
                "works": works
            }

    # 订单数据
    cursor.execute("""
        SELECT date, order_count, order_amount
        FROM daily_orders WHERE date >= ?
        ORDER BY date ASC
    """, (cutoff,))
    orders_rows = [dict(r) for r in cursor.fetchall()]
    data["orders"] = orders_rows

    conn.close()
    return data


def migrate_from_json(json_data):
    """从旧的 JSON 数据迁移到 SQLite"""
    init_db()
    conn = get_connection()
    cursor = conn.cursor()

    # 迁移 daily_snapshots
    for snapshot in json_data.get("daily_snapshots", []):
        cursor.execute("""
            INSERT OR IGNORE INTO daily_accounts
            (date, platform, followers, total_views, total_likes,
             total_comments, total_shares, total_collects, total_works)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot.get("date", ""),
            snapshot.get("platform", ""),
            snapshot.get("followers", 0),
            snapshot.get("total_views", 0),
            snapshot.get("total_likes", 0),
            snapshot.get("total_comments", 0),
            snapshot.get("total_shares", 0),
            snapshot.get("total_collects", 0),
            snapshot.get("total_works", 0)
        ))

    # 迁移各平台数据
    for platform in ["douyin", "xiaohongshu", "shipinhao", "gongzhonghao"]:
        platform_data = json_data.get(platform, {})

        # 更新账号信息到最新记录
        if platform_data.get("account"):
            account = platform_data["account"]
            cursor.execute("""
                UPDATE daily_accounts
                SET account_name = ?, account_id = ?, avatar_url = ?
                WHERE platform = ? AND date = (
                    SELECT MAX(date) FROM daily_accounts WHERE platform = ?
                )
            """, (
                account.get("account_name", ""),
                account.get("account_id", ""),
                account.get("avatar_url", ""),
                platform,
                platform
            ))

        # 迁移作品
        for work in platform_data.get("works", []):
            cursor.execute("""
                INSERT OR REPLACE INTO works
                (work_id, platform, title, publish_time, cover_url, url,
                 views, likes, comments, shares, collects)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                work.get("work_id", ""),
                platform,
                work.get("title", ""),
                work.get("publish_time", ""),
                work.get("cover_url", ""),
                work.get("url", ""),
                work.get("views", 0),
                work.get("likes", 0),
                work.get("comments", 0),
                work.get("shares", 0),
                work.get("collects", 0)
            ))

    conn.commit()
    conn.close()
    print(f"数据迁移完成: {DB_PATH}")


def cleanup_old_data(keep_days=90):
    """清理超过指定天数的旧数据"""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")

    cursor.execute("DELETE FROM daily_accounts WHERE date < ?", (cutoff,))
    deleted = cursor.rowcount

    conn.commit()
    conn.close()

    return deleted


def backup_db(keep_days=7):
    """备份数据库，保留最近 N 天的备份"""
    import shutil
    backup_dir = DB_PATH.parent / "backups"
    backup_dir.mkdir(exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    backup_path = backup_dir / f"tracker_{today}.db"

    if not backup_path.exists() and DB_PATH.exists():
        shutil.copy2(DB_PATH, backup_path)

    # 清理旧备份
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    for f in backup_dir.glob("tracker_*.db"):
        date_str = f.stem.replace("tracker_", "")
        if date_str < cutoff:
            f.unlink()


# 初始化数据库
init_db()
