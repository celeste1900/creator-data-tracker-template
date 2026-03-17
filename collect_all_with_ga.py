#!/usr/bin/env python3
"""
运行所有数据采集任务：平台数据 + Google Analytics + 视频号（独立）
"""
import os
import signal
import subprocess
import sys
import json
import time
from pathlib import Path
from datetime import datetime

ROOT_DIR = Path(__file__).parent
GA_DATA_FILE = ROOT_DIR / "data" / "ga_data.json"
ALL_DATA_FILE = ROOT_DIR / "data" / "all_data.json"


def log(message: str):
    """带时间戳的日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def get_ga_data_timestamp() -> str | None:
    """获取 GA 数据文件的更新时间"""
    if GA_DATA_FILE.exists():
        try:
            with open(GA_DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("updated_at")
        except Exception:
            pass
    return None


def get_platform_timestamps() -> dict:
    """获取各平台数据的 last_updated 时间戳"""
    result = {}
    if ALL_DATA_FILE.exists():
        try:
            with open(ALL_DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for p in ["douyin", "xiaohongshu", "shipinhao"]:
                ts = data.get(p, {}).get("account", {}).get("last_updated")
                if ts:
                    result[p] = ts
        except Exception:
            pass
    return result


def check_platform_freshness(old_ts: dict, new_ts: dict):
    """对比采集前后各平台的时间戳，报告哪些没更新"""
    today = datetime.now().strftime("%Y-%m-%d")
    stale = []
    for p in ["douyin", "xiaohongshu"]:
        ts = new_ts.get(p, "")
        if not ts.startswith(today):
            stale.append(f"{p}({ts or '无数据'})")
        elif old_ts.get(p) == new_ts.get(p):
            stale.append(f"{p}(未变化)")
    if stale:
        log(f"⚠️  数据未刷新: {', '.join(stale)}")
        notify("平台数据过期", f"以下平台数据未更新: {', '.join(stale)}")
    else:
        log("✅ 平台数据均已刷新")


def run_command(cmd: list, description: str, cwd=None, env=None, timeout=300) -> bool:
    """运行命令并返回是否成功。使用进程组确保超时时能杀掉所有子进程。"""
    log(f"执行: {description}")
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd or ROOT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=True,  # 新建进程组，超时时可整组杀掉
        )
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            # 杀掉整个进程组
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait(timeout=5)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
            log(f"❌ {description} 超时 ({timeout}s)")
            return False

        # 记录子进程输出到日志，方便排查
        if stdout and stdout.strip():
            for line in stdout.strip().splitlines()[-20:]:  # 最后 20 行
                log(f"   | {line}")

        if proc.returncode != 0:
            log(f"❌ {description} 失败 (返回码: {proc.returncode})")
            if stderr:
                log(f"   错误: {stderr[:500]}")
            return False
        log(f"✅ {description} 成功")
        return True
    except Exception as e:
        log(f"❌ {description} 异常: {e}")
        return False


def check_data_integrity() -> bool:
    """检查 all_data.json 是否异常缩小，防止数据丢失"""
    all_data_file = ROOT_DIR / "data" / "all_data.json"
    new_size = all_data_file.stat().st_size if all_data_file.exists() else 0
    try:
        result = subprocess.run(
            ["git", "show", "HEAD:data/all_data.json"],
            cwd=ROOT_DIR, capture_output=True, timeout=5
        )
        old_size = len(result.stdout)
        if old_size > 5000 and new_size < old_size * 0.5:
            log(f"⛔ 数据完整性校验失败：all_data.json 从 {old_size} 字节缩小到 {new_size} 字节，疑似数据丢失")
            return False
    except Exception:
        pass
    return True


def notify(title, message):
    """发送 macOS 通知"""
    try:
        subprocess.run([
            "osascript", "-e",
            f'display notification "{message}" with title "{title}"'
        ], capture_output=True, timeout=5)
    except Exception:
        pass


def git_push_safe(commit_msg):
    """安全推送：add → commit → stash → rebase → push(重试) → stash pop"""
    # 添加所有数据文件（包括 ga_data.json、all_data.json、数据库等）
    add_ok = run_command(["git", "add", "data/"], "Git add data/")
    if not add_ok:
        return False

    # 检查是否有变更
    diff_result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"], cwd=ROOT_DIR
    )
    if diff_result.returncode == 0:
        log("没有新的变更需要提交")
        return True

    if not run_command(["git", "commit", "-m", commit_msg], "Git commit"):
        return False

    # stash 未跟踪修改，防止脏工作区阻塞 rebase
    stash_result = subprocess.run(
        ["git", "stash", "--include-untracked"],
        cwd=ROOT_DIR, capture_output=True, text=True
    )
    stashed = "No local changes" not in stash_result.stdout

    rebase_ok = run_command(["git", "pull", "--rebase"], "Git pull --rebase")
    if not rebase_ok:
        log("Rebase 冲突，中止 rebase，尝试 merge 策略...")
        run_command(["git", "rebase", "--abort"], "Git rebase --abort")
        # 回退到 merge，数据文件冲突时保留本地版本（刚采集的最新数据）
        merge_ok = run_command(
            ["git", "pull", "--no-rebase", "-X", "ours"],
            "Git pull --merge (ours strategy)"
        )
        if not merge_ok:
            log("Merge 也失败，跳过推送，等待补推")
            if stashed:
                subprocess.run(["git", "stash", "pop"], cwd=ROOT_DIR, capture_output=True)
            return False

    # 推送，失败后指数退避重试 3 次
    push_ok = False
    for attempt, delay in enumerate([0, 15, 30, 60], 1):
        if delay:
            log(f"推送失败，{delay} 秒后第 {attempt} 次重试...")
            time.sleep(delay)
        push_ok = run_command(["git", "push"], f"Git push (第 {attempt} 次)")
        if push_ok:
            break

    if stashed:
        subprocess.run(["git", "stash", "pop"], cwd=ROOT_DIR, capture_output=True)

    return push_ok


def main():
    python = sys.executable
    log("=" * 50)
    log("开始数据采集任务")
    log("=" * 50)

    env = {**dict(os.environ), "AUTO_MODE": "1"}

    # ── 步骤 1/4：小红书 + 抖音 ──
    log("")
    log("【步骤 1/4】采集小红书 + 抖音...")
    old_platform_ts = get_platform_timestamps()
    platform_success = run_command(
        [python, str(ROOT_DIR / "collect_all.py")],
        "平台数据采集（小红书+抖音）",
        env=env
    )
    if platform_success:
        new_platform_ts = get_platform_timestamps()
        check_platform_freshness(old_platform_ts, new_platform_ts)

    # ── 步骤 2/4：Google Analytics ──
    log("")
    log("【步骤 2/4】采集 Google Analytics 数据...")
    old_timestamp = get_ga_data_timestamp()
    ga_success = run_command(
        [python, str(ROOT_DIR / "scripts" / "collect_ga.py")],
        "GA 数据采集",
        timeout=600  # 10 分钟超时（30+ 次 API 调用）
    )
    new_timestamp = get_ga_data_timestamp()
    ga_data_updated = (
        ga_success and
        new_timestamp is not None and
        new_timestamp != old_timestamp
    )
    if ga_success and not ga_data_updated:
        log("⚠️  GA 采集脚本执行完成，但数据未更新")

    # ── 步骤 3/4：推送小红书 + 抖音 + GA ──
    log("")
    log("【步骤 3/4】推送数据到 GitHub...")
    push_success = False
    if platform_success or ga_data_updated:
        if not check_data_integrity():
            log("⛔ 推送中止：数据完整性校验未通过")
        else:
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
            push_success = git_push_safe(f"Auto update: {now_str}")
    else:
        log("⚠️  平台数据和 GA 数据均未更新，跳过推送")

    # ── 步骤 4/4：视频号（独立采集，不阻塞前面的流程）──
    log("")
    log("【步骤 4/4】采集视频号（独立）...")
    notify("视频号采集", "即将弹出扫码窗口，请准备扫码")

    shipinhao_success = run_command(
        [python, str(ROOT_DIR / "collect_all.py"), "--platform", "shipinhao"],
        "视频号采集",
        timeout=180  # 3 分钟超时（含扫码等待）
    )

    if shipinhao_success:
        log("视频号采集成功，推送更新...")
        git_push_safe(
            f"Auto update shipinhao ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
        )

    # ── 输出摘要 ──
    log("")
    log("=" * 50)
    log("采集任务完成")
    log("=" * 50)
    log(f"  小红书+抖音: {'✅ 成功' if platform_success else '❌ 失败'}")
    log(f"  GA 数据:     {'✅ 成功' if ga_data_updated else '❌ 失败'}")
    log(f"  视频号:      {'✅ 成功' if shipinhao_success else '⏭️ 未扫码/超时'}")
    if ga_data_updated:
        log(f"  GA 更新时间: {new_timestamp}")

    # 发送通知
    parts = []
    if platform_success:
        parts.append("小红书+抖音 ✅")
    if ga_data_updated:
        parts.append("GA ✅")
    if shipinhao_success:
        parts.append("视频号 ✅")

    failures = []
    if not platform_success:
        failures.append("平台数据")
    if not ga_data_updated:
        failures.append("GA")

    if failures:
        notify("数据采集异常", f"成功: {', '.join(parts) or '无'} | 失败: {', '.join(failures)}")
    elif not shipinhao_success:
        notify("数据采集完成", f"{', '.join(parts)} | 视频号未扫码")
    else:
        notify("数据采集完成", ', '.join(parts))

    # 小红书+抖音+GA 失败才返回错误码，视频号不影响
    if not platform_success or not ga_data_updated:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        # 防止非零退出码导致 launchd 永久停止调度 (exit 78 = EX_CONFIG)
        log(f"脚本退出码: {e.code}")
    except Exception as e:
        log(f"未捕获异常: {e}")
    # 始终以 0 退出，保证 launchd 继续调度
    sys.exit(0)
