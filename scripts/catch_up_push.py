#!/usr/bin/env python3
"""补推脚本：检查是否有未推送的 commit，有则自动推送（Python 版，避免 bash 权限问题）"""
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent

def run(cmd, **kwargs):
    return subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True, **kwargs)

def log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

def main():
    # fetch remote
    run(["git", "fetch", "origin"])

    # check ahead count
    r = run(["git", "rev-list", "--count", "@{u}..HEAD"])
    ahead = int(r.stdout.strip()) if r.stdout.strip().isdigit() else 0

    if ahead == 0:
        return

    log(f"发现 {ahead} 个未推送的 commit，开始补推...")

    # try direct push
    r = run(["git", "push"])
    if r.returncode == 0:
        log("补推成功")
        return

    log("直接 push 失败，尝试 rebase...")
    r = run(["git", "pull", "--rebase"])
    if r.returncode == 0:
        r2 = run(["git", "push"])
        if r2.returncode == 0:
            log("rebase + push 成功")
            return

    # abort rebase, try merge
    run(["git", "rebase", "--abort"])
    log("Rebase 冲突，回退到 merge 策略...")

    r = run(["git", "pull", "--no-rebase", "-X", "ours"])
    if r.returncode == 0:
        r2 = run(["git", "push"])
        if r2.returncode == 0:
            log("merge + push 成功")
            return

    log("补推失败，等待下次重试")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"未捕获异常: {e}")
    sys.exit(0)
