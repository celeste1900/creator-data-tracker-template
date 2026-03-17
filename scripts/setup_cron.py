#!/usr/bin/env python3
"""
定时任务设置脚本

在 Mac 上设置 launchd 定时任务，每天自动运行数据采集
"""
import os
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).parent
PLIST_NAME = "com.creator-data-tracker.collect"
PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / f"{PLIST_NAME}.plist"


def get_plist_content(hour: int = 9, minute: int = 0) -> str:
    """生成 launchd plist 配置"""
    python_path = sys.executable
    script_path = PROJECT_DIR.parent / "collect_all_with_ga.py"
    log_path = PROJECT_DIR / "logs"

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{PLIST_NAME}</string>

    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>{script_path}</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{PROJECT_DIR}</string>

    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>{hour}</integer>
        <key>Minute</key>
        <integer>{minute}</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>{log_path}/collect.log</string>

    <key>StandardErrorPath</key>
    <string>{log_path}/collect.error.log</string>

    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
"""


def install():
    """安装定时任务"""
    print("设置定时任务")
    print("=" * 40)

    # 获取执行时间
    print("\n每天什么时候执行数据采集？")
    hour_input = input("小时 (0-23, 默认 9): ").strip()
    minute_input = input("分钟 (0-59, 默认 0): ").strip()

    hour = int(hour_input) if hour_input else 9
    minute = int(minute_input) if minute_input else 0

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        print("时间格式错误")
        return

    # 创建日志目录
    log_dir = PROJECT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)

    # 创建 plist 文件
    PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    plist_content = get_plist_content(hour, minute)

    with open(PLIST_PATH, "w") as f:
        f.write(plist_content)

    print(f"\n已创建配置文件: {PLIST_PATH}")

    # 加载定时任务
    os.system(f"launchctl unload {PLIST_PATH} 2>/dev/null")
    result = os.system(f"launchctl load {PLIST_PATH}")

    if result == 0:
        print(f"\n定时任务已启动!")
        print(f"每天 {hour:02d}:{minute:02d} 将自动执行数据采集")
        print(f"\n日志文件: {log_dir}/collect.log")
    else:
        print("\n启动失败，请检查配置")


def uninstall():
    """卸载定时任务"""
    if PLIST_PATH.exists():
        os.system(f"launchctl unload {PLIST_PATH}")
        PLIST_PATH.unlink()
        print("定时任务已移除")
    else:
        print("定时任务不存在")


def status():
    """查看定时任务状态"""
    result = os.popen(f"launchctl list | grep {PLIST_NAME}").read()
    if result:
        print("定时任务状态: 运行中")
        print(result)
    else:
        print("定时任务状态: 未运行")


def run_now():
    """立即执行一次"""
    print("立即执行数据采集...")
    os.system(f"cd {PROJECT_DIR.parent} && python3 collect_all_with_ga.py")


def main():
    print("定时任务管理")
    print("=" * 40)
    print("\n1. 安装定时任务")
    print("2. 卸载定时任务")
    print("3. 查看状态")
    print("4. 立即执行一次")
    print("0. 退出")

    choice = input("\n请选择 (0-4): ").strip()

    if choice == "1":
        install()
    elif choice == "2":
        uninstall()
    elif choice == "3":
        status()
    elif choice == "4":
        run_now()
    elif choice == "0":
        pass
    else:
        print("无效选项")


if __name__ == "__main__":
    main()
