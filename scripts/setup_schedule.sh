#!/bin/bash
# 安装定时任务脚本

PLIST_NAME="com.creator.datacollector.plist"
PLIST_SRC="$(dirname "$0")/$PLIST_NAME"
PLIST_DST="$HOME/Library/LaunchAgents/$PLIST_NAME"

echo "=== 创作者数据采集 - 定时任务安装 ==="

# 检查 plist 文件是否存在
if [ ! -f "$PLIST_SRC" ]; then
    echo "错误: 找不到 $PLIST_SRC"
    exit 1
fi

# 如果已存在，先卸载
if [ -f "$PLIST_DST" ]; then
    echo "卸载现有定时任务..."
    launchctl unload "$PLIST_DST" 2>/dev/null
    rm "$PLIST_DST"
fi

# 复制 plist 文件
echo "安装定时任务..."
cp "$PLIST_SRC" "$PLIST_DST"

# 加载定时任务
launchctl load "$PLIST_DST"

echo ""
echo "✅ 定时任务已安装！"
echo ""
echo "运行时间: 每天 9:00 和 21:00"
echo "日志文件: $(dirname "$0")/data/launchd.log"
echo ""
echo "常用命令:"
echo "  手动运行: python $(dirname "$0")/collect_all.py"
echo "  查看状态: launchctl list | grep creator"
echo "  卸载任务: launchctl unload ~/Library/LaunchAgents/$PLIST_NAME"
echo ""
