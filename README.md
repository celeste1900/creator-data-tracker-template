# Creator Data Tracker

一个自动化的全平台运营数据仪表盘。每天自动采集数据，部署到 Vercel，随时查看。

## 仪表盘预览

用浏览器打开 `index.html`，输入访问码 `888888` 即可查看虚拟数据示例（StarBrew咖啡虚拟品牌）。

## 你提供什么 → 你得到什么

| 你提供 | 你得到 | 是否必选 |
|--------|--------|----------|
| 抖音/小红书创作者后台 Cookie | 粉丝、播放、点赞、评论、分享、收藏等数据的每日自动采集和趋势分析 | 至少选一个平台 |
| 视频号微信扫码（每天一次） | 视频号粉丝、播放、互动数据采集 | 可选 |
| Google Analytics 服务账号 | 网站流量、来源、用户行为等 GA4 数据面板 | 可选 |
| 后台订单/注册 API 对接 | 订单金额、注册用户、转化率趋势图表 | 可选 |

**最终产出**：一个 Vercel 在线仪表盘，每天自动更新，团队随时查看。

## 快速开始

### 1. 克隆并安装

```bash
git clone https://github.com/你的用户名/creator-data-tracker.git
cd creator-data-tracker
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 2. 配置

```bash
cp config.example.json config.json
# 编辑 config.json，填入你的平台 Cookie
```

### 3. 运行一次看效果

```bash
python collect_all.py
open index.html
```

### 4. 设置每日定时采集

```bash
python scripts/setup_cron.py
# 选择每天几点采集（视频号需要你在电脑前扫码）
```

### 5. 部署到 Vercel

1. 将仓库推送到你的 GitHub
2. 在 [Vercel](https://vercel.com) 导入仓库，Framework 选 `Other`
3. 之后每次采集完自动推送，仪表盘自动更新

## 各模块配置

按需选择，详见 docs 目录：

| 模块 | 配置文档 | 备注 |
|------|----------|------|
| 抖音/小红书/视频号 | [平台配置](docs/配置文档.md) | Cookie 获取、视频号扫码流程、定时任务 |
| Google Analytics | [GA 配置指南](docs/GA配置指南.md) | 服务账号创建、域名过滤、代理设置 |
| 后台业务数据 | [后台数据对接](docs/后台数据对接.md) | 订单、注册、收入数据推送方式 |
| 完整操作手册 | [操作手册](docs/操作手册.md) | 环境配置、故障排查 |

## 注意事项

- **Cookie 有效期**：抖音/小红书约 14 天更新一次
- **视频号特殊**：Cookie 仅几小时有效，每天采集都需要微信扫码（定时弹窗提醒，2 分钟内扫码）
- **数据安全**：`config.json` 和凭证文件已加入 `.gitignore`，不会提交到 Git

## License

MIT
