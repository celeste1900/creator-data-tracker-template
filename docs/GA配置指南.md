# Google Analytics 4 配置指南

本文档详细介绍如何配置 Google Analytics 4（GA4）数据采集，将网站流量数据集成到仪表盘中。

---

## 一、前置条件

- 已有 Google Analytics 4 账号和属性
- 已在网站中部署 GA4 跟踪代码（gtag.js 或 GTM）
- 拥有 Google Cloud 账号（用于创建服务账号）

---

## 二、获取 GA4 属性 ID

1. 登录 [Google Analytics](https://analytics.google.com/)
2. 左下角点击 **管理**（齿轮图标）
3. 在 **属性** 列中，点击 **属性设置 → 属性详情**
4. 复制 **属性 ID**（纯数字，如 `485215519`）

> 注意：需要的是纯数字的属性 ID，不是 `G-XXXXXXX` 格式的衡量 ID。

---

## 三、创建 Google Cloud 服务账号

### 3.1 创建项目（如果没有）

1. 打开 [Google Cloud Console](https://console.cloud.google.com/)
2. 点击顶部项目选择器 → **新建项目**
3. 输入项目名称（如 `creator-data-tracker`），点击 **创建**

### 3.2 启用 GA Data API

1. 在 Google Cloud Console 中，进入 **API 和服务 → 库**
2. 搜索 `Google Analytics Data API`
3. 点击 **Google Analytics Data API**（注意是 Data API，不是 Admin API）
4. 点击 **启用**

### 3.3 创建服务账号

1. 进入 **API 和服务 → 凭据**
2. 点击 **创建凭据 → 服务账号**
3. 填写：
   - 服务账号名称：`ga-data-reader`（自定义）
   - 服务账号 ID：会自动生成
4. 点击 **创建并继续**
5. 角色可以跳过（权限在 GA 中设置），点击 **继续 → 完成**

### 3.4 下载密钥文件

1. 在 **凭据** 页面，点击刚创建的服务账号
2. 切换到 **密钥** 标签
3. 点击 **添加密钥 → 创建新密钥**
4. 选择 **JSON** 格式，点击 **创建**
5. 浏览器会自动下载一个 JSON 文件
6. 将该文件重命名为 `ga_credentials.json`，放到项目的 `config/` 目录下：

```
config/ga_credentials.json
```

> **安全提醒**：此文件包含私钥，已在 `.gitignore` 中排除，不会被提交到 Git。

---

## 四、在 GA4 中授权服务账号

1. 登录 [Google Analytics](https://analytics.google.com/)
2. 左下角点击 **管理**
3. 在 **账号** 列中，点击 **账号访问权限管理**（或在 **属性** 列中点击 **属性访问权限管理**）
4. 点击右上角 **+** → **添加用户**
5. 输入服务账号的邮箱地址（在 JSON 密钥文件的 `client_email` 字段中，格式如：`ga-data-reader@your-project.iam.gserviceaccount.com`）
6. 角色选择 **查看者**（Viewer）—— 只需要读取权限
7. 点击 **添加**

> 授权后需要等待几分钟才能生效。

---

## 五、配置项目

### 5.1 编辑 config.json

在 `config.json` 中配置 GA 相关字段：

```json
{
  "ga": {
    "property_id": "485215519"
  },
  "google_analytics": {
    "enabled": true,
    "credentials_file": "config/ga_credentials.json"
  },
  "settings": {
    "ga_measurement_protocol": {
      "measurement_id": "G-XXXXXXXXXX",
      "api_secret": "你的API密钥"
    }
  }
}
```

| 字段 | 说明 | 示例 |
|------|------|------|
| `ga.property_id` | GA4 属性 ID（纯数字） | `485215519` |
| `google_analytics.credentials_file` | 服务账号密钥文件路径 | `config/ga_credentials.json` |
| `measurement_id` | GA4 衡量 ID（可选，用于 Measurement Protocol） | `G-ABC123XYZ` |
| `api_secret` | Measurement Protocol API 密钥（可选） | 在 GA4 管理后台生成 |

### 5.2 配置域名过滤

编辑 `scripts/collect_ga.py`，修改以下变量为你的实际域名：

```python
# 允许的域名（过滤掉 localhost、Vercel 预览等非生产环境数据）
ALLOWED_HOSTNAMES = ['yourdomain.com', 'app.yourdomain.com']

# 前台域名（用于区分前后台流量）
FRONTEND_HOSTNAMES = ['yourdomain.com']

# 后台域名
BACKEND_HOSTNAMES = ['app.yourdomain.com']
```

**为什么需要域名过滤？**
- GA4 会收集所有安装了跟踪代码的页面数据
- 包括 `localhost`、Vercel 预览部署（`xxx.vercel.app`）等
- 域名过滤确保仪表盘只显示正式环境的数据

### 5.3 代理配置（中国大陆用户）

脚本会自动检测网络环境：
1. 优先使用已设置的 `HTTPS_PROXY` 环境变量
2. 其次检测本地代理 `http://127.0.0.1:7890`（Clash 默认端口）
3. 最后尝试直连 Google

如果你的代理端口不同，修改 `scripts/collect_ga.py` 中的：

```python
DEFAULT_PROXY = "http://127.0.0.1:7890"  # 改为你的代理地址
```

或运行时指定：

```bash
HTTPS_PROXY=http://127.0.0.1:1080 python collect_all_with_ga.py
```

---

## 六、Measurement Protocol API 密钥（可选）

Measurement Protocol 用于从服务端发送事件到 GA4（如服务端注册事件追踪）。如果不需要服务端事件追踪，可以跳过此步骤。

### 获取 API 密钥

1. 登录 [Google Analytics](https://analytics.google.com/)
2. 左下角 **管理** → **数据流**
3. 点击你的网站数据流
4. 拉到底部，找到 **Measurement Protocol API secrets**
5. 点击 **创建**，输入名称（如 `data-tracker`）
6. 复制生成的密钥值

### 配置

将 `measurement_id` 和 `api_secret` 填入 `config.json`：

```json
"ga_measurement_protocol": {
  "measurement_id": "G-XXXXXXXXXX",
  "api_secret": "your_api_secret_here"
}
```

---

## 七、验证配置

### 7.1 单独测试 GA 采集

```bash
cd scripts
python collect_ga.py
```

成功输出示例：
```
[2026-03-17 10:00:00] === Google Analytics 数据采集 ===
  自动检测到代理: http://127.0.0.1:7890
[2026-03-17 10:00:01] 采集概览数据 (1天)...
[2026-03-17 10:00:02] 采集概览数据 (7天)...
[2026-03-17 10:00:03] 采集概览数据 (30天)...
...
[2026-03-17 10:00:10] GA 数据采集完成
```

### 7.2 完整采集（平台 + GA）

```bash
python collect_all_with_ga.py
```

### 7.3 常见错误

| 错误 | 原因 | 解决方案 |
|------|------|----------|
| `google.auth.exceptions.DefaultCredentialsError` | 找不到凭据文件 | 检查 `config/ga_credentials.json` 是否存在 |
| `PermissionDenied: 403` | 服务账号无权限 | 在 GA4 中添加服务账号为"查看者" |
| `Property not found` | 属性 ID 错误 | 检查 `property_id` 是否正确（纯数字） |
| `Connection refused` / 超时 | 网络问题 | 检查代理配置或网络连接 |
| 数据为空 | 域名过滤不匹配 | 检查 `ALLOWED_HOSTNAMES` 是否包含你的域名 |

---

## 八、采集的数据内容

配置完成后，GA 采集脚本会自动获取以下数据：

| 数据类别 | 包含指标 | 时间范围 |
|----------|----------|----------|
| **流量概览** | 活跃用户、会话数、页面浏览量、跳出率、平均会话时长 | 1天/7天/30天 |
| **流量来源** | 来源/媒介、会话数、用户数、跳出率 | 1天/7天/30天 |
| **热门页面** | 页面路径、浏览量、用户数 | 30天 |
| **地理分布** | 国家/地区、用户数、会话数 | 30天 |
| **设备分布** | 设备类别（桌面/移动/平板）、用户数 | 30天 |
| **每日趋势** | 每日活跃用户、新用户、会话数、页面浏览量 | 30天 |
| **注册事件** | sign_up_completed 事件次数、注册方式 | 7天/30天 |
| **落地页/退出页** | 页面路径、会话数 | 30天 |

数据存储在：
- `data/ga_data.json` — 前端仪表盘使用
- `data/tracker.db` — SQLite 数据库（历史数据）

---

## 九、配置检查清单

- [ ] Google Cloud 项目已创建
- [ ] Google Analytics Data API 已启用
- [ ] 服务账号已创建并下载 JSON 密钥
- [ ] 密钥文件放在 `config/ga_credentials.json`
- [ ] 在 GA4 中给服务账号添加了"查看者"权限
- [ ] `config.json` 中 `google_analytics.enabled` 设为 `true`
- [ ] `config.json` 中 `property_id` 填写正确
- [ ] `scripts/collect_ga.py` 中 `ALLOWED_HOSTNAMES` 已修改为实际域名
- [ ] 运行 `python scripts/collect_ga.py` 测试成功

---

*最后更新：2026-03-17*
