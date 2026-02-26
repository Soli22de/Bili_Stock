# Smart Momentum Quant (智能动量量化系统)

[![License](https://img.shields.io/badge/license-Private-red.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![Database](https://img.shields.io/badge/database-Supabase%20%7C%20TiDB-green.svg)](https://supabase.com/)

**Smart Momentum Quant** 是一个基于“聪明钱”跟踪与量价异动捕捉的量化交易系统。它通过监控雪球等平台上的数千个精英组合，结合严苛的量价共振模型，旨在发现机构潜伏后的右侧爆发机会。

## 🌟 核心策略 (Core Strategy)

### 1. 聪明钱选股 (Smart Money Selection)
- **精英组合池**：监控 1400+ 个经过筛选的精英组合（关注 > 40人，收益 > 0%）。
- **共识机制**：利用 `SmartSignalLoader` 计算多组合对同一只股票的买入共识。
- **Watchlist**：触发买入信号的股票不直接买入，而是加入“待观察名单”。

### 2. 量价异动择时 (Volume-Price Breakout)
- **量能爆发**：`Vol > 1.5 * MA5_Vol`
- **价格突破**：`Close > Open` 且 `PctChg > 3%`
- **趋势确认**：`Close > MA10`
- **严控风险**：剔除 ST、退市股、科创/创业板（可选）。

## 🛠️ 协作开发指南 (Collaboration Guide)

### 1. 环境准备
```bash
# 1. 克隆项目
git clone https://github.com/YourUsername/Smart-Momentum-Quant.git
cd Smart-Momentum-Quant

# 2. 安装依赖
pip install -r requirements.txt
pip install psycopg2-binary  # 数据库驱动
```

### 2. 数据库配置 (关键!)
本项目使用 **Supabase (PostgreSQL)** 作为中心化数据库，实现多人数据共享。

1.  复制配置模板：
    ```bash
    cp config.example.py config.py
    ```
2.  修改 `config.py`：
    *   **DB_URL**: 填入团队共享的 Supabase 连接串（请向项目负责人索取）。
    *   **DEEPSEEK_API_KEY**: 填入您的 DeepSeek Key（用于 AI 分析）。

### 3. 数据同步
如果是第一次运行，或者需要更新本地数据到云端：
```bash
python scripts/migrate_sqlite_to_mysql.py
```
*(注：平时开发直接读取云端数据，无需手动同步)*

## 📂 项目结构

```
Smart-Momentum-Quant/
├── core/                   # 核心库
│   ├── storage.py          # 数据库 ORM 模型 (SQLAlchemy)
│   └── ...
├── scripts/
│   ├── xueqiu/             # 雪球策略核心代码
│   │   ├── strategy_smart_momentum.py  # ⭐ 主策略回测引擎
│   │   ├── fetch_cube_history.py       # 爬虫脚本
│   │   └── ...
│   └── migrate_sqlite_to_mysql.py      # 数据迁移工具
├── data/                   # 本地数据缓存 (不提交到 Git)
├── config.py               # 配置文件 (不提交到 Git)
└── archive/                # 归档的旧代码 (B站策略等)
```

## 🚀 常用命令

**运行主策略回测：**
```bash
python scripts/xueqiu/strategy_smart_momentum.py
```

**抓取最新数据：**
```bash
python scripts/xueqiu/fetch_cube_history.py data/elite_5000_candidates.json
```

## 🤝 贡献流程 (Workflow)
1.  **Pull** 最新代码：`git pull origin main`
2.  **Checkout** 新分支：`git checkout -b feature/new-idea`
3.  **Commit** 修改：`git commit -m "feat: add new indicator"`
4.  **Push** 分支：`git push origin feature/new-idea`
5.  **Pull Request**：在 GitHub 上发起合并请求。

---
**注意**：`config.py` 和 `data/` 目录下的文件包含敏感信息，已配置 `.gitignore`，请勿强制提交！
