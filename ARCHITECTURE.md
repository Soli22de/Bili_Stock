# 系统架构设计

## 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│              Bili_Stock 量化交易系统 (Smart Money)           │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │  数据采集层  │  │  策略研究层  │  │     执行风控层      │ │
│  │  Data Layer │  │  Research   │  │  Execution Layer    │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 核心策略：雪球精英组合共振

### 数据流
```
雪球组合采集 → 精英筛选(1,400+) → 调仓信号提取 → 共振检测(3日滚动) → 市场状态适配 → 风控过滤 → 交易信号
```

### 关键逻辑
- 采集 55,000+ 条雪球组合调仓记录
- 筛选精英组合: 按收益率 + 关注者数量
- 共振信号: 3日内 2+ 精英组合买入同一标的 → 候选
- 市场状态适配: 牛市反转动量 / 震荡及熊市正向动量
- 回测 2010-2025: 胜率 57.6%, Calmar 0.359

## 核心模块 (core/)

### 数据层
| 模块 | 职责 |
|------|------|
| `data_provider.py` | 多源数据获取 (BaoStock / AkShare / TuShare)，自动降级 |
| `data_cache.py` | SQLite 本地缓存，减少 API 调用 |
| `storage.py` | SQLAlchemy ORM，组合/调仓数据持久化 |
| `realtime_baostock.py` | BaoStock 实时价格获取 |
| `realtime_market.py` | 多源实时行情聚合 (新浪/腾讯/东财)，加权共识 |
| `net_env.py` | 代理环境控制 |

### 信号层
| 模块 | 职责 |
|------|------|
| `extract_signals.py` | 从文本/视频标题提取交易信号，关键词匹配 + 股票映射 |
| `signal_fusion.py` | 多源信号融合 (组合调仓 + 博主观点)，可配置权重 |
| `factor_miner.py` | 因子挖掘: SmartResonanceFactor, PanicReversalFactor |
| `bayesian_scorer.py` | 贝叶斯创作者可信度评分 |
| `strategies.py` | 策略抽象基类 + DragonStrategy (小资金激进打板) |

### 回测层
| 模块 | 职责 |
|------|------|
| `backtest_engine.py` | A 股回测引擎 (处理 T+1、涨跌停、停牌) |
| `backtest_analyzer.py` | 绩效分析: 夏普比率、最大回撤、Calmar 等 |

### 执行层
| 模块 | 职责 |
|------|------|
| `risk_engine.py` | 仓位管理、日亏损限制、流动性/波动率检查 |
| `intraday_validator.py` | 盘中信号验证 (RSI、MA 等技术指标) |
| `intraday_trader.py` | 集合竞价过滤、盘中择时 |
| `generate_trading_plan.py` | 每日交易计划生成 |
| `monitor_and_notify.py` | 监控主循环: 信号提取 → 验证 → 推送 → 模拟持仓 |
| `notifier.py` | 钉钉 Webhook 推送 |

### 入口
| 模块 | 职责 |
|------|------|
| `run_pipeline_today.py` | 日间流水线: 提取 → 计划 → 推送 |
| `run_realtime_monitor.py` | 实时监控循环 (盘中运行) |

## 研究基线 (research/)

```
research/
├── baseline_v4/       # 市场状态分类 (牛/震荡/熊), Calmar 0.173
├── baseline_v4_2/     # 牛市选股修正, Calmar 0.181
├── baseline_v5/       # 全样本 2019-2025 + 实时约束, Calmar 0.359
└── baseline_v6_1/     # 风控模块 + E3 微调 + 灰度部署 (当前生产)
```

每个版本遵循: `code/ → output/ → report/` 结构，锁定后不再修改。

## 目录结构

```
Bili_Stock/
├── core/                    # 核心模块 (上述所有)
├── research/                # 版本化研究基线
├── scripts/
│   ├── xueqiu/              # 雪球数据采集 & 策略回测
│   └── analysis/            # 分析脚本
├── archive/
│   └── bilibili_legacy/     # 已归档: B站视频信号提取 (OCR/ASR)
├── data/                    # 数据库 & 缓存 (gitignored)
├── docs/                    # 设计文档
└── logs/                    # 日志 (gitignored)
```

## 技术栈

- **Python 3.12** — 主语言
- **Pandas / NumPy** — 数据处理
- **BaoStock / AkShare / TuShare** — A 股数据源
- **SQLite / SQLAlchemy** — 存储
- **Scikit-learn / Statsmodels** — 统计分析
- **aiohttp** — 异步网络
- **钉钉 Webhook** — 消息推送

## 每日运行流程

1. **09:00** — 数据更新，信号提取
2. **09:15** — 生成策略计划，发送钉钉
3. **09:25** — 集合竞价过滤
4. **09:30-15:00** — 盘中实时监控
5. **15:05** — 收盘汇总，盈亏统计

---

*最后更新: 2026-04-10*
