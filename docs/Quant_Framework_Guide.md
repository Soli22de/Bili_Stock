# 量化策略框架与提示词指南

## 🎯 核心框架结构

### 1. 项目目录规范
```
Bili_Stock/
├── core/                 # 核心策略模块
│   ├── signal_generation.py    # 信号生成
│   ├── risk_management.py      # 风控管理
│   ├── portfolio_management.py  # 组合管理
│   └── backtest_engine.py      # 回测引擎
├── data/                 # 数据层
│   ├── raw/                   # 原始数据
│   ├── processed/             # 处理后数据
│   └── signals/               # 信号数据
├── models/               # 模型层
│   ├── technical_models/      # 技术指标模型
│   ├── sentiment_models/      # 情绪模型
│   └── ml_models/             # 机器学习模型
├── utils/                # 工具函数
│   ├── data_loader.py        # 数据加载
│   ├── logger.py             # 日志管理
│   └── notification.py       # 通知服务
└── config/               # 配置文件
    ├── trading_config.py     # 交易配置
    ├── risk_config.py        # 风控配置
    └── model_config.py       # 模型配置
```

### 2. 核心类设计规范

#### Signal 信号类
```python
class TradingSignal:
    def __init__(self, symbol, signal_type, confidence, timestamp, **kwargs):
        self.symbol = symbol           # 股票代码
        self.signal_type = signal_type  # BUY/SELL/HOLD
        self.confidence = confidence    # 置信度 0-1
        self.timestamp = timestamp      # 时间戳
        self.metadata = kwargs         # 元数据
        
    def to_dict(self):
        return {
            'symbol': self.symbol,
            'signal_type': self.signal_type,
            'confidence': self.confidence,
            'timestamp': self.timestamp,
            **self.metadata
        }
```

#### Portfolio 组合类
```python
class Portfolio:
    def __init__(self, initial_capital):
        self.cash = initial_capital
        self.positions = {}  # {symbol: shares}
        self.trade_history = []
    
    def execute_trade(self, signal, price, shares):
        # 执行交易逻辑
        pass
```

## 🤖 多模型协作提示词模板

### 1. DeepSeek V3.1 (量化策略师)

**技术指标开发**
```
作为A股量化专家，请帮我开发一个基于MACD+RSI+成交量加权的复合技术指标。

要求：
1. 使用Python实现，基于pandas计算
2. 包含信号生成函数，返回买入/卖出信号和置信度
3. 添加详细的参数说明和数学公式解释
4. 包含回测验证逻辑

请提供完整的类实现和单元测试。
```

**风控算法设计**
```
设计一个动态止损算法，要求：
1. 基于ATR和价格波动率动态调整止损位
2. 考虑大盘环境和个股流动性
3. 提供数学推导和参数优化方法
4. 实现为可配置的Python类
```

### 2. GPT-5.2 Codex (全栈工程师)

**工程落地任务清单**
```
请基于现有 Bili_Stock 仓库完成工程落地与整理。

背景：
- 代码已包含 core/、scripts/、data/、docs/、config.py
- 现有核心模块：monitor_and_notify.py、intraday_validator.py、realtime_baostock.py、backtest_engine.py

要求：
1. 保持现有功能可用，不新增无必要的目录
2. 清晰划分模块职责并优化导入结构
3. 所有改动以最小侵入方式完成
4. 输出改动清单与关键代码片段
```

**实时监控链路完善**
```
请完善实时监控与通知链路。

目标：
1. 在 core/monitor_and_notify.py 中补齐收盘汇总发送逻辑
2. 汇总数据来源 data/trading_signals.csv，按日期聚合
3. 使用 config.py 中的 CLOSE_SUMMARY_TIME 配置触发
4. 保障去重机制不被破坏

输出：
- 修改文件列表
- 关键函数改动说明
```

**回测与数据治理**
```
请在 core/backtest_engine.py 基础上完善回测流程。

目标：
1. 统一读取 data/backtest_report.csv 与 data/trading_signals.csv
2. 输出标准绩效指标到 data/backtest_report.csv
3. 与现有信号字段保持兼容
4. 不能改变历史数据的字段语义

输出：
- 完整改动方案
- 关键函数接口说明
```

### 3. Gemini 3 Pro (数据吞噬者)

**大数据分析**
```
请分析以下A股历史数据（包含1000只股票3年的日线数据）：
[提供数据链接或描述]

分析要求：
1. 识别市场风格轮动规律
2. 发现有效的因子组合
3. 提供可视化分析结果
4. 给出策略开发建议
```

**文档理解**
```
请阅读并总结以下量化交易论文的核心思想：
[提供论文链接或文本]

总结要求：
1. 提取关键算法和数学模型
2. 评估策略的实盘适用性
3. 指出可能的改进方向
```

### 4. Kimi k2-0905 (舆情分析员)

**舆情分析**
```
请分析以下股票社区的热门讨论：
[提供讨论文本或链接]

分析要求：
1. 提取核心观点和情绪倾向
2. 识别被频繁提及的股票
3. 评估讨论质量和技术含量
4. 生成简洁的舆情报告
```

**内容摘要**
```
请对以下财经新闻进行摘要：
[提供新闻文本]

摘要要求：
1. 提取关键信息和市场影响
2. 识别潜在的投资机会
3. 评估信息的可靠性和时效性
4. 限制在200字以内
```

## 🎯 策略开发工作流

### 1. 策略设计阶段
```prompt
请设计一个基于均值回归的短线交易策略：

输入要求：
- 标的：A股主板股票
- 频率：日内5分钟级别
- 持仓：不超过2小时
- 风控：单日最大亏损2%

输出要求：
1. 完整的策略逻辑描述
2. 数学公式和参数设置
3. 预期的夏普比率和最大回撤
4. 可能的失效场景和应对措施
```

### 2. 代码实现阶段
```prompt
请实现上述均值回归策略的Python代码：

要求：
1. 使用面向对象设计
2. 包含数据预处理、信号生成、回测验证
3. 添加详细的注释和文档
4. 包含性能评估指标计算
```

### 3. 回测验证阶段
```prompt
请对以下策略进行历史回测：
[提供策略代码]

回测要求：
1. 测试周期：2020-2024年
2. 考虑交易成本和滑点
3. 提供详细的绩效报告
4. 进行参数敏感度分析
```

### 4. 实盘部署阶段
```prompt
请将以下策略部署到实盘环境：
[提供策略代码]

部署要求：
1. 设计监控和报警机制
2. 确保系统稳定性和容错性
3. 添加实时性能监控
4. 准备应急处理方案
```

## 📊 绩效评估标准

### 核心绩效指标
```python
PERFORMANCE_METRICS = {
    'annual_return': '年化收益率',
    'sharpe_ratio': '夏普比率', 
    'max_drawdown': '最大回撤',
    'win_rate': '胜率',
    'profit_factor': '盈亏比',
    'calmar_ratio': '卡玛比率',
    'sortino_ratio': '索提诺比率'
}
```

### 风险评估指标
```python
RISK_METRICS = {
    'var_95': '95%置信度VaR',
    'cvar_95': '95%置信度CVaR',
    'volatility': '波动率',
    'beta': '贝塔系数',
    'alpha': '阿尔法收益'
}
```

## 🔧 工具函数模板

### 数据加载函数
```python
def load_stock_data(symbol, start_date, end_date, frequency='1d'):
    """
    加载股票数据
    
    Args:
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        frequency: 数据频率
    
    Returns:
        DataFrame with OHLCV数据
    """
    # 实现数据加载逻辑
    pass
```

### 信号生成函数
```python
def generate_signals(data, config):
    """
    生成交易信号
    
    Args:
        data: 输入数据
        config: 策略配置
    
    Returns:
        List of TradingSignal objects
    """
    # 实现信号生成逻辑
    pass
```

## 🚀 快速开始示例

### 示例策略：双均线策略
```python
# 提示词：
"请实现一个简单的双均线策略，使用5日和20日移动平均线"

# 预期输出：完整的策略类实现，包含回测和绩效评估
```

### 示例风控：动态止损
```python
# 提示词：
"请设计一个基于ATR的动态止损算法"

# 预期输出：风控类实现，包含参数优化方法
```

---

## 📝 使用说明

1. **复制粘贴**: 直接复制相应的提示词模板
2. **定制修改**: 根据具体需求调整参数和要求
3. **多模型协作**: 按框架分工使用不同的AI模型
4. **迭代优化**: 基于回测结果不断改进策略

这个框架指南将帮助你系统性地开发和管理量化策略，确保代码质量和策略效果。
