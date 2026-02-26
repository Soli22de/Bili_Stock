# 混合信号挖掘系统 (Hybrid Signal Mining System)

## 系统架构概述

本系统通过整合雪球组合调仓信号与博主文本观点，构建了一个多模态的量化信号挖掘框架。系统采用"广撒网发现 + 强过滤清洗 + 信号共振验证"的三层架构。

```
数据源层 → 信号发现层 → 信号融合层 → 策略执行层
```

## 1. 组合池扩容与清洗 (Cube Expansion & Cleaning)

### 实现文件: `scripts/xueqiu/discover_and_clean.py`

### 广撒网发现策略
- **多维度榜单抓取**: 同时监控4个核心榜单
  - 最赚钱榜单 (category=10)
  - 热门榜单 (category=12) 
  - 本月最佳 (category=13)
  - 年化最高 (category=14)
- **关键词搜索**: 针对"量化"、"实盘"、"策略"等关键词定向搜索
- **批量处理**: 每榜单抓取10页，总计可达1000+组合

### 强过滤清洗标准
```python
# 筛选配置 (configurable)
criteria = {
    "min_total_gain": 20.0,      # 最小累计收益20%
    "max_drawdown": -30.0,       # 最大回撤不超过30%
    "min_followers": 1000,       # 最小关注人数1000
    "min_net_value": 1.2,        # 最小净值1.2
    "max_inactive_days": 30,      # 30天内必须有调仓
    "min_capital_suspected": 500000,  # 疑似实盘资金规模
}
```

### 僵尸组合识别算法
```python
def filter_zombie_cubes(df):
    current_time = int(time.time() * 1000)
    # 计算距离上次调仓的天数
    df["days_since_last_rebalance"] = (current_time - df["last_rebalancing_time"]) / (1000 * 60 * 60 * 24)
    # 过滤30天以上未调仓的组合
    return df[df["days_since_last_rebalance"] <= 30]
```

## 2. 博主言论的量化映射 (Opinion Quantization)

### 实现文件: `core/signal_fusion.py`

### 观点权重打分体系
| 特征 | 权重系数 | 说明 |
|------|---------|------|
| 实盘截图验证 (OCR Verified) | 1.5× | 晒单真实性最高 |
| 历史胜率 > 70% | 1.2× | 经过验证的选股能力 |
| 普通口嗨观点 | 0.8× | 无验证的文本观点 |
| 组合调仓信号 | 1.0× | 基准信号强度 |

### LLM观点提取伪代码
```python
def extract_opinions_with_llm(text, blogger_credibility):
    """使用LLM从文本中提取结构化观点"""
    prompt = f"""
    从以下文本中提取股票观点：
    {text}
    
    要求：
    1. 识别提到的股票代码和名称
    2. 判断情绪倾向：看多/看空/观望  
    3. 输出置信度(0-1)
    4. 提取目标价位(如果有)
    """
    
    # 调用LLM API (Gemini/OpenAI)
    response = llm_api(prompt)
    
    # 解析结构化结果
    opinions = parse_llm_response(response)
    
    # 应用博主权重
    for opinion in opinions:
        opinion['weight'] = apply_blogger_weight(opinion, blogger_credibility)
    
    return opinions
```

## 3. 信号共振策略 (Signal Resonance Strategy)

### 核心算法实现

#### 时间窗口匹配
```python
def find_resonance_in_time_window(cube_signal, opinions_df, window_hours=24):
    """在时间窗口内查找匹配观点"""
    signal_time = cube_signal['time']
    
    # 前后24小时时间窗口
    time_low = signal_time - timedelta(hours=window_hours)
    time_high = signal_time + timedelta(hours=window_hours)
    
    # 查找匹配观点
    matching_opinions = opinions_df[
        (opinions_df['time'] >= time_low) & 
        (opinions_df['time'] <= time_high) &
        (opinions_df['stock_code'] == cube_signal['stock_code']) &
        (opinions_df['sentiment'] == get_matching_sentiment(cube_signal['action']))
    ]
    
    return matching_opinions
```

#### 共振强度计算
```python
def calculate_resonance_strength(cube_signal, matching_opinions):
    """计算信号共振强度"""
    base_strength = 1.0  # 基础调仓信号强度
    
    # 观点权重总和
    opinion_strength = matching_opinions['weight'].sum()
    
    # 共振强度公式
    resonance = base_strength + opinion_strength
    
    # 强力买入阈值: 2.5
    if resonance >= 2.5:
        return {
            'strength': resonance,
            'signal_type': 'STRONG_BUY',
            'matching_count': len(matching_opinions)
        }
    else:
        return None
```

#### 背离检测算法
```python
def detect_divergence(cube_signal, opinions_df):
    """检测信号背离"""
    signal_sentiment = "看多" if cube_signal['action'] == "BUY" else "看空"
    
    # 查找相反观点
    opposite_opinions = opinions_df[
        (opinions_df['stock_code'] == cube_signal['stock_code']) &
        (opinions_df['sentiment'] != signal_sentiment) &
        (opinions_df['time'] within 24 hours of cube_signal['time'])
    ]
    
    if len(opposite_opinions) >= 2:  # 至少2个相反观点
        divergence_strength = opposite_opinions['weight'].sum()
        return {
            'strength': divergence_strength,
            'signal_type': 'DIVERGENCE',
            'opposite_count': len(opposite_opinions)
        }
    
    return None
```

### 信号类型定义

| 信号类型 | 触发条件 | 操作建议 |
|----------|---------|---------|
| **强力买入 (STRONG_BUY)** | 共振强度 ≥ 2.5 | 满仓操作，高置信度 |
| **普通调仓 (NORMAL)** | 仅有调仓信号 | 正常仓位，中等置信度 |
| **信号背离 (DIVERGENCE)** | ≥2个相反观点 | 降低仓位或观望 |
| **强力卖出 (STRONG_SELL)** | 共振卖出信号 | 清仓操作 |

## 4. 数据流与API接口

### 雪球核心API端点
```python
# 组合排行榜
RANK_API = "https://xueqiu.com/cubes/discover/rank/cube/list.json"

# 组合搜索  
SEARCH_API = "https://xueqiu.com/cubes/discover/search/cube/list.json"

# 调仓历史
REBALANCING_API = "https://xueqiu.com/cubes/rebalancing/history.json"

# 净值数据（计算回撤）
NAV_API = "https://xueqiu.com/cubes/nav_daily/all.json"
```

### 数据持久化结构
```json
// active_target_cubes.json
[
  {
    "symbol": "ZH123456",
    "name": "量化策略实盘",
    "total_gain": 150.5,
    "max_drawdown": -15.2,
    "last_rebalancing_time": 1771300000000,
    "follower_count": 5000,
    "estimated_capital": 500000,
    "discovery_source": "search_量化"
  }
]

// fused_signals.csv
timestamp,stock_code,stock_name,signal_type,strength,matching_count,bloggers,description
2024-01-15 10:30:00,SZ000858,五粮液,STRONG_BUY,3.2,2,量化老王,短线小李,优质组合调仓+2位博主观点共振
```

## 5. 风险控制与对策

### 数据滞后性风险
- **问题**: 雪球调仓数据发布延迟1-2小时
- **对策**: 实时监控+延迟执行，设置最小价格变动阈值

### 反爬虫风险  
- **问题**: 雪球严格的反爬机制
- **对策**: Cookie轮询池 + 请求频率控制 + 备用数据源

### 信号质量风险
- **问题**: 博主观点质量参差不齐
- **对策**: 权重体系 + 历史胜率验证 + OCR实盘验证

### 实盘执行风险
- **问题**: 信号到执行的滑点问题
- **对策**: 滑点控制算法 + 分批建仓 + 止损策略

## 6. 部署与监控

### 推荐运行频率
- **组合发现**: 每日一次 (09:00 AM)
- **信号监控**: 每30分钟一次 (09:30-15:00)  
- **共振分析**: 实时触发 (有调仓信号时)

### 监控指标
```python
monitoring_metrics = {
    "active_cubes_count": "活跃组合数量",
    "daily_signals_count": "每日信号数量", 
    "resonance_signals_ratio": "共振信号比例",
    "avg_resonance_strength": "平均共振强度",
    "divergence_detection_rate": "背离检测率"
}
```

## 7. 扩展性与优化方向

### 短期优化
- [ ] 集成更多LLM提供商 (Gemini, Claude, DeepSeek)
- [ ] 增加实时净值监控
- [ ] 优化反爬虫策略

### 中期规划  
- [ ] 加入基本面和情绪分析
- [ ] 开发自动跟单执行模块
- [ ] 构建信号回测框架

### 长期愿景
- [ ] 全自动多策略信号工厂
- [ ] AI驱动的信号权重优化
- [ ] 跨平台信号聚合

## 总结

本混合信号挖掘系统通过结合雪球组合的"行为信号"与博主观点的"文本信号"，构建了一个更加稳健和可解释的量化信号体系。系统采用动态发现、多重过滤、信号共振的三层架构，有效解决了单一数据源的信噪比问题，为实盘交易提供了高质量的决策支持。