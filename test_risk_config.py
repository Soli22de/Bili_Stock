#!/usr/bin/env python3
"""测试小资金超短线风险配置"""

from core.risk_engine import SimpleRiskManager

def test_small_capital_mode():
    """测试小资金模式配置"""
    
    # 创建小资金模式风险管理器
    risk_mgr = SimpleRiskManager(small_capital_mode=True)
    
    print("=== 小资金超短线模式配置 ===")
    for key, value in risk_mgr.config.items():
        print(f"{key}: {value}")
    
    print("\n=== 仓位计算测试 ===")
    for score in [1.8, 1.5, 1.3, 1.2, 1.05]:
        position = risk_mgr.calculate_simple_position(score, 50000)
        status = "✅全仓" if position == 1.0 else "❌不入场"
        print(f"信号强度 {score:.2f} -> {status} ({position:.1%})")
    
    print("\n=== 止损止盈测试 ===")
    entry_price = 10.0
    for score in [1.8, 1.3]:
        stops = risk_mgr.get_stop_levels(entry_price, score)
        stop_pct = (entry_price - stops['stop_loss']) / entry_price * 100
        profit_pct = (stops['take_profit'] - entry_price) / entry_price * 100
        print(f"信号{score:.1f}: 止损{stops['stop_loss']:.2f} (-{stop_pct:.1f}%) | 止盈{stops['take_profit']:.2f} (+{profit_pct:.1f}%)")

if __name__ == "__main__":
    test_small_capital_mode()