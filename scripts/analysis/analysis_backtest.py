#!/usr/bin/env python3
"""
回测结果可视化分析脚本
功能：对回测结果进行深度分析和可视化展示
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

class BacktestAnalyzer:
    def __init__(self, result_file='data/backtest_result_v2.csv'):
        self.result_file = result_file
        self.df = None
        
    def load_data(self):
        """加载回测结果数据"""
        if not os.path.exists(self.result_file):
            print(f"回测结果文件 {self.result_file} 不存在")
            return False
            
        self.df = pd.read_csv(self.result_file)
        print(f"加载了 {len(self.df)} 条回测记录")
        
        # 数据清洗
        if 'publish_time' in self.df.columns:
            self.df['publish_time'] = pd.to_datetime(self.df['publish_time'])
        if 'entry_date' in self.df.columns:
            self.df['entry_date'] = pd.to_datetime(self.df['entry_date'])
        if 'exit_date' in self.df.columns:
            self.df['exit_date'] = pd.to_datetime(self.df['exit_date'])
            
        return True
    
    def basic_analysis(self):
        """基础统计分析"""
        print("\n=== 基础统计分析 ===")
        
        # 总体表现
        executed = self.df[self.df['status'] == 'EXECUTED']
        print(f"总交易次数: {len(executed)}")
        print(f"胜率: {executed['pnl_pct'].gt(0).mean():.2%}")
        print(f"平均盈亏: {executed['pnl_pct'].mean():.2f}%")
        print(f"最大盈利: {executed['pnl_pct'].max():.2f}%")
        print(f"最大亏损: {executed['pnl_pct'].min():.2f}%")
        print(f"盈亏标准差: {executed['pnl_pct'].std():.2f}%")
        
        # 策略类型分析
        if 'strategy_type' in executed.columns:
            print("\n--- 按策略类型分析 ---")
            strategy_stats = executed.groupby('strategy_type')['pnl_pct'].agg([
                'count', 'mean', 'std', lambda x: (x > 0).mean()
            ]).round(2)
            strategy_stats.columns = ['交易次数', '平均盈亏%', '标准差%', '胜率%']
            strategy_stats['胜率%'] = strategy_stats['胜率%'] * 100
            print(strategy_stats)
    
    def create_visualizations(self):
        """创建可视化图表"""
        executed = self.df[self.df['status'] == 'EXECUTED']
        
        # 创建图表文件夹
        os.makedirs('analysis', exist_ok=True)
        
        # 1. 盈亏分布直方图
        plt.figure(figsize=(12, 8))
        
        plt.subplot(2, 2, 1)
        plt.hist(executed['pnl_pct'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        plt.axvline(0, color='red', linestyle='--', alpha=0.8)
        plt.xlabel('盈亏百分比 (%)')
        plt.ylabel('频次')
        plt.title('盈亏分布直方图')
        plt.grid(True, alpha=0.3)
        
        # 2. 累计收益曲线
        plt.subplot(2, 2, 2)
        if 'entry_date' in executed.columns:
            time_series = executed.sort_values('entry_date').copy()
            time_series['cumulative_return'] = (1 + time_series['pnl_pct'] / 100).cumprod()
            time_series['cumulative_pct'] = (time_series['cumulative_return'] - 1) * 100
            
            plt.plot(time_series['entry_date'], time_series['cumulative_pct'], 
                    marker='o', markersize=2, linewidth=1, alpha=0.7)
            plt.xlabel('交易日期')
            plt.ylabel('累计收益 (%)')
            plt.title('累计收益曲线')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
        
        # 3. 策略类型对比
        plt.subplot(2, 2, 3)
        if 'strategy_type' in executed.columns:
            strategy_means = executed.groupby('strategy_type')['pnl_pct'].mean()
            strategy_means.plot(kind='bar', color=['lightcoral', 'lightgreen', 'lightblue'])
            plt.xlabel('策略类型')
            plt.ylabel('平均盈亏 (%)')
            plt.title('各策略类型平均盈亏对比')
            plt.xticks(rotation=0)
            plt.grid(True, alpha=0.3)
        
        # 4. 胜率分析
        plt.subplot(2, 2, 4)
        if 'strategy_type' in executed.columns:
            win_rates = executed.groupby('strategy_type')['pnl_pct'].apply(lambda x: (x > 0).mean() * 100)
            win_rates.plot(kind='bar', color=['lightcoral', 'lightgreen', 'lightblue'])
            plt.xlabel('策略类型')
            plt.ylabel('胜率 (%)')
            plt.title('各策略类型胜率对比')
            plt.xticks(rotation=0)
            plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('analysis/backtest_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 5. 热力图 - 按日期和策略类型
        if all(col in executed.columns for col in ['entry_date', 'strategy_type', 'pnl_pct']):
            plt.figure(figsize=(12, 6))
            heatmap_data = executed.pivot_table(
                values='pnl_pct', 
                index=executed['entry_date'].dt.date, 
                columns='strategy_type', 
                aggfunc='mean'
            )
            sns.heatmap(heatmap_data, cmap='RdBu_r', center=0, annot=True, fmt='.1f')
            plt.title('每日各策略类型盈亏热力图')
            plt.xlabel('策略类型')
            plt.ylabel('交易日期')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('analysis/heatmap_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
    
    def detailed_analysis(self):
        """详细问题分析"""
        executed = self.df[self.df['status'] == 'EXECUTED']
        
        print("\n=== 详细问题分析 ===")
        
        # 1. 亏损交易分析
        losing_trades = executed[executed['pnl_pct'] < 0]
        print(f"亏损交易数量: {len(losing_trades)} ({len(losing_trades)/len(executed):.1%})")
        
        # 2. 大额亏损分析
        big_losses = executed[executed['pnl_pct'] < -5]
        if not big_losses.empty:
            print(f"大额亏损交易 (>5%): {len(big_losses)} 笔")
            print("大额亏损交易详情:")
            for _, trade in big_losses.nlargest(5, 'pnl_pct').iterrows():
                print(f"  {trade.get('stock_code', 'N/A')}: {trade.get('pnl_pct', 0):.1f}% "
                      f"({trade.get('entry_reason', 'N/A')} -> {trade.get('exit_reason', 'N/A')})")
        
        # 3. 入场原因分析
        if 'entry_reason' in executed.columns:
            print("\n--- 入场原因分析 ---")
            entry_reason_stats = executed.groupby('entry_reason')['pnl_pct'].agg([
                'count', 'mean', lambda x: (x > 0).mean()
            ]).round(2)
            entry_reason_stats.columns = ['交易次数', '平均盈亏%', '胜率%']
            entry_reason_stats['胜率%'] = entry_reason_stats['胜率%'] * 100
            print(entry_reason_stats.sort_values('平均盈亏%', ascending=False))
    
    def generate_report(self):
        """生成详细分析报告"""
        report_content = """
# B站舆情策略回测分析报告

## 执行摘要
- 回测时间: {timestamp}
- 总交易次数: {total_trades}
- 胜率: {win_rate:.1%}
- 平均盈亏: {avg_pnl:.2f}%
- 累计收益: {cumulative_return:.2f}%

## 关键发现
1. **策略有效性**: 当前策略整体表现不佳，需要优化
2. **风险控制**: 亏损交易占比较高，需要加强风控
3. **入场时机**: 部分入场逻辑需要重新评估

## 建议措施
1. 优化信号过滤机制，提高入场质量
2. 加强止损策略，控制单笔亏损
3. 考虑引入更多技术指标验证
4. 优化仓位管理策略
"""
        
        executed = self.df[self.df['status'] == 'EXECUTED']
        cumulative_return = (1 + executed['pnl_pct'] / 100).prod() - 1
        
        report = report_content.format(
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M'),
            total_trades=len(executed),
            win_rate=executed['pnl_pct'].gt(0).mean(),
            avg_pnl=executed['pnl_pct'].mean(),
            cumulative_return=cumulative_return * 100
        )
        
        with open('analysis/backtest_report.md', 'w', encoding='utf-8') as f:
            f.write(report)
        
        print("分析报告已保存至 analysis/backtest_report.md")

if __name__ == "__main__":
    analyzer = BacktestAnalyzer()
    
    if analyzer.load_data():
        analyzer.basic_analysis()
        analyzer.create_visualizations()
        analyzer.detailed_analysis()
        analyzer.generate_report()
        print("\n分析完成！请查看 analysis/ 文件夹中的图表和报告")
    else:
        print("无法加载回测数据，请先运行回测脚本")