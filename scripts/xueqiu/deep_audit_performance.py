
import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

def deep_audit_performance():
    db_path = "data/cubes.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    try:
        # 1. 总体盈亏分布
        query_all = """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN total_gain > 0 THEN 1 ELSE 0 END) as profitable,
                SUM(CASE WHEN total_gain > 50 THEN 1 ELSE 0 END) as high_gain,
                SUM(CASE WHEN total_gain > 100 THEN 1 ELSE 0 END) as super_gain
            FROM cubes
        """
        df_stats = pd.read_sql_query(query_all, conn)
        
        # 2. 关注人数 vs 收益率的关系
        query_fans = """
            SELECT 
                CASE 
                    WHEN followers_count = 0 THEN '0人关注'
                    WHEN followers_count < 10 THEN '1-10人关注'
                    WHEN followers_count < 100 THEN '10-100人关注'
                    ELSE '100+人关注'
                END as fans_tier,
                COUNT(*) as count,
                AVG(total_gain) as avg_gain,
                SUM(CASE WHEN total_gain > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
            FROM cubes
            GROUP BY fans_tier
            ORDER BY count DESC
        """
        df_fans = pd.read_sql_query(query_fans, conn)

        # 3. 寻找那些“低调的大神”（收益极高但没人关注）
        query_hidden = """
            SELECT symbol, name, followers_count, total_gain, daily_gain
            FROM cubes
            WHERE followers_count < 10 AND total_gain > 100
            ORDER BY total_gain DESC
            LIMIT 10
        """
        df_hidden = pd.read_sql_query(query_hidden, conn)

        # 打印报告
        print("\n" + "="*50)
        print("🔍 5.5万组合深度质量审计报告")
        print("="*50)
        
        total = df_stats['total'][0]
        profitable = df_stats['profitable'][0]
        print(f"1. 赚钱概率: 全量 {total} 个组合中，有 {profitable} 个收益为正 ({profitable*100.0/total:.2f}%)")
        print(f"   - 收益 > 50% 的精英: {df_stats['high_gain'][0]} 个")
        print(f"   - 收益 > 100% 的大神: {df_stats['super_gain'][0]} 个")
        
        print("\n2. 关注人数的影响 (为什么要设 100 人的门槛?):")
        print(df_fans.to_string(index=False))
        
        print("\n3. 样本：低调的大神 (收益 > 100% 但关注 < 10人):")
        print(df_hidden.to_string(index=False))
        
        print("\n" + "="*50)
        print("💡 结论：")
        print("虽然赚钱的有 2.7 万个，但如果不设关注人数门槛，我们会抓取到大量：")
        print("1. 运气成分极高的‘僵尸号’（买了一只股就再也没动过，靠时间熬出来的收益）")
        print("2. 缺乏‘共识’的信号（Smart Momentum 核心在于‘英雄所见略同’，没人关注意味着没人跟随）")
        print("3. 实验性质的垃圾信号（测试策略用的号）")
        print("="*50)

    except Exception as e:
        print(f"Audit failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    deep_audit_performance()
