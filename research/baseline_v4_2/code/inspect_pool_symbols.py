import sqlite3

import pandas as pd


def main():
    conn = sqlite3.connect(r"C:/jz_code/Bili_Stock/data/cubes.db")
    df = pd.read_sql_query(
        "select stock_symbol, count(*) as c from rebalancing_history group by stock_symbol order by c desc limit 50",
        conn,
    )
    conn.close()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
