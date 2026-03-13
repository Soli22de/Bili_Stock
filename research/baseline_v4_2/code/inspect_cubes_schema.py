import sqlite3


def main():
    p = r"C:/jz_code/Bili_Stock/data/cubes.db"
    conn = sqlite3.connect(p)
    cur = conn.cursor()
    cur.execute("select name from sqlite_master where type='table' order by name")
    tables = [r[0] for r in cur.fetchall()]
    print("tables", tables)
    for t in tables:
        cur.execute(f"pragma table_info({t})")
        cols = [x[1] for x in cur.fetchall()]
        if cols:
            print(t, cols)
    conn.close()


if __name__ == "__main__":
    main()
