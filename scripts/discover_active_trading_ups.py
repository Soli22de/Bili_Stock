import argparse
import datetime as dt
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd

import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from core.bili_collector import BiliCollector
from core.data_provider import DataProvider


def _load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def _save_json(path: str, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def _save_csv(path: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


FULLWIDTH_TRANSLATION = str.maketrans(
    "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
)


def normalize_text(s: str) -> str:
    s = (s or "").translate(FULLWIDTH_TRANSLATION)
    s = re.sub(r"\s+", "", s)
    return s


def load_stock_map() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], set]:
    path = getattr(config, "STOCK_MAP_PATH", "data/stock_map_final.json")
    raw = _load_json(path, {})
    if not isinstance(raw, dict):
        raw = {}
    normalized_to_code: Dict[str, str] = {}
    normalized_to_display: Dict[str, str] = {}
    for name, code in raw.items():
        n = normalize_text(str(name))
        c = str(code).zfill(6)
        if len(n) < 2:
            continue
        normalized_to_code[n] = c
        normalized_to_display[n] = str(name)
    return raw, normalized_to_code, normalized_to_display, set(normalized_to_code.keys())


TRADE_TERMS = [
    "实盘",
    "交割单",
    "持仓",
    "对账单",
    "仓位",
    "买入",
    "卖出",
    "低吸",
    "打板",
    "连板",
    "涨停",
    "龙头",
    "复盘",
    "早评",
    "午评",
    "收评",
    "明日计划",
    "止盈",
    "止损",
]

HYPE_TERMS = [
    "起飞",
    "翻倍",
    "大肉",
    "爆赚",
    "躺赢",
    "主升",
    "妖股",
    "吃板",
]

EX_POST_TERMS = [
    "昨天",
    "前天",
    "已经",
    "恭喜",
    "感谢",
    "涨停了",
    "翻倍了",
    "吃板了",
    "吃面",
    "马后炮",
]

FORWARD_TERMS = [
    "明天",
    "下周",
    "计划",
    "预期",
    "关注",
    "备选",
    "机会",
    "看好",
    "观察",
    "低吸点",
    "止损位",
]


STOCK_CODE_PATTERNS = [
    re.compile(r"\b[036]\d{5}\b"),
    re.compile(r"(?:sh|sz)?\d{6}", re.IGNORECASE),
]


def extract_stock_mentions(text: str, stock_name_set: set) -> List[str]:
    t = normalize_text(text)
    if not t:
        return []

    names: List[str] = []
    for token in re.findall(r"[\u4e00-\u9fff]{2,8}", t):
        if token in stock_name_set and token not in names:
            names.append(token)
    for token in re.findall(r"\*ST[\u4e00-\u9fff]{1,3}", t, flags=re.IGNORECASE):
        token = normalize_text(token)
        if token in stock_name_set and token not in names:
            names.append(token)
    return names


def has_stock_code(text: str) -> bool:
    t = normalize_text(text)
    return any(p.search(t) for p in STOCK_CODE_PATTERNS)


def count_hits(text: str, terms: List[str]) -> int:
    t = normalize_text(text)
    return sum(1 for term in terms if term and term in t)


def extract_prices_and_pct(text: str) -> Tuple[bool, bool]:
    t = normalize_text(text)
    has_price = bool(re.search(r"\b\d{1,3}(?:\.\d{1,2})\b", t))
    has_pct = bool(re.search(r"\b\d{1,3}(?:\.\d{1,2})?%\b", t))
    return has_price, has_pct


@dataclass
class PostFeatures:
    is_stock_related: bool
    stock_names: List[str]
    trade_hits: int
    hype_hits: int
    ex_post_hits: int
    forward_hits: int
    has_code: bool
    has_price: bool
    has_pct: bool

    @property
    def specificity_score(self) -> float:
        score = 0.0
        score += 1.0 if self.stock_names else 0.0
        score += 1.0 if self.has_code else 0.0
        score += 1.0 if self.has_price else 0.0
        score += 1.0 if self.has_pct else 0.0
        score += min(1.0, self.trade_hits / 3.0)
        return min(1.0, score / 4.5)


def build_post_features(text: str, stock_name_set: set) -> PostFeatures:
    stock_names = extract_stock_mentions(text, stock_name_set)
    trade_hits = count_hits(text, TRADE_TERMS)
    hype_hits = count_hits(text, HYPE_TERMS)
    ex_post_hits = count_hits(text, EX_POST_TERMS)
    forward_hits = count_hits(text, FORWARD_TERMS)
    code = has_stock_code(text)
    has_price, has_pct = extract_prices_and_pct(text)
    is_stock_related = bool(stock_names) or code or trade_hits > 0
    return PostFeatures(
        is_stock_related=is_stock_related,
        stock_names=stock_names,
        trade_hits=trade_hits,
        hype_hits=hype_hits,
        ex_post_hits=ex_post_hits,
        forward_hits=forward_hits,
        has_code=code,
        has_price=has_price,
        has_pct=has_pct,
    )


def load_up_list(path: str) -> Dict[int, str]:
    raw = _load_json(path, {})
    if not isinstance(raw, dict):
        return {}
    out: Dict[int, str] = {}
    for k, v in raw.items():
        try:
            out[int(k)] = str(v)
        except Exception:
            continue
    return out


def enrich_with_up_comments(videos: pd.DataFrame, comments: pd.DataFrame) -> pd.DataFrame:
    if videos.empty:
        return videos
    if comments is None or comments.empty:
        videos["up_comments"] = ""
        return videos

    grouped = defaultdict(list)
    for _, row in comments.iterrows():
        did = str(row.get("dynamic_id", "")).strip()
        msg = str(row.get("content", "")).strip()
        if not did or not msg:
            continue
        if len(grouped[did]) < 3:
            grouped[did].append(msg)

    up_comments = []
    for _, row in videos.iterrows():
        did = str(row.get("dynamic_id", "")).strip()
        up_comments.append(" ".join(grouped.get(did, [])))
    videos = videos.copy()
    videos["up_comments"] = up_comments
    return videos


def compute_weekly_active_days(df: pd.DataFrame) -> Dict[int, List[int]]:
    if df.empty:
        return {}
    df = df.copy()
    df["publish_dt"] = pd.to_datetime(df["publish_time"], errors="coerce")
    df = df.dropna(subset=["publish_dt"])
    df["date"] = df["publish_dt"].dt.date
    df["weekday"] = df["publish_dt"].dt.weekday
    df = df[df["weekday"] < 5]

    df["week"] = df["publish_dt"].dt.to_period("W-MON").astype(str)

    out: Dict[int, List[int]] = {}
    for author_id, g in df.groupby("author_id"):
        week_to_days = g.groupby("week")["date"].nunique().to_dict()
        out[int(author_id)] = [int(week_to_days[w]) for w in sorted(week_to_days.keys())][-4:]
    return out


def extract_keyword_candidates(texts: List[str], top_k: int = 50) -> List[str]:
    counter = Counter()
    for t in texts:
        s = normalize_text(t)
        for token in re.findall(r"[\u4e00-\u9fff]{2,6}", s):
            if token in {"今天", "明天", "我们", "大家", "一个", "这个", "那个"}:
                continue
            counter[token] += 1
    return [w for w, _ in counter.most_common(top_k)]


def update_keyword_pool_from_top_posts(videos: pd.DataFrame, keyword_pool_path: str, top_posts: int = 200):
    if videos.empty:
        return
    v = videos.copy()
    v["publish_dt"] = pd.to_datetime(v["publish_time"], errors="coerce")
    v = v.dropna(subset=["publish_dt"])
    v = v.sort_values("publish_dt", ascending=False).head(top_posts)
    texts = (v["title"].fillna("") + " " + v["content"].fillna("")).tolist()

    candidates = extract_keyword_candidates(texts, top_k=80)
    pool = _load_json(keyword_pool_path, {})
    existing = pool.get("keywords") if isinstance(pool, dict) else None
    if not isinstance(existing, list):
        existing = []
    merged = list(dict.fromkeys([*(str(x) for x in existing), *candidates]))
    _save_json(keyword_pool_path, {"keywords": merged, "updated_at": int(dt.datetime.now().timestamp())})


def score_bloggers(
    videos: pd.DataFrame,
    stock_name_set: set,
    normalized_to_display: Dict[str, str],
    normalized_to_code: Dict[str, str],
    min_weekdays_per_week: int,
    weeks_required: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if videos.empty:
        return pd.DataFrame(), pd.DataFrame()

    v = videos.copy()
    v["publish_dt"] = pd.to_datetime(v["publish_time"], errors="coerce")
    v = v.dropna(subset=["publish_dt"])
    v["text"] = (
        v["title"].fillna("").astype(str)
        + "\n"
        + v["content"].fillna("").astype(str)
        + "\n"
        + v.get("up_comments", "").fillna("").astype(str)
    )

    feat_rows = []
    per_up_stock_counter: Dict[int, Counter] = defaultdict(Counter)
    for idx, row in v.iterrows():
        author_id = int(row["author_id"])
        text = row["text"]
        feat = build_post_features(text, stock_name_set)
        
        # Filter out hindsight content from scoring
        # If a post contains ex-post terms, we treat it as not stock related for scoring purposes
        # unless it also has significant forward-looking content (e.g. plan + review)
        # User instruction: "马后炮只是不列入评分和参考而已"
        if feat.ex_post_hits > 0:
             feat.is_stock_related = False

        feat_rows.append(
            {
                "author_id": author_id,
                "dynamic_id": row.get("dynamic_id"),
                "publish_time": row.get("publish_time"),
                "is_stock_related": feat.is_stock_related,
                "trade_hits": feat.trade_hits,
                "hype_hits": feat.hype_hits,
                "ex_post_hits": feat.ex_post_hits,
                "forward_hits": feat.forward_hits,
                "specificity": feat.specificity_score,
                "stock_names": "|".join(feat.stock_names),
            }
        )

        title = str(row.get("title", ""))
        content = str(row.get("content", ""))
        title_names = extract_stock_mentions(title, stock_name_set)
        content_names = extract_stock_mentions(content, stock_name_set)
        for n in title_names:
            per_up_stock_counter[author_id][n] += 3
        for n in content_names:
            per_up_stock_counter[author_id][n] += 2

    feat_df = pd.DataFrame(feat_rows)
    posts_df = v[["author_id", "publish_time"]].copy()
    posts_df["is_stock_related"] = feat_df["is_stock_related"].values
    weekly_active = compute_weekly_active_days(posts_df[posts_df["is_stock_related"]])

    rank_rows = []
    for author_id, g in feat_df.groupby("author_id"):
        stock_related_posts = int(g["is_stock_related"].sum())
        specificity_avg = float(g.loc[g["is_stock_related"], "specificity"].mean()) if stock_related_posts else 0.0
        forward_hits = int(g["forward_hits"].sum())
        ex_post_hits = int(g["ex_post_hits"].sum())

        weeks = weekly_active.get(author_id, [])
        weeks_meeting = sum(1 for d in weeks if d >= min_weekdays_per_week)
        passes = weeks_meeting >= weeks_required

        active_days_sum = sum(weeks)
        activity_score = min(1.0, active_days_sum / max(1.0, float(min_weekdays_per_week * max(weeks_required, 1))))

        forward_component = min(1.0, (forward_hits + 1) / (ex_post_hits + 1))
        ex_post_component = min(1.0, ex_post_hits / max(1.0, forward_hits + 1))

        quality = 0.0
        quality += 50.0 * activity_score
        quality += 35.0 * specificity_avg
        quality += 25.0 * forward_component
        # Removed penalty for ex-post as those posts are now filtered out upstream
        # quality -= 20.0 * ex_post_component 
        quality = max(0.0, min(100.0, quality))

        top_stocks = per_up_stock_counter.get(author_id, Counter()).most_common(10)
        top_stock_items = []
        for n, cnt in top_stocks:
            display = normalized_to_display.get(n, n)
            code = normalized_to_code.get(n, "")
            top_stock_items.append(f"{display}({code})*{cnt}")

        rank_rows.append(
            {
                "blogger_id": author_id,
                "stock_related_posts_30d": stock_related_posts,
                "weekly_active_days": json.dumps(weeks, ensure_ascii=False),
                "weeks_meeting": weeks_meeting,
                "passes_weekly_rule": bool(passes),
                "top_stocks": "|".join(top_stock_items),
                "quality_score": round(float(quality), 2),
                "specificity_avg": round(float(specificity_avg), 4),
                "forward_hits": forward_hits,
                "ex_post_hits": ex_post_hits,
            }
        )

    rank_df = pd.DataFrame(rank_rows).sort_values(["passes_weekly_rule", "quality_score"], ascending=[False, False])

    hot_counter = Counter()
    for author_id, c in per_up_stock_counter.items():
        row = rank_df.loc[rank_df["blogger_id"] == author_id]
        if row.empty:
            continue
        if bool(row.iloc[0]["passes_weekly_rule"]):
            hot_counter.update(c)
    hot_rows = []
    for n, cnt in hot_counter.most_common(200):
        hot_rows.append(
            {
                "stock_name": normalized_to_display.get(n, n),
                "stock_code": normalized_to_code.get(n, ""),
                "mention_weight": int(cnt),
            }
        )
    hot_df = pd.DataFrame(hot_rows)
    return rank_df, hot_df


def merge_up_list(existing_path: str, selected: Dict[int, str]):
    current = _load_json(existing_path, {})
    if not isinstance(current, dict):
        current = {}
    for uid, name in selected.items():
        current[str(uid)] = name
    _save_json(existing_path, current)


def backtest_hot_stocks(hot_df: pd.DataFrame, top_n: int = 50) -> pd.DataFrame:
    if hot_df is None or hot_df.empty:
        return pd.DataFrame()
    dp = DataProvider()
    rows = []
    today = dt.date.today().strftime("%Y-%m-%d")
    for _, row in hot_df.head(top_n).iterrows():
        code = str(row.get("stock_code", "")).zfill(6)
        if not code or not code.isdigit() or len(code) != 6:
            continue
        start_date = (dt.date.today() - dt.timedelta(days=60)).strftime("%Y-%m-%d")
        df = dp.get_daily_data(code, start_date=start_date, end_date=today)
        if df is None or df.empty:
            continue
        df = df.reset_index().rename(columns={"index": "date"})
        if "close" not in df.columns:
            continue
        df = df.sort_values("date").reset_index(drop=True)
        if len(df) < 6:
            continue
        base = float(df.loc[len(df) - 6, "close"])
        last = float(df.loc[len(df) - 1, "close"])
        ret5 = (last / base) - 1.0 if base > 0 else None
        rows.append(
            {
                "stock_code": code,
                "stock_name": row.get("stock_name", ""),
                "mention_weight": int(row.get("mention_weight", 0)),
                "ret_5d_proxy": None if ret5 is None else round(float(ret5), 4),
                "base_date_proxy": str(df.loc[len(df) - 6, "date"]),
                "last_date_proxy": str(df.loc[len(df) - 1, "date"]),
            }
        )
    return pd.DataFrame(rows).sort_values(["ret_5d_proxy", "mention_weight"], ascending=[False, False])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=30)
    parser.add_argument("--max-ups", type=int, default=0)
    parser.add_argument("--min-weekdays-per-week", type=int, default=3)
    parser.add_argument("--weeks-required", type=int, default=2)
    parser.add_argument("--collect", action="store_true")
    parser.add_argument("--analyze-only", action="store_true")
    parser.add_argument("--backtest-hot", action="store_true")
    args = parser.parse_args()

    up_list_path = os.path.join("data", "up_list.json")
    uid_map = load_up_list(up_list_path)
    if args.max_ups and args.max_ups > 0:
        uid_map = dict(list(uid_map.items())[: args.max_ups])

    if args.collect and not args.analyze_only:
        collector = BiliCollector(mode="discovery", days_back=args.days_back)
        import asyncio

        asyncio.run(collector.run(uid_map=uid_map))

    videos_path = os.path.join("data", "discovery_videos.csv")
    comments_path = os.path.join("data", "discovery_comments.csv")
    if not os.path.exists(videos_path):
        print(f"Missing {videos_path}. Run with --collect first.")
        return

    videos = pd.read_csv(videos_path, encoding="utf-8-sig")
    comments = pd.read_csv(comments_path, encoding="utf-8-sig") if os.path.exists(comments_path) else pd.DataFrame()
    videos = enrich_with_up_comments(videos, comments)

    _, normalized_to_code, normalized_to_display, name_set = load_stock_map()

    rank_df_full, hot_df = score_bloggers(
        videos=videos,
        stock_name_set=name_set,
        normalized_to_display=normalized_to_display,
        normalized_to_code=normalized_to_code,
        min_weekdays_per_week=args.min_weekdays_per_week,
        weeks_required=args.weeks_required,
    )

    rank_df = rank_df_full.copy()
    if not rank_df.empty:
        rank_df["home_url"] = rank_df["blogger_id"].apply(lambda x: f"https://space.bilibili.com/{int(x)}")
        rank_df["author_name"] = rank_df["blogger_id"].apply(lambda x: uid_map.get(int(x), f"User_{int(x)}"))

        rank_df = rank_df[(rank_df["passes_weekly_rule"] == True) & (rank_df["stock_related_posts_30d"] >= 3)].copy()

        rank_df = rank_df[
            [
                "blogger_id",
                "author_name",
                "home_url",
                "stock_related_posts_30d",
                "weekly_active_days",
                "top_stocks",
                "quality_score",
            ]
        ]

    _save_csv(os.path.join("data", "trader_bloggers_rank.csv"), rank_df)
    _save_csv(os.path.join("data", "hot_stocks.csv"), hot_df)

    if args.backtest_hot:
        backtest_df = backtest_hot_stocks(hot_df, top_n=50)
        _save_csv(os.path.join("data", "hot_stocks_backtest.csv"), backtest_df)

    # Clean up up_list.json by removing UPs that were analyzed but failed the criteria
    # We keep UPs that were NOT analyzed (e.g. no data yet) to avoid accidental deletion of new UPs before collection
    if not rank_df_full.empty:
        # IDs that were part of the analysis (meaning we had some data/video for them)
        analyzed_ids = set(rank_df_full["blogger_id"].astype(int))
        # IDs that passed the filter
        passed_ids = set(rank_df["blogger_id"].astype(int)) if not rank_df.empty else set()
        
        # Identify failed IDs
        failed_ids = analyzed_ids - passed_ids
        
        print(f"Analysis coverage: {len(analyzed_ids)} UPs. Passed: {len(passed_ids)}. Failed: {len(failed_ids)}.")
        
        current_ups = load_up_list(up_list_path)
        original_count = len(current_ups)
        
        # Remove failed UPs
        new_ups = {k: v for k, v in current_ups.items() if int(k) not in failed_ids}
        
        # Ensure passed UPs are definitely in (merge/update names)
        if not rank_df.empty:
            for _, r in rank_df.iterrows():
                new_ups[str(r["blogger_id"])] = str(r["author_name"])
            
        if len(new_ups) < original_count or failed_ids:
            _save_json(up_list_path, new_ups)
            print(f"Updated up_list.json. Removed {original_count - len(new_ups)} ineligible UPs. Current count: {len(new_ups)}")
        else:
            print("No UPs removed.")

    update_keyword_pool_from_top_posts(
        videos=videos,
        keyword_pool_path=os.path.join("data", "keyword_pool.json"),
        top_posts=200,
    )

    print(f"Rank rows: {0 if rank_df is None else len(rank_df)}")


if __name__ == "__main__":
    main()
