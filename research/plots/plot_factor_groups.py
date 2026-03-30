import os

import matplotlib.pyplot as plt
import pandas as pd


def plot_group_curves(group_ret: pd.DataFrame, ls_curve: pd.DataFrame, out_png: str, title: str) -> None:
    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    g = group_ret.copy()
    g["date"] = pd.to_datetime(g["date"])
    g = g.sort_values("date")
    qcols = [c for c in ["Q1", "Q2", "Q3", "Q4", "Q5"] if c in g.columns]
    curves = pd.DataFrame({"date": g["date"]})
    for c in qcols:
        curves[c] = (1 + g[c].fillna(0)).cumprod()
    l = ls_curve.copy()
    l["date"] = pd.to_datetime(l["date"])
    l = l.sort_values("date")
    plt.figure(figsize=(12, 7))
    for c in qcols:
        plt.plot(curves["date"], curves[c], label=c)
    if "long_short_curve" in l.columns:
        plt.plot(l["date"], l["long_short_curve"], label="Q5-Q1", linewidth=2.2, linestyle="--")
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Cumulative Return")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()
