# visualizations.py
# Charts for the report, drawn from the result tables written by
# spark_sql_analytics.py and integrated_clv.py (./parquet/analytics/*).
#
#   figures/1_sales_over_time.png       daily revenue + 7-day average
#   figures/2_revenue_by_category.png   revenue per category
#   figures/3_views_vs_sales.png        most viewed vs top-selling products
#   figures/4_customer_segments.png     CLV segments: value vs engagement
#
# Usage:
#   python visualizations.py
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

ANALYTICS_PATH = "./parquet/analytics"
FIGURES_PATH = "./figures"

# Palette (light mode): categorical slots in fixed order, a sequential blue ramp
SURFACE = "#fcfcfb"
TEXT, TEXT_2, MUTED = "#0b0b0b", "#52514e", "#8a8984"
GRID = "#e6e5e0"
BLUE, ORANGE = "#2a78d6", "#eb6834"
BLUE_LIGHT = "#86b6ef"
OTHER = "#c9c8c2"
TIER_RAMP = {"Bronze": "#86b6ef", "Silver": "#5598e7", "Gold": "#2a78d6", "Platinum": "#184f95",
             "No purchases": OTHER}

plt.rcParams.update({
    "text.parse_math": False,   # "$" in labels is currency, not LaTeX math
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.size": 10, "text.color": TEXT, "axes.labelcolor": TEXT_2,
    "xtick.color": TEXT_2, "ytick.color": TEXT_2, "axes.edgecolor": GRID,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
    "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
})

money = mticker.FuncFormatter(
    lambda v, _: "$0" if v == 0 else f"${v / 1e6:.1f}M" if v >= 1e6 else f"${v / 1e3:.0f}K")


def load(name):
    return pd.read_parquet(f"{ANALYTICS_PATH}/{name}")


def subtitle(ax, text):
    ax.text(0, 1.02, text, transform=ax.transAxes, color=TEXT_2, fontsize=9.5, va="bottom")


def finish(fig, name):
    path = f"{FIGURES_PATH}/{name}"
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"saved {path}")


def sales_over_time():
    daily = load("daily_revenue").sort_values("date")
    daily["date"] = pd.to_datetime(daily["date"])
    # the first and last days are cut off by the data window; drop them if clearly partial,
    # then average over full 7-day windows only
    full_day = daily["orders"] >= 0.8 * daily["orders"].median()
    daily = daily[full_day].copy()
    daily["revenue_7d_avg"] = daily["revenue"].rolling(7, min_periods=7).mean()

    fig, ax = plt.subplots(figsize=(10, 4.6))
    ax.plot(daily["date"], daily["revenue"], color=BLUE_LIGHT, linewidth=1.2, label="Daily revenue")
    ax.plot(daily["date"], daily["revenue_7d_avg"], color=BLUE, linewidth=2.2, label="7-day average")
    ax.yaxis.set_major_formatter(money)
    ax.set_ylim(bottom=0)
    ax.grid(axis="x", visible=False)
    ax.legend(frameon=False, loc="lower right", labelcolor=TEXT_2)
    ax.set_title("Sales performance over time", pad=22)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    subtitle(ax, f"{daily['date'].min():%d %b} - {daily['date'].max():%d %b %Y} (full days)  ·  "
                 f"${daily['revenue'].sum() / 1e6:,.1f}M from {daily['orders'].sum():,} orders  ·  "
                 f"avg ${daily['revenue'].mean() / 1e6:.2f}M per day")
    finish(fig, "1_sales_over_time.png")


def revenue_by_category():
    cat = (load("category_monthly").groupby("category", as_index=False)["revenue"].sum()
           .sort_values("revenue"))
    total = cat["revenue"].sum()

    fig, ax = plt.subplots(figsize=(9, 8))
    bars = ax.barh(cat["category"], cat["revenue"], color=BLUE, height=0.7)
    ax.xaxis.set_major_formatter(money)
    ax.grid(axis="y", visible=False)
    ax.tick_params(axis="y", length=0)
    # label only the three largest categories
    for bar, value in list(zip(bars, cat["revenue"]))[-3:]:
        ax.text(bar.get_width(), bar.get_y() + bar.get_height() / 2,
                f"  ${value / 1e6:.1f}M ({value / total:.1%})", va="center", color=TEXT, fontsize=9)
    ax.set_title("Revenue by category", pad=22)
    subtitle(ax, f"Gross merchandise value of all order lines, {len(cat)} categories")
    finish(fig, "2_revenue_by_category.png")


def views_vs_sales(top_n=10):
    p = load("product_performance")
    top_sold = p["sales_rank"] <= top_n
    top_viewed = p["views_rank"] <= top_n
    r = p["sessions_viewed"].corr(p["units_sold"])

    fig, ax = plt.subplots(figsize=(9, 6.5))
    rest = p[~top_sold & ~top_viewed]
    ax.scatter(rest["sessions_viewed"], rest["units_sold"], s=10, color=OTHER, alpha=0.6,
               linewidths=0, label="Other products")
    ax.scatter(p.loc[top_viewed, "sessions_viewed"], p.loc[top_viewed, "units_sold"], s=46, color=BLUE,
               edgecolors=SURFACE, linewidths=1.5, label=f"Top {top_n} most viewed", zorder=3)
    ax.scatter(p.loc[top_sold, "sessions_viewed"], p.loc[top_sold, "units_sold"], s=46, color=ORANGE,
               edgecolors=SURFACE, linewidths=1.5, label=f"Top {top_n} best selling", zorder=4)
    # name the #1 product of each list
    for rank_col in ["sales_rank", "views_rank"]:
        row = p.loc[p[rank_col].idxmin()]
        ax.annotate(f"{row['product_id']}", (row["sessions_viewed"], row["units_sold"]),
                    xytext=(6, 6), textcoords="offset points", fontsize=8.5, color=TEXT)
    ax.set_xlabel("Sessions that viewed the product")
    ax.set_ylabel("Units sold")
    ax.legend(frameon=False, loc="upper left", labelcolor=TEXT_2)
    ax.set_title("Product performance: most viewed vs top selling", pad=22)
    both = (top_sold & top_viewed).sum()
    subtitle(ax, f"{len(p):,} products  ·  correlation r = {r:.2f}  ·  "
                 f"{both} of the top {top_n} appear in both lists")
    finish(fig, "3_views_vs_sales.png")


def customer_segments():
    order = [s for s in ["Bronze", "Silver", "Gold", "Platinum", "No purchases"]]
    seg = load("clv_segments").set_index("segment").reindex(order).dropna(subset=["customers"])
    seg = seg[seg["customers"] > 0]
    colors = [TIER_RAMP[s] for s in seg.index]

    panels = [("avg_clv", "Average predicted CLV", lambda v: f"${v:,.0f}"),
              ("orders_per_month", "Orders per month", lambda v: f"{v:.1f}"),
              ("sessions_per_month", "Sessions per month (HBase sample)", lambda v: f"{v:.1f}")]
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.4))
    for ax, (col, title, fmt) in zip(axes, panels):
        values = seg[col].fillna(0)
        bars = ax.bar(seg.index, values, color=colors, width=0.62)
        for bar, v in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), fmt(v),
                    ha="center", va="bottom", fontsize=9, color=TEXT)
        ax.set_title(title, fontsize=11, pad=8)
        ax.grid(axis="x", visible=False)
        ax.tick_params(axis="x", length=0)
        ax.set_yticks([])
    fig.suptitle("Customer segmentation by predicted lifetime value", x=0.01, ha="left",
                 fontsize=13, fontweight="bold", y=1.04)
    sizes = ", ".join(f"{s}: {int(n):,}" for s, n in seg["customers"].items())
    fig.text(0.01, 0.95, f"Customers per segment - {sizes}", color=TEXT_2, fontsize=9.5)
    finish(fig, "4_customer_segments.png")


if __name__ == "__main__":
    os.makedirs(FIGURES_PATH, exist_ok=True)
    sales_over_time()
    revenue_by_category()
    views_vs_sales()
    customer_segments()
