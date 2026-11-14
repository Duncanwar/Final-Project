# visualizations.py
# Charts for the report, drawn from the result tables written by
# dataframe.py, spark_sql_analytics.py and integrated_recommendations.py
# (./parquet/analytics/*), plus the curated transactions for the order patterns.
#
#   figures/sales over time.png       daily revenue + 7-day average
#   figures/views vs sales.png        most viewed vs top-selling products
#   figures/cohort spending.png       spend per user by registration cohort and month
#   figures/recommendations.png       how purchases (MongoDB) and views (HBase) build a recommendation
#   figures/orders by hour.png        when orders are placed: weekday x hour heatmap
#   figures/basket size.png           orders vs revenue by number of items per order
#
# Usage:
#   python visualizations.py
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.colors as mcolors
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

plt.rcParams.update({
    "text.parse_math": False,   # "$" in labels is currency, not LaTeX math
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.size": 10, "text.color": TEXT, "axes.labelcolor": TEXT_2,
    "xtick.color": TEXT_2, "ytick.color": TEXT_2, "axes.edgecolor": GRID,
    "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.spines.left": False,
    "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
})

money = mticker.FuncFormatter(
    lambda v, _: "$0" if v == 0 else f"${v / 1e6:.1f}M" if v >= 1e6 else f"${v / 1e3:.0f}K")


def short(name, n=30):
    """Product names are long; cut them for labels."""
    return name if len(name) <= n else name[:n - 1].rstrip() + "…"


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
    finish(fig, "sales over time.png")



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
        ax.annotate(short(row["name"]), (row["sessions_viewed"], row["units_sold"]),
                    xytext=(6, 6), textcoords="offset points", fontsize=8.5, color=TEXT)
    ax.set_xlabel("Sessions that viewed the product")
    ax.set_ylabel("Units sold")
    ax.legend(frameon=False, loc="upper left", labelcolor=TEXT_2)
    ax.set_title("Product performance: most viewed vs top selling", pad=22)
    both = (top_sold & top_viewed).sum()
    subtitle(ax, f"{len(p):,} products  ·  correlation r = {r:.2f}  ·  "
                 f"{both} of the top {top_n} appear in both lists")
    finish(fig, "views vs sales.png")



def cohort_spending():
    """Spend per registered user per day, one line per registration cohort (dataframe.py).
    Per day, so the 29 days of data in August compare fairly with 30-31 in Sep/Oct."""
    c = pd.read_parquet("./parquet/analytics/user_cohorts").sort_values(["cohort", "order_month"])
    cohorts = sorted(c["cohort"].unique())
    # one hue, light (oldest cohort) -> dark (newest): the order of cohorts shows in the shade
    shades = mcolors.LinearSegmentedColormap.from_list("blues", ["#86b6ef", "#2a78d6", "#184f95"])
    months = sorted(c["order_month"].unique())
    x = {m: i for i, m in enumerate(months)}

    fig, ax = plt.subplots(figsize=(9, 4.8))
    for k, cohort in enumerate(cohorts):
        g = c[c["cohort"] == cohort]
        ax.plot([x[m] for m in g["order_month"]], g["revenue_per_user_per_day"], marker="o",
                markersize=5, linewidth=1.8, color=shades(k / max(len(cohorts) - 1, 1)),
                label=f"{pd.Period(cohort).strftime('%b %Y')} (n={int(g['cohort_size'].iloc[0]):,})")
    ax.set_xticks(range(len(months)), [pd.Period(m).strftime("%b %Y") for m in months])
    ax.set_ylim(0, c["revenue_per_user_per_day"].max() * 1.25)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_ylabel("Spend per registered user per day")
    ax.grid(axis="x", visible=False)
    ax.legend(title="Registration cohort", frameon=False, ncol=4, loc="lower center",
              fontsize=8.5, title_fontsize=9, labelcolor=TEXT_2)
    low, high = c["revenue_per_user_per_day"].min(), c["revenue_per_user_per_day"].max()
    ax.set_title("Every cohort spends the same, whatever its registration month", pad=22)
    subtitle(ax, f"Spend per user per day ranges only ${low:,.0f}-${high:,.0f} across "
                 f"{len(cohorts)} cohorts; {c['active_pct'].min():.0f}% of every cohort bought every month")
    finish(fig, "cohort spending.png")


def recommendations():
    """Stacked bars: each suggestion's score = purchase part (MongoDB) + view part (HBase),
    for the best-selling product's page; plus the evidence mix across all suggestions."""
    recs = load("product_affinity")
    sales = load("product_performance")
    top = sales.loc[sales["sales_rank"].idxmin(), "product_id"]
    page = recs[recs["product_id"] == top].sort_values("rank", ascending=False)
    buy_part = 0.6 * page["buy_confidence"]
    view_part = 0.4 * page["view_confidence"]
    labels = [short(n) for n in page["suggested_name"]]

    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(11, 4.2), gridspec_kw={"width_ratios": [2.2, 1]})
    ax.barh(labels, buy_part, color=BLUE, height=0.6, label="Bought together (MongoDB orders)")
    ax.barh(labels, view_part, left=buy_part, color=ORANGE, height=0.6,
            label="Viewed together (HBase sessions)")
    ax.set_xlim(0, (buy_part + view_part).max() * 1.15)
    ax.set_xlabel("Recommendation score")
    ax.grid(axis="y", visible=False)
    ax.tick_params(axis="y", length=0)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.45, -0.16), ncol=2, fontsize=8.5,
              labelcolor=TEXT_2)
    ax.set_title(f"Suggestions for the best seller: {short(page['name'].iloc[0])}", fontsize=11, pad=8)

    mix = recs["evidence"].value_counts().reindex(["both", "bought together", "viewed together"]).fillna(0)
    colors = ["#184f95", BLUE, ORANGE]
    bars = ax2.bar(["Both", "Bought\ntogether", "Viewed\ntogether"], mix.values, color=colors, width=0.6)
    for bar, n in zip(bars, mix.values):
        ax2.text(bar.get_x() + bar.get_width() / 2, n, f"{n / mix.sum():.0%}", ha="center",
                 va="bottom", fontsize=9, color=TEXT)
    ax2.set_yticks([])
    ax2.grid(False)
    ax2.tick_params(axis="x", length=0)
    ax2.set_title(f"Evidence behind all {len(recs):,} suggestions", fontsize=11, pad=8)

    fig.suptitle("Recommendations combine what customers buy and what they browse",
                 x=0.01, ha="left", fontsize=13, fontweight="bold", y=1.06)
    fig.text(0.01, 0.97, "Score = 0.6 x share of orders with both products + 0.4 x share of sessions "
                         "viewing both", color=TEXT_2, fontsize=9.5)
    finish(fig, "recommendations.png")


def orders_by_hour():
    """Heatmap: number of orders by weekday and hour of day (curated transactions)."""
    t = pd.read_parquet("./parquet/curated/transactions", columns=["timestamp"])
    t["weekday"] = t["timestamp"].dt.dayofweek
    t["hour"] = t["timestamp"].dt.hour
    grid = t.pivot_table(index="weekday", columns="hour", aggfunc="size", fill_value=0)
    # orders per hour slot on an average day (each weekday occurs ~13 times in 90 days)
    days = t.groupby("weekday")["timestamp"].apply(lambda s: s.dt.date.nunique())
    grid = grid.div(days, axis=0)

    ramp = mcolors.LinearSegmentedColormap.from_list("blues", ["#eaf2fc", "#86b6ef", "#2a78d6", "#184f95"])
    fig, ax = plt.subplots(figsize=(11, 3.6))
    im = ax.imshow(grid.values, cmap=ramp, aspect="auto", vmin=0)
    ax.grid(False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_yticks(range(7), ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}:00" for h in range(0, 24, 2)])
    ax.tick_params(length=0)
    ax.set_xlabel("Hour of day")
    cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.01)
    cbar.outline.set_visible(False)
    cbar.set_label("Orders per hour", color=TEXT_2)
    peak = grid.stack().idxmax()
    ax.set_title("Orders arrive evenly across days and hours", pad=22)
    subtitle(ax, f"Average orders per hour: {grid.values.mean():,.0f}; busiest slot "
                 f"{['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][peak[0]]} {peak[1]:02d}:00 "
                 f"with {grid.values.max():,.0f}, quietest {grid.values.min():,.0f}")
    finish(fig, "orders by hour.png")


def basket_size():
    """Share of orders vs share of revenue by number of items in the order."""
    t = pd.read_parquet("./parquet/curated/transactions", columns=["item_count", "total"])
    t["item_count"] = t["item_count"].clip(upper=4)   # 4+ items are rare: one group
    g = t.groupby("item_count").agg(orders=("total", "size"), revenue=("total", "sum"))
    share = pd.DataFrame({"Share of orders": g["orders"] / g["orders"].sum(),
                          "Share of revenue": g["revenue"] / g["revenue"].sum()})

    fig, ax = plt.subplots(figsize=(9, 4.4))
    x = range(len(share))
    w = 0.38
    ax.bar([i - w / 2 for i in x], share["Share of orders"], width=w, color=BLUE_LIGHT, label="Share of orders")
    ax.bar([i + w / 2 for i in x], share["Share of revenue"], width=w, color=BLUE, label="Share of revenue")
    for i, (o, r) in enumerate(share.values):
        ax.text(i - w / 2, o, f"{o:.0%}", ha="center", va="bottom", fontsize=8.5, color=TEXT_2)
        ax.text(i + w / 2, r, f"{r:.0%}", ha="center", va="bottom", fontsize=8.5, color=TEXT)
    ax.set_xticks(list(x), [("4+ items" if n == 4 else f"{n} item{'s' if n > 1 else ''}") for n in share.index])
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
    ax.grid(axis="x", visible=False)
    ax.tick_params(axis="x", length=0)
    ax.legend(frameon=False, labelcolor=TEXT_2)
    multi = share.loc[share.index > 1]
    aov = (g["revenue"] / g["orders"])
    ax.set_title(f"Orders with 2+ items bring {multi['Share of revenue'].sum():.0%} of revenue", pad=22)
    subtitle(ax, f"Orders with 2+ items: {multi['Share of orders'].sum():.0%} of orders, "
                 f"{multi['Share of revenue'].sum():.0%} of revenue; average order "
                 f"${aov.iloc[0]:,.0f} (1 item) vs ${aov.iloc[2]:,.0f} (3 items)")
    finish(fig, "basket size.png")


if __name__ == "__main__":
    os.makedirs(FIGURES_PATH, exist_ok=True)
    sales_over_time()
    views_vs_sales()
    cohort_spending()
    recommendations()
    orders_by_hour()
    basket_size()
