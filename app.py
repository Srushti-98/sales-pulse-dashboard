# app.py
import pandas as pd
from pathlib import Path
import streamlit as st
import plotly.express as px

BASE = Path(__file__).resolve().parent
CURATED = BASE / "data" / "curated"   # <-- robust path for Render

st.set_page_config(page_title="Sales Pulse", layout="wide")

@st.cache_data(show_spinner=False)
def load_parquet(name):
    path = CURATED / f"{name}.parquet"
    if not path.exists():
        st.error(f"Missing {path}. Run: `python etl.py`")
        st.stop()
    return pd.read_parquet(path)

kpis_by_day = load_parquet("kpis_by_day")
cat_daily   = load_parquet("category_daily")
rfm_scores  = load_parquet("rfm_scores")

# Convert dtypes (PyArrow) to nice pandas
kpis_by_day["date"] = pd.to_datetime(kpis_by_day["date"])

# Sidebar filters
st.sidebar.header("Filters")
min_d, max_d = kpis_by_day["date"].min(), kpis_by_day["date"].max()
date_range = st.sidebar.date_input("Date range", (min_d, max_d), min_value=min_d, max_value=max_d)
if isinstance(date_range, tuple):
    start_d, end_d = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
else:
    start_d, end_d = min_d, max_d

available_cats = sorted(cat_daily["category"].dropna().unique().tolist())
sel_cats = st.sidebar.multiselect("Categories", available_cats, default=available_cats)

# Apply filters
kpis_f = kpis_by_day[(kpis_by_day["date"] >= start_d) & (kpis_by_day["date"] <= end_d)]
cat_f = cat_daily[(pd.to_datetime(cat_daily["date"]) >= start_d) &
                  (pd.to_datetime(cat_daily["date"]) <= end_d) &
                  (cat_daily["category"].isin(sel_cats))]
# KPI header
total_rev = float(kpis_f["revenue"].sum())
total_orders = int(kpis_f["orders"].sum())
active_users = int(kpis_f["active_users"].sum())  # sum over days (not distinct across range)
aov = (total_rev / total_orders) if total_orders else 0.0

st.title("🛍️ Sales Pulse — E-commerce KPIs + RFM")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Revenue (₹)", f"{total_rev:,.0f}")
c2.metric("Orders", f"{total_orders:,}")
c3.metric("AOV (₹)", f"{aov:,.2f}")
c4.metric("Active Users (daily sum)", f"{active_users:,}")

# Revenue trend
st.subheader("Revenue by Day")
fig = px.line(kpis_f, x="date", y="revenue", markers=True)
fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=350)
st.plotly_chart(fig, use_container_width=True)

# Category share and trend
left, right = st.columns((1,1))
with left:
    st.subheader("Revenue Share by Category")
    share = (cat_f.groupby("category", as_index=False)["revenue"].sum()
                 .sort_values("revenue", ascending=False))
    if not share.empty:
        pie = px.pie(share, names="category", values="revenue", hole=0.45)
        pie.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=360, legend_title_text="")
        st.plotly_chart(pie, use_container_width=True)
    else:
        st.info("No data for selected filters.")

with right:
    st.subheader("Category Trend")
    if not cat_f.empty:
        trend = cat_f.copy()
        trend["date"] = pd.to_datetime(trend["date"])
        line = px.line(trend, x="date", y="revenue", color="category")
        line.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=360, legend_title_text="")
        st.plotly_chart(line, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# RFM heatmap (R x F with avg Monetary)
st.subheader("RFM Heatmap (Avg Monetary by R & F)")
rfm = rfm_scores.copy()
# Keep only present users (no filter by date in RFM; it's snapshot at ETL run)
pivot = (rfm.groupby(["R","F"], as_index=False)
             .agg(avg_M=("monetary","mean"),
                  users=("user_id","count")))
heat = pivot.pivot(index="R", columns="F", values="avg_M").sort_index(ascending=False)
heat = heat.round(0)

if not heat.empty:
    heat_fig = px.imshow(
        heat.values,
        x=[str(c) for c in heat.columns],
        y=[str(i) for i in heat.index],
        labels=dict(x="F (Frequency Score)", y="R (Recency Score)", color="Avg ₹ Monetary"),
        aspect="auto",
        text_auto=True
    )
    heat_fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=420)
    st.plotly_chart(heat_fig, use_container_width=True)
    st.caption("Scores range 1–5. Higher R & F cells = more valuable cohorts.")
else:
    st.info("RFM data not available.")

# Top customers table
st.subheader("Top Customers by Monetary")
topn = (rfm.sort_values("monetary", ascending=False)
            .loc[:, ["user_id","recency_days","frequency","monetary","R","F","M"]]
            .head(20))
topn = topn.rename(columns={
    "user_id":"User",
    "recency_days":"Recency (days)",
    "frequency":"Frequency",
    "monetary":"Monetary (₹)"
})
st.dataframe(topn, use_container_width=True)
