# Sales Pulse - Spark + Streamlit

An e‑commerce analytics project. A small Spark ETL produces daily KPIs, category trends, and RFM scores; a Streamlit dashboard renders the results for screenshots and a short demo.

---


## [Click here for Sales Pulse Dashbaord Demo](https://sales-pulse-g80r.onrender.com)

---
## Features

* Daily KPIs: Revenue, Orders, AOV, Active Users
* Trends: Revenue by day; Category share and category trend
* RFM scoring: 1–5 scores for Recency, Frequency, Monetary and a 5×5 heatmap
* Runs fully local: Spark in `local[*]`; synthetic dataset included

---

## Tech Stack

* PySpark 3.5 (Spark SQL, UDFs, approxQuantile)
* Streamlit and Plotly for the dashboard
* Pandas / PyArrow for Parquet I/O

---

## Project Structure

```
sales-pulse/
├─ app.py                 # Streamlit dashboard (reads curated parquet via pandas)
├─ etl.py                 # Spark ETL: KPIs, category daily, RFM
├─ gen_orders.py          # Synthetic data generator (orders.csv)
├─ requirements.txt
├─ data/
│  ├─ raw/orders.csv      # Generated sample data
│  └─ curated/            # Parquet outputs written by etl.py
└─ docs/                  # (Optional) screenshots/GIFs for your portfolio
```

---

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Generate sample data
python gen_orders.py

# Run Spark ETL -> writes parquet to data/curated
python etl.py

# Launch dashboard
streamlit run app.py
```

Open the local URL printed by Streamlit (usually [http://localhost:8501](http://localhost:8501)). During the ETL step, open the Spark UI link from the terminal to capture a DAG screenshot.

---

## Data Model

**orders.csv** (generated):

* `order_id` (long), `user_id` (int), `ts` (ISO‑8601 timestamp)
* `amount` (double), `category` (string), `used_promo` (0/1), `payment_type` (string)

**Curated parquet** written by ETL:

* `kpis_by_day` → `date, revenue, orders, active_users, aov`
* `category_daily` → `date, category, revenue, orders`
* `rfm_scores` → per‑user `recency_days, frequency, monetary, R, F, M, RFM`

RFM uses quantile‑based cutoffs. Recency is inverted (lower days = higher score).

### RFM — What it means

* **Recency**: days since a user’s last purchase (lower is better).
* **Frequency**: number of orders a user placed in the dataset (higher is better).
* **Monetary**: total amount the user spent in the dataset (higher is better).

**Scoring approach used here:** quantile cutoffs at \~20/40/60/80 percentiles to map each metric to a 1–5 score. Recency is inverted so more recent activity yields a higher R score. The final code is concatenated as `RFM` (e.g., `545`), and the heatmap shows average monetary value across R×F cells.

### Using your own CSV data

You can run the dashboard on your own orders file. Prepare a CSV with the required columns below and place it at `data/raw/orders.csv` (overwrite the sample), then re‑run the ETL and dashboard.

**Required columns**

* `order_id` (integer/long)
* `user_id` (integer)
* `ts` (timestamp in ISO‑8601, e.g., `2025-08-01T14:23:00`)
* `amount` (numeric; positive)
* `category` (string)

**Optional columns**

* `used_promo` (0/1)
* `payment_type` (e.g., `CARD`, `UPI`, `WALLET`, `COD`)

**Example**

```csv
order_id,user_id,ts,amount,category,used_promo,payment_type
1,101,2025-08-01T14:23:00,1299.00,Electronics,1,CARD
```

**Run with your file**

```bash
python etl.py
streamlit run app.py
```

**Changing the input path (optional)**
If you prefer a different filename or location, edit this line in `etl.py`:

```python
# etl.py
from pathlib import Path
RAW = Path("data/raw/orders.csv")  # change this to your CSV path
```

**Notes**

* Keep data sizes small (tens of MB) for smooth laptop runs.
* Ensure timestamps are parseable; invalid rows are dropped.
* Extra columns are ignored by this project unless you extend the ETL.

---

## Dashboard Sections

1. KPI header — Revenue, Orders, AOV, Active Users
2. Revenue by Day — line chart with date range filter
3. Category Share — donut chart; Category Trend — multi‑line by day
4. RFM Heatmap — average monetary by R × F
5. Top Customers — table sorted by monetary value

## How to Read the Dashboard

### KPI Header

* **Revenue**: total `amount` within the selected date range and categories.
* **Orders**: count of orders in the selection.
* **AOV (Average Order Value)**: `Revenue ÷ Orders`.
* **Active Users (daily sum)**: sum of *per‑day* distinct users. It indicates day‑level activity across the period, not the unique users across the whole range. If you need that, add a separate "unique users across range" metric.

**What to look for:** spikes after campaigns, dips on specific weekdays, and changes in AOV that may signal discounting or mix shifts (e.g., higher‑priced categories dominating).

### Revenue by Day (line chart)

* **X‑axis**: date, **Y‑axis**: revenue.
* Use the **date range** filter to zoom into weeks or months.
* Hover to see exact values. Look for **seasonality** (weekday vs weekend), **trend** (upward/downward), and **outliers** (unusually high/low days).

**Inference tips:**

* Rising revenue with flat orders → AOV is increasing (higher prices or larger baskets).
* Flat revenue with rising orders → AOV is shrinking (discounting or low‑priced items).

### Category Share (donut chart)

* Each slice shows a category’s **percentage of total revenue** for the current filters.
* Use it to identify dominant categories vs. long‑tail categories.

**How to read:**

* Bigger slice = higher contribution. Compare before/after changing the date range or toggling categories to see mix shifts.

### Category Trend (multi‑line chart)

* Lines show **revenue by day per category**.
* Watch for **crossovers** (one category overtakes another) and **sustained growth/decline**.

**Inference tips:**

* A sudden spike in one category paired with higher overall AOV suggests premium items or successful upsell.
* If share is high in the donut but the trend is volatile, reliance on promos may be high.

### RFM: Raw Metrics vs. Scores

* **Raw metrics** in data: `recency_days`, `frequency`, `monetary`.
* **Scores** in dashboard: `R`, `F`, `M` (each 1–5 via quantiles). Keeping **both** provides transparency (exact values) and comparability (bucketed scores).

**How to interpret:**

* **R (Recency)**: lower `recency_days` → higher R score. Recently active users.
* **F (Frequency)**: more orders → higher F score. Habitual buyers.
* **M (Monetary)**: more spend → higher M score. High‑value buyers.

**Heatmap (R × F with Avg Monetary):**

* Rows = R, columns = F. Darker cells indicate higher average monetary value.
* Top‑right cells (R=5, F=5) are best: **recent and frequent**.
* High F but low R → previously loyal, now at risk; consider win‑back.
* High R but low F → **new or reactivated** users; consider nurture sequences.

### Top Customers (table)

* Sorted by `Monetary` by default, with R/F/M shown for quick context.
* Use it to spot accounts worth follow‑up, and to compare their RFM profiles.

---

## Dashboard preview 

![Streamlit dashboard](/gifs/salespulse.gif)


## Troubleshooting

* If Spark/Java is not found: install Java 11+ and restart the terminal.
* If PyArrow errors occur: upgrade pyarrow and pandas.
* If the dashboard is empty: re‑run `python etl.py` to regenerate `data/curated`.
