# gen_orders.py
from pathlib import Path
import pandas as pd
import numpy as np

RAW = Path("data/raw")
RAW.mkdir(parents=True, exist_ok=True)
out = RAW / "orders.csv"

np.random.seed(42)

# 90 days of synthetic orders across ~1.2k users
days = pd.date_range(end=pd.Timestamp.today().normalize(), periods=90, freq="D")
users = np.arange(1001, 2201)  # ~1200 users
categories = ["Electronics", "Beauty", "Fashion", "Grocery", "Sports", "Books", "Home"]
methods = ["CARD", "UPI", "WALLET", "COD"]

rows = []
order_id = 1
for d in days:
    # orders per day (weekday/weekend seasonality)
    base = 450 if d.weekday() < 5 else 650
    n = int(np.random.normal(loc=base, scale=60))
    n = max(n, 200)

    user_sample = np.random.choice(users, size=n, replace=True)
    cat_sample = np.random.choice(categories, size=n, p=[0.20,0.12,0.22,0.18,0.10,0.08,0.10])
    pay_sample = np.random.choice(methods, size=n, p=[0.55,0.25,0.15,0.05])

    # price distribution per category
    cat_price = {
        "Electronics": (2500, 9000),
        "Beauty":      (300, 1200),
        "Fashion":     (700, 2500),
        "Grocery":     (200, 900),
        "Sports":      (600, 3000),
        "Books":       (250, 1200),
        "Home":        (400, 2500)
    }

    # order time spread during the day
    seconds = np.random.randint(8*3600, 22*3600, size=n)  # 08:00–22:00
    tss = [d + pd.Timedelta(int(s), "s") for s in seconds]

    for i in range(n):
        cat = cat_sample[i]
        low, high = cat_price[cat]
        amount = float(np.round(np.random.gamma(shape=2.0, scale=(high-low)/6.0) + low, 2))
        amount = max(amount, 50.0)
        rows.append([
            order_id,
            int(user_sample[i]),
            tss[i].isoformat(),
            amount,
            cat,
            np.random.choice([0,1], p=[0.7,0.3]),  # promo flag
            pay_sample[i],
        ])
        order_id += 1

df = pd.DataFrame(rows, columns=[
    "order_id","user_id","ts","amount","category","used_promo","payment_type"
])
df.to_csv(out, index=False)
print(f"Wrote {out} with {len(df):,} rows.")
