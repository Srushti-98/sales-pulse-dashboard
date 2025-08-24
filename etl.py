# etl.py
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

RAW = Path("data/raw/orders.csv")
CURATED = Path("data/curated")
CURATED.mkdir(parents=True, exist_ok=True)

def build_spark():
    return (SparkSession.builder
            .appName("SalesPulse-ETL")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())

def read_orders(spark):
    schema = T.StructType([
        T.StructField("order_id", T.LongType(), False),
        T.StructField("user_id", T.IntegerType(), False),
        T.StructField("ts", T.StringType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("category", T.StringType(), True),
        T.StructField("used_promo", T.IntegerType(), True),
        T.StructField("payment_type", T.StringType(), True),
    ])
    df = (spark.read
          .option("header", True)
          .schema(schema)
          .csv(str(RAW)))
    df = (df
          .withColumn("ts", F.to_timestamp("ts"))
          .withColumn("amount", F.col("amount").cast("double"))
          .filter(F.col("amount") > 0)
          .dropna(subset=["ts","amount","user_id","order_id"]))
    return df

def write(df, name):
    (df.coalesce(1)
       .write.mode("overwrite").parquet(str(CURATED / f"{name}.parquet")))

def build_daily_kpis(df):
    daily = (df
        .withColumn("date", F.to_date("ts"))
        .groupBy("date")
        .agg(
            F.sum("amount").alias("revenue"),
            F.count("*").alias("orders"),
            F.countDistinct("user_id").alias("active_users")
        )
        .withColumn("aov", F.round(F.col("revenue")/F.col("orders"), 2))
        .orderBy("date"))
    return daily

def build_category_daily(df):
    cat_daily = (df
        .withColumn("date", F.to_date("ts"))
        .groupBy("date","category")
        .agg(F.sum("amount").alias("revenue"),
             F.count("*").alias("orders"))
        .orderBy("date","category"))
    return cat_daily

def build_rfm(df):
    # recency measured vs. data max date
    max_ts = df.agg(F.max("ts").alias("max_ts")).collect()[0]["max_ts"]
    max_date = F.lit(max_ts)

    per_user = (df.groupBy("user_id")
      .agg(
        F.max("ts").alias("last_ts"),
        F.count("*").alias("frequency"),
        F.sum("amount").alias("monetary")
      )
      .withColumn("recency_days", F.greatest(F.lit(0.0),
         F.round((F.unix_timestamp(max_date) - F.unix_timestamp("last_ts")) / F.lit(86400.0), 2)))
    )

    # quantile cutoffs for scoring (20/40/60/80)
    rec_cuts = per_user.approxQuantile("recency_days", [0.2,0.4,0.6,0.8], 0.01)
    freq_cuts = per_user.approxQuantile("frequency",    [0.2,0.4,0.6,0.8], 0.01)
    mon_cuts  = per_user.approxQuantile("monetary",     [0.2,0.4,0.6,0.8], 0.01)

    def score_value(v, cuts, higher_is_better=True):
        if v is None: return 1
        # bins: (-inf, c0], (c0,c1], (c1,c2], (c2,c3], (c3, inf)
        if v <= cuts[0]: s = 1
        elif v <= cuts[1]: s = 2
        elif v <= cuts[2]: s = 3
        elif v <= cuts[3]: s = 4
        else: s = 5
        return s if higher_is_better else 6 - s

    score_udf_rec = F.udf(lambda v: score_value(v, rec_cuts, higher_is_better=False), T.IntegerType())
    score_udf_hib = F.udf(lambda v, cuts: score_value(v, cuts, True), T.IntegerType())

    # We can’t pass lists directly to UDF easily twice; define per column UDFs
    score_udf_freq = F.udf(lambda v: score_value(v, freq_cuts, True), T.IntegerType())
    score_udf_mon  = F.udf(lambda v: score_value(v, mon_cuts,  True), T.IntegerType())

    scored = (per_user
              .withColumn("R", score_udf_rec(F.col("recency_days")))
              .withColumn("F", score_udf_freq(F.col("frequency")))
              .withColumn("M", score_udf_mon(F.col("monetary")))
              .withColumn("RFM", F.concat_ws("", F.col("R"), F.col("F"), F.col("M")))
              )
    return scored

def main():
    if not RAW.exists():
        raise SystemExit(f"Missing {RAW}. Run: python gen_orders.py")

    spark = build_spark()

    orders = read_orders(spark)
    print(f"Orders: {orders.count():,}")

    daily = build_daily_kpis(orders)
    cat_daily = build_category_daily(orders)
    rfm = build_rfm(orders)

    # Save curated outputs
    write(orders, "orders_clean")
    write(daily, "kpis_by_day")
    write(cat_daily, "category_daily")
    write(rfm, "rfm_scores")

    print("✅ Wrote parquet to data/curated")
    print("📸 Screenshots to take later: Spark UI > SQL DAG (during this run)")

    spark.stop()

if __name__ == "__main__":
    main()
