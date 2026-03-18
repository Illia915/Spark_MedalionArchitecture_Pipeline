from pathlib import Path

import pyspark.sql.functions as F 
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .getOrCreate()
)


SPARK_APP = Path('/opt/spark-app')
SPARK_DATA = Path('/opt/spark-data')
silver_file_path = SPARK_DATA / f"silver/*.parquet"
gold_base_path = SPARK_DATA / f"gold"

silver_df = (
    spark.read
    .parquet(str(silver_file_path))
)


silver_df = silver_df.withColumn(
    "total_amount", 
    F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100), 2)
)


daily_sales_df = (
    silver_df.groupBy("order_date")
    .agg(
        F.round(F.sum(F.col("total_amount")),2).alias("total_revenue"),
        F.count(F.col("transaction_id")).alias("orders_amount"),
        F.round(F.avg(F.col("total_amount")),2).alias("avg_order_value")
    )
)

daily_sales_df.write.mode("overwrite").parquet(str(gold_base_path / f"daily_sales_metrics.parquet"))


product_performance_df = (
    silver_df.groupBy("product_category")
    .agg(
        F.sum(F.col("quantity")).alias("category_units_bought"),
        F.round(F.sum(F.col("total_amount")),2).alias("category_revenue"),
        F.round(F.avg(F.col("customer_age")),2).alias("avg_customer_age"),
        F.count(F.col("transaction_id")).alias("orders_count")
    )
)

product_performance_df.write.mode("overwrite").parquet(str(gold_base_path / f"product_performance.parquet"))

city_revenue_df = silver_df.withColumn(
    "delivery_time", 
    F.when(
        F.col("order_date").isNotNull() & F.col("ship_date").isNotNull(), 
        F.datediff(F.col("ship_date"), F.col("order_date"))
    ).otherwise(None)
)

city_revenue_analyz_df = (
    city_revenue_df.groupBy("city")
    .agg(
        F.count(F.col("transaction_id")).alias("orders_count"),
        F.round(F.sum(F.col("total_amount")),2).alias("city_revenue"),
        F.round(F.avg(F.col("delivery_time")),2).alias("avg_delivery_time"),
        F.round(F.avg(F.col("total_amount")),2).alias("avg_order_value")
    )
)


city_revenue_analyz_df.write.mode("overwrite").parquet(str(gold_base_path / f"city_revenue.parquet"))


daily_sales_df.limit(5).show()
product_performance_df.limit(5).show()
city_revenue_analyz_df.limit(5).show()


"""
IDEAS FOR GOLD LAYER 
1. Check for delivery time by each month in year and compare 
2. Top 5 order cities 
3. TOP 5 prdct categories
4.  
"""
