"""
DATA QUALITY & EDA CHECKLIST
----------------------------
01. Duplicate IDs    : Keep only the record with the earliest 'order_date'.
02. Date Logic       : Identify records where 'ship_date' < 'order_date'.
03. Gender Format    : Standardize gender values into a single format.
04. Category Check   : Audit unique product categories.
05. Quantity Check   : Flag records where quantity <= 0.
06. Price Check      : Handle invalid or null unit prices.
07. Discount Check   : Flag discounts outside the 0-100% range.
08. Null Handling    : Define strategy for missing values (Impute/Drop).
09. Age Validation   : Flag age anomalies (Age <= 0 or > 110).
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- 1. Remove Duplicates (Keep earliest order_date) ---
window_spec = Window.partitionBy("transaction_id").orderBy("order_date")

bronze_df_unique = (
    bronze_df.withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

df_date_errors = bronze_df.filter(F.col("order_date") > F.col("ship_date"))

df_gender_audit = bronze_df.groupBy("gender").count()

df_quantity_errors = bronze_df.filter(F.col("quantity") <= 0)

df_discount_errors = bronze_df.filter(
    (F.col("discount_pct") > 100) | (F.col("discount_pct") < 0)
)

df_age_errors = bronze_df.filter(
    (F.col("age") <= 0) | (F.col("age") > 110)
)

print(f"Date Errors: {df_date_errors.count()}")
print(f"Quantity Errors: {df_quantity_errors.count()}")
print(f"Discount Errors: {df_discount_errors.count()}")