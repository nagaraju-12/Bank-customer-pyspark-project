# Databricks notebook source
# MAGIC %md
# MAGIC ---------------------------------------------
# MAGIC Notebook 02: business reporting + summary analytics on the cleaned data you saved in Parquet.
# MAGIC ---------------------------------------------
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, countDistinct

# Load cleaned data from Parquet
df = spark.read.parquet("/FileStore/tables/bank_transactions_cleaned")

df.printSchema()
df.select("CUSTOMER_ID", "AMOUNT", "TXN_TYPE", "EMI_FLAG").display()


# COMMAND ----------

# MAGIC %md
# MAGIC 📊 Business Analytics

# COMMAND ----------

#Total Spending Per Customer
from pyspark.sql.functions import sum
spend_df=df.groupBy("CUSTOMER_ID").agg(sum("Amount").alias("Total_spent")).orderBy("Total_spent",ascending=False)
spend_df.display()

# COMMAND ----------

#save
spend_df.write.mode("overwrite").parquet("/FileStore/tables/spend_per_customer")


# COMMAND ----------

#Top 5 Customers by Debit
top_debit_df=df.filter(df.TXN_TYPE=='Debit').groupBy("CUSTOMER_ID").agg(sum("Amount").alias("TOTAL_DEBIT")).orderBy("TOTAL_DEBIT",ascending=False).limit(5)
top_debit_df.display()

# COMMAND ----------

top_debit_df.write.mode("overwrite").parquet("/FileStore/tables/top_5_debit_customers")


# COMMAND ----------

#Monthly Average EMI, Bills, Credit Usage
monthly_emi_df = df.filter(df.TXN_TYPE.isin("EMI", "Loan_Repayment", "Bill_Payment")) \
                   .groupBy("TXN_MONTH", "TXN_TYPE") \
                   .agg(avg("AMOUNT").alias("AVG_AMOUNT")) \
                   .orderBy("TXN_MONTH", "TXN_TYPE")

monthly_emi_df.display()


# COMMAND ----------

monthly_emi_df.write.mode("overwrite").parquet("/FileStore/tables/monthly_emi_avg")


# COMMAND ----------

# EMI vs Non-EMI User Count
emi_user_count = df.groupBy("EMI_FLAG") \
                   .agg(countDistinct("CUSTOMER_ID").alias("UNIQUE_USERS"))

emi_user_count.display()


# COMMAND ----------

emi_user_count.write.mode("overwrite").parquet("/FileStore/tables/emi_user_count")


# COMMAND ----------

#City-wise Avg Transaction Amount
city_avg_amt = df.groupBy("CITY") \
                 .agg(avg("AMOUNT").alias("AVG_TXN_AMOUNT")) \
                 .orderBy("AVG_TXN_AMOUNT", ascending=False)

city_avg_amt.display()


# COMMAND ----------

city_avg_amt.write.mode("overwrite").parquet("/FileStore/tables/city_avg_txn")


# COMMAND ----------

