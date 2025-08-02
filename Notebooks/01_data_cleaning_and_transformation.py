# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # ---------------------------------------------
# MAGIC # Notebook 01: Data Cleaning and Transformation
# MAGIC # ---------------------------------------------

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/tables/")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,dayofweek,month,year,when

spark=SparkSession.builder.appName("bank_customer").getOrCreate()
# 1. Load CSV
df=spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/tables/bank_transactions_1L.csv")

df.printSchema()
df.display(10)

# COMMAND ----------

# 2. Clean and Cast
df=df.dropna(subset=["CUSTOMER_ID", "TXN_DATE", "AMOUNT", "TXN_TYPE"])

#Cast Data Types
df=df.withColumn("AMOUNT",col("AMOUNT").cast("double"))\
    .withColumn("LOAN_AMOUNT",col("LOAN_AMOUNT").cast("double"))\
        .withColumn("LOAN_BALANCE",col("LOAN_BALANCE").cast("double"))\
            .withColumn("IS_CREDITED",col("IS_CREDITED").cast("boolean"))\
                .withColumn("EMI_FLAG",col("EMI_FLAG").cast("boolean"))\
                    .withColumn("LOAN_ACCOUNT",col("LOAN_ACCOUNT").cast("boolean"))\
                        .withColumn("IS_INTERNATIONAL",col("IS_INTERNATIONAL").cast("boolean"))

df.display(10)


# COMMAND ----------

# 3. Feature Engineering
df=df.withColumn("Txn_day",dayofweek(col("TXN_DATE")))\
    .withColumn("Txn_month",month(col("TXN_DATE")))\
        .withColumn("Txn_year",year(col("TXN_DATE")))\
            .withColumn("HIGH_VALUE_TXN",when(col("Amount")>60000,True).otherwise(False))\
                .withColumn("Txn_group",when(col("TXN_TYPE").isin("EMI", "Loan_Repayment"), "LOAN")\
                    .when(col("TXN_TYPE")=='Credit',"CREDIT")\
                        .when(col("TXN_TYPE")=='Debit',"DEBIT")\
                            .otherwise("OTHER"))
df.select("TRANSACTION_ID", "AMOUNT", "HIGH_VALUE_TXN", "TXN_GROUP", "TXN_MONTH","Txn_day","Txn_year").display()


# COMMAND ----------

df.display(10)

# COMMAND ----------

# 4. Save Cleaned Data
df.write.mode("overwrite").partitionBy("TXN_TYPE").parquet("/FileStore/tables/bank_transactions_cleaned")
print("✅ Cleaned and transformed data saved.")