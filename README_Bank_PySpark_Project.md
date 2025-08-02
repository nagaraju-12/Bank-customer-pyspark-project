
# 🏦 Bank Customer Transaction Insights Using PySpark

This is a mini project demonstrating real-world Data Engineering tasks using PySpark on the Databricks platform. The goal is to simulate data ingestion, cleaning, transformation, and business reporting on 1,00,000 synthetic bank transaction records.

---

## 📁 Project Structure

```
bank-customer-pyspark-project/
├── data/
│   └── bank_transactions_1L.csv
├── notebooks/
│   └── 01_data_cleaning_and_transformation.ipynb
│   └── 02_transaction_analytics.ipynb
├── output/
│   └── parquet/
│       ├── spend_per_customer/
│       ├── top_5_debit_customers/
│       ├── monthly_emi_avg/
│       ├── emi_user_count/
│       └── city_avg_txn/
├── README.md
```

---

## 📊 Project Flow

### ✅ 1. Load Data (from CSV to DataFrame)
- Read the CSV file using PySpark
- Infer schema and display sample records

### ✅ 2. Clean Nulls / Cast Data Types
- Drop rows with critical null values
- Cast all numeric and boolean fields properly

### ✅ 3. Feature Engineering
- Add the following:
  - `HIGH_VALUE_TXN`: Boolean flag if amount > ₹50,000
  - `TXN_GROUP`: Categorize into DEBIT, CREDIT, LOAN, OTHER
  - Extract `TXN_YEAR`, `TXN_MONTH`, `TXN_DAY` from TXN_DATE

### ✅ 4. Aggregations
- Total spending per customer
- Monthly average EMI, Loan Repayment, and Bill Payment amounts

### ✅ 5. Save to Parquet
- Save the cleaned data partitioned by `TXN_TYPE`
- Output summaries saved in respective folders

### ✅ 6. Basic Reporting
- Top 5 customers by total **debit** amount
- EMI vs Non-EMI customer counts
- City-wise average transaction value

---

## 📦 Technologies Used
- 🐍 PySpark
- 🧱 Databricks Community Edition
- 💾 Parquet Storage Format
- 🐘 Spark SQL
- 📝 Python (for logic & UDFs)

---

## 📁 Output
All processed files are saved in Parquet under:

```
output/parquet/
├── spend_per_customer/
├── top_5_debit_customers/
├── monthly_emi_avg/
├── emi_user_count/
└── city_avg_txn/
```

---

## 🚀 How to Run
1. Clone this repo
2. Upload `bank_transactions_1L.csv` to Databricks `/FileStore`
3. Run both notebooks in order:
   - `01_data_cleaning_and_transformation.ipynb`
   - `02_transaction_analytics.ipynb`
4. Explore Parquet outputs and verify results

---

## ✍️ Author
**Nagaraju Chiluveru**  
Aspiring Data Engineer | SQL | PySpark | ETL Developer  
[GitHub](https://github.com/nagaraju-12) | [LinkedIn](https://www.linkedin.com/in/nagaraju-chiluveru-327711236/)

---

## 🏁 License
Free to use for learning and portfolio-building purposes.
