README.md
# PySpark Production-Style Data Pipeline

This project implements a modular **PySpark batch data pipeline** that ingests, cleans, and processes synthetic financial transaction data.  
It demonstrates a realistic **data engineering workflow**, including data ingestion, transformation, and distributed output using Apache Spark.

The pipeline is designed using a **production-style architecture** with clear separation of ingestion, transformation, pipeline orchestration, and output layers.

---

## Project Overview

The pipeline performs the following tasks:

1. **Synthetic Data Generation**
   - Generates realistic transaction data using Python.
   - Includes transaction amount, currency, channel, country, and fraud indicator.

2. **Data Ingestion**
   - Loads CSV data using PySpark.

3. **Data Transformation**
   - Cleans and prepares the dataset.
   - Extracts a `year` feature from the transaction timestamp.

4. **Distributed Output**
   - Writes processed data using Spark to a structured output directory.

```

## Project Structure

pyspark-production-data-pipeline
│
├── data
│ ├── raw
│ │ └── synthetic_transactions.csv
│ └── processed
│
├── scripts
│ └── generate_synthetic_data.py
│
├── src
│ ├── ingestion
│ │ └── read_data.py
│ │
│ ├── transformations
│ │ └── clean_transform.py
│ │
│ ├── output
│ │ └── write_data.py
│ │
│ ├── pipelines
│ │ └── batch_pipeline.py
│ │
│ └── utils
│ └── spark_session.py
│
├── run_pipeline.py
├── .gitignore
└── README.md

```

## Technologies Used

- **Python**
- **PySpark**
- **Pandas / NumPy**
- **WSL (Linux environment)**
- **Apache Spark Distributed Processing**

---

## Running the Pipeline
Generate synthetic data and run the pipeline:
make run

Clean generated output:
make clean

### Environment Note
This project was executed successfully in WSL/Ubuntu with Java 17 and PySpark 4.1.1.

### 1. Clone the repository

```bash
git clone <repo-url>
cd pyspark-production-data-pipeline
2. Create a Python environment
python3 -m venv pyspark-venv
source pyspark-venv/bin/activate
3. Install dependencies
pip install -r requirements.txt
4. Generate synthetic data
python scripts/generate_synthetic_data.py
5. Run the pipeline
python run_pipeline.py

Example Output
After execution, the processed data will be written to:
data/processed/transactions_csv
Example contents:
_SUCCESS
part-00000-xxxxxxxx.csv
This indicates that the Spark job completed successfully.

Example Dataset Fields
Column	Description
transaction_id	Unique transaction identifier
customer_id	Unique customer identifier
transaction_date	Timestamp of transaction
amount	Transaction amount
currency	Currency used
channel	Transaction channel (mobile/web/branch)
country	Transaction country
is_fraud	Fraud indicator (0/1)
year	Extracted year for partitioning

Key Learning Objectives
This project demonstrates:
•	Modular PySpark pipeline architecture
•	Data ingestion and transformation workflows
•	SparkSession configuration
•	Distributed data processing with Apache Spark
•	Structured output management
•	Debugging Spark runtime environments (WSL + Java)

Future Improvements
Potential enhancements include:
•	Partitioned data output by year
•	Apache Airflow orchestration
•	Data validation and schema enforcement
•	Integration with cloud object storage (S3 / Azure Data Lake)
•	Streaming ingestion with Spark Structured Streaming

Author
Walter Roye Taju Fanka
Data Scientist | Data Engineering | Analytics
GitHub: https://github.com/fankaroy
