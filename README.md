AWS Crypto ETL – S3 -> Glue -> Athena

End-to-end data engineering pipeline built on AWS using S3, AWS Glue (PySpark), and Amazon Athena.
This project simulates crypto price ingestion, transforms raw JSON into optimized Parquet files, and queries the results using Athena.

Architecture Overview

1. Local Python Ingestion

Script: extract_crypto_to_s3.py

Generates simulated crypto (Bitcoin) price snapshots in JSON format.

Uploads raw JSON files to:

s3://crypto-etl-meghana/raw/

2. AWS Glue ETL (PySpark)

Glue job: crypto_raw_to_clean

Reads raw JSON from S3 → flattens + cleans the data

Extracts:

fetched_at_utc, api_time_utc

currencies and their prices (USD, GBP, EUR)

Writes cleaned output as Parquet to:

s3://crypto-etl-meghana/clean/crypto_prices/

3. Athena + Glue Data Catalog

Glue database: crypto_etl_db

External Athena table:

CREATE EXTERNAL TABLE IF NOT EXISTS crypto_etl_db.crypto_prices (
    fetched_at_utc string,
    api_time_utc string,
    currency_usd string,
    price_usd double,
    currency_gbp string,
    price_gbp double,
    currency_eur string,
    price_eur double
)
STORED AS PARQUET
LOCATION 's3://crypto-etl-meghana/clean/crypto_prices/';


Example query:

SELECT fetched_at_utc, price_usd
FROM crypto_etl_db.crypto_prices;

How to Run the Project-
1️. Install dependencies
pip install -r requirements.txt

2️. Configure AWS credentials
aws configure


Enter:

AWS Access Key

AWS Secret Key

Region (e.g., us-east-2)

Output: json

⚠️ Do NOT commit your credentials.

3️. Run ingestion script
python extract_crypto_to_s3.py


This uploads raw JSON snapshots to:

s3://crypto-etl-meghana/raw/

4️. Run AWS Glue job

From AWS Console → Glue → ETL Jobs → crypto_raw_to_clean → Run

This creates Parquet files in:

s3://crypto-etl-meghana/clean/crypto_prices/

5️. Query using Athena

Open Amazon Athena -> Query Editor

Create or repair schema:

MSCK REPAIR TABLE crypto_etl_db.crypto_prices;


Query your data:

SELECT * FROM crypto_etl_db.crypto_prices LIMIT 10;


Project Structure--
aws-crypto-etl/
│
├── extract_crypto_to_s3.py
├── glue_crypto_raw_to_clean.py
├── requirements.txt
├── README.md
│
├── architecture.png
├── raw_json_s3.png
├── clean_parquet_s3.png
├── glue_job_success.png
└── athena_results.csv