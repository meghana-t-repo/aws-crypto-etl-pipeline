import json
import time
import random
from datetime import datetime
import boto3

# ---- CONFIG ----
BUCKET_NAME = "crypto-etl-meghana"   # your bucket
RAW_PREFIX = "raw/"
AWS_REGION = "us-east-2"             # Ohio
NUM_RECORDS = 5                      # how many JSON files to create
SLEEP_SECONDS = 1                    # delay between files (optional)

print("Starting OFFLINE crypto ETL script...")

# ---- SETUP AWS S3 CLIENT ----
s3 = boto3.client("s3", region_name=AWS_REGION)

def fake_crypto_price():
    """Simulate API response structure for BTC prices in USD/GBP/EUR."""
    now_iso = datetime.utcnow().isoformat()

    base_price = 60000  # base BTC price
    # add some random noise
    price_usd = base_price + random.uniform(-2000, 2000)
    price_gbp = price_usd * 0.8
    price_eur = price_usd * 0.9

    data = {
        "time": {
            "updatedISO": now_iso,
        },
        "bpi": {
            "USD": {"code": "USD", "rate_float": price_usd},
            "GBP": {"code": "GBP", "rate_float": price_gbp},
            "EUR": {"code": "EUR", "rate_float": price_eur},
        },
        "fetched_at_utc": now_iso,
    }
    return data

def upload_json_to_s3(obj, key):
    """Upload a JSON object to S3 as a file."""
    body = json.dumps(obj)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Uploaded: s3://{BUCKET_NAME}/{key}")

def main():
    print(f"Will write {NUM_RECORDS} simulated records to bucket {BUCKET_NAME}...")
    for i in range(NUM_RECORDS):
        data = fake_crypto_price()
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"{RAW_PREFIX}crypto_price_{ts}_{i}.json"
        upload_json_to_s3(data, key)
        if i < NUM_RECORDS - 1:
            print(f"Sleeping {SLEEP_SECONDS} seconds...")
            time.sleep(SLEEP_SECONDS)
    print("Done.")

if __name__ == "__main__":
    main()
