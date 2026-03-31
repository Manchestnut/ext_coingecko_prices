import requests
import json
import pandas as pd
from datetime import datetime
import os
import snowflake.connector

# Coingecko extraction logic
def extract_crypto_prices():
    URL = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        'ids': 'bitcoin,ethereum,solana',
        'vs_currencies': 'usd',
        'include_last_updated_at': 'true'
    }

    try:
        response = requests.get(URL, params=params)
        response.raise_for_status()
        data = response.json()

        records = []
        for coin_id, values in data.items():
            records.append({
                'coin_id': coin_id,
                'price_usd': values['usd'],
                'updated_at_unix': values['last_updated_at'],
                'extracted_at': datetime.now().isoformat()
            })

        # Save to a JSON file that we will later upload to Snowflake
        with open("crypto_data.json", "w") as f:
            json.dump(records, f)
            
        print(f"Successfully extracted {len(records)} coins to crypto_data.json")

    except Exception as e:
        print(f"Extraction failed: {e}")

# Snowflake connection
def load_to_snowflake():
    print("Connecting to Snowflake...")
    # FIX #2: Initialize variables as None so the 'finally' block doesn't crash
    ctx = None
    cs = None
    
    try:
        ctx = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse='COMPUTE_WH',
            database='CRYPTO_DB',
            schema='RAW'
        )
        cs = ctx.cursor()

        print("Uploading file to Snowflake Stage...")
        cs.execute("PUT file://crypto_data.json @crypto_stage AUTO_COMPRESS=TRUE")

        print("Copying data into stg_coin_prices...")
        cs.execute("""
            COPY INTO stg_coin_prices (raw_data)
            FROM @crypto_stage
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
            PURGE = TRUE
            """)

        print("Load Successful!")

    except Exception as e:
        print(f"Loading failed: {e}")
    finally:
        # Only try to close them if they were actually opened!
        if cs:
            cs.close()
        if ctx:
            ctx.close()

if __name__ == "__main__":
    # 1. Run the extraction
    extract_crypto_prices()
    
    # 2. Run the Snowflake load
    load_to_snowflake()