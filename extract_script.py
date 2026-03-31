import requests
import json
import pandas as pd
from datetime import datetime
import os

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
    try:
        # 1. Connect using the Environment Variables from your YAML
        ctx = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse='COMPUTE_WH', # Adjust if yours is named differently
            database='CRYPTO_DB',
            schema='RAW'
        )
        cs = ctx.cursor()

        # 2. Upload the file to the Stage (The Loading Dock)
        # We use 'file://' to tell Snowflake the file is on the local runner disk
        print("Uploading file to Snowflake Stage...")
        cs.execute("PUT file://crypto_data.json @crypto_stage AUTO_COMPRESS=TRUE")

        # 3. Copy from Stage into the Table (The Forklift)
        print("Copying data into stg_coin_prices...")
        cs.execute("""
            COPY INTO stg_coin_prices 
            FROM @crypto_stage 
            FILE_FORMAT = (TYPE = 'JSON')
            PURGE = TRUE; 
        """)
        # PURGE = TRUE cleans up the stage after the load is done!

        print("Load Successful!")

    except Exception as e:
        print(f"Loading failed: {e}")
    finally:
        cs.close()
        ctx.close()

if __name__ == "__main__":
    if extract_crypto_prices():
        load_to_snowflake()