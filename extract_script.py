import requests
import json
import pandas as pd
from datetime import datetime
import os

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

if __name__ == "__main__":
    extract_crypto_prices()