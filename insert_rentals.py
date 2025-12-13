import pandas as pd
import sqlite3
import os
import re

# config
DB_NAME = 'rentals.db'
CSV_PATH = 'rentals.csv'

# existed funcs modified
def clean_zip_code(zip_code_str):
    """Cleans and validates the zip code field."""
    if pd.isna(zip_code_str):
        return None
    # Assuming the zip code is the last 5 digits if it's not a clear string
    match = re.search(r'(\d{5})', str(zip_code_str))
    return match.group(1) if match else None

def insert_data_into_db(df):
    """
    Connects to SQLite and performs row-by-row insertion using 
    'INSERT OR REPLACE' to prevent crashing on duplicate listings.
    """

    print(f"\nAttempting to insert {len(df)} records into {DB_NAME}.")
    con = sqlite3.connect(DB_NAME)
    cur = con.cursor()


    # Map the DataFrame columns to the database schema
    df_to_insert = df.rename(columns={
        'pid': 'post_id',
        'hood': 'neighborhood',
        'beds': 'bedrooms',
        'zip code': 'zip_code',
        'description': 'full_description',
    })
    # Final preparation (ensure required columns exist and are correct types)
    df_to_insert = df_to_insert.dropna(subset=['post_id', 'price'])

    print("Count non-null:", df_to_insert['neighborhood'].notna().sum())
    print("Count null:", df_to_insert['neighborhood'].isna().sum())

    # iterate through the DataFrame and apply the SQL fix to each row
    for index, row in df_to_insert.iterrows():
        try:
            cur.execute("""
                        INSERT
                        OR REPLACE INTO rentals (
                    post_id,
                    price,
                    bedrooms,
                    bathrooms,
                    sqft,
                    zip_code,
                    neighborhood,
                    full_description
                )
                VALUES (
                    :post_id,
                    :price,
                    :bedrooms,
                    :bathrooms,
                    :sqft,
                    :zip_code,
                    :neighborhood,
                    :full_description
                )
                        """, {
                            "post_id": row["post_id"],
                            "price": row["price"],
                            "bedrooms": row["bedrooms"],
                            "bathrooms": row["bathrooms"],
                            "sqft": row["sqft"],
                            "zip_code": row["zip_code"],
                            "neighborhood": row["neighborhood"],
                            "full_description": row["full_description"],
                        })


        except Exception as e:
            # If the error is not the UNIQUE constraint
            print(f"ERROR: Failed to insert PID {row['post_id']} due to: {e}")

    con.commit()
    con.close()

    print(f"Insertion complete. Database is updated.")

def main():
    df_merged = pd.read_csv(CSV_PATH)

    df_merged['zip code'] = df_merged['zip code'].apply(clean_zip_code)
    insert_data_into_db(df_merged)


if __name__ == '__main__':
    main()
