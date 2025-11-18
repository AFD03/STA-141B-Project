import pandas as pd
from sodapy import Socrata
import sqlite3

# note: possible warning "NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'"

def fetch_crime_data():
    # confirmation of fetching data
    print("Fetching crime data for DataSF")
    client = Socrata('data.sfgov.org', None)

    # police incidents (2018-present) id
    dataset_id = 'wg3w-h783'

    # query for num of incidents, grouped by zip code, for 2025
    sql_query = """
            SELECT
                incident_zipcode,
                COUNT(*) AS crime_count_2025
            WHERE
                incident_date >= '2025-01-01T00:00:00'
            GROUP BY
                incident_zipcode
        """

    results = client.get(dataset_id, query = sql_query)
    crime_df = pd.DataFrame.from_records(results) # creating dataframe for crime

    # clean data and rename
    crime_df = crime_df.rename(columns = {'incident_zipcode': 'zip_code'})
    crime_df = crime_df.dropna(subset = ['zip_code'])
    crime_df['crime_count_2025'] = pd.to_numeric(crime_df['crime_count_2025'])

    # verify number of zipcodes were we find crime incidents
    print(f"Found crime data for {len(crime_df)} zip codes.")

    # store in the rentals database
    conn = sqlite3.connect('rentals.db')

    crime_df.to_sql(
        'neighborhood_data',
        conn,
        if_exists = 'replace', # reload the table each time
        index = False,
        dtype = {'zip_code': 'TEXT PRIMARY KEY', 'crime_count_2025': 'INTEGER'}
    )

    conn.commit()
    conn.close()
    # verify data loaded into table
    print("Successfully loaded crime data into 'neighborhood_data' table.")