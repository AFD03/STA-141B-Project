import pandas as pd
from sodapy import Socrata
import sqlite3
import requests

HUD_API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI2IiwianRpIjoiZGViOGNhYjBmYmU3MmViYWI3OTkwOGM5NjVlYzI4NTQ4OGY1NmVmYWI1MThkN2NhMzYwYjY2NzVlZmFjMzkxMzllNWI3MWY1ZjhmNDVjYmIiLCJpYXQiOjE3NjM0NDQ2NDguOTMwNDM0LCJuYmYiOjE3NjM0NDQ2NDguOTMwNDM2LCJleHAiOjIwNzg5Nzc0NDguOTI0MDg2LCJzdWIiOiIxMTMyNjEiLCJzY29wZXMiOltdfQ.RFJKmr9mfPEGvtL8rzuujmReAs4t8w_2rFJKHAPMjpHRm47Xgh3PGDxx7CDrKzhiiQ_zcSj3SzR42zY3aGy9tA"

# note: possible warning "NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'"

def fetch_crime_data():
    """
    Fetches 2025 crime incidents grouped by 'analysis_neighborhood'
    and stores them in 'raw_crime_by_neighborhood'
    """
    # confirmation of fetching data
    print("Fetching crime data from DataSF.")
    client = Socrata('data.sfgov.org', None)

    # police incidents (2018-present) id
    dataset_id = 'wg3w-h783'

    # getting crime data
    soql_select = "analysis_neighborhood"

    # get all incidents from 2025.
    soql_where = "incident_date >= '2025-01-01T00:00:00'"

    # set a high limit for crime data to get (on avg ~150k-200k incidents per year)
    results = client.get(
        dataset_id,
        select=soql_select,
        where=soql_where,
        limit=250000  # up to 250,000 raw incidents
    )
    crime_df = pd.DataFrame.from_records(results) # creating dataframe for crime
    crime_df = crime_df.dropna(subset=['analysis_neighborhood'])

    # convert all neighborhood names to lowercase before grouping (also strip white space)
    crime_df['analysis_neighborhood'] = crime_df['analysis_neighborhood'].astype(str).str.strip().str.lower()

    # drop any rows that are now just an empty string
    crime_df = crime_df[crime_df['analysis_neighborhood'] != '']
    crime_counts = crime_df.groupby('analysis_neighborhood').size()

    # convert crime_counts to a DataFrame
    crime_df_agg = crime_counts.to_frame(name='crime_count').reset_index()

    # verify number of neighborhoods were we find crime incidents
    print(f"Successfully aggregated crime for {len(crime_df_agg)} neighborhoods.")

    # store in the rentals database
    conn = sqlite3.connect('rentals.db')
    c = conn.cursor()

    # explicitly DELETE all old data from the table
    print("Deleting old crime data from table.")
    c.execute("DELETE FROM raw_crime_by_neighborhood")

    # APPEND the new, clean DataFrame to the empty table
    print("Appending new, clean data.")
    crime_df_agg.to_sql(
        'raw_crime_by_neighborhood',
        conn,
        if_exists='append',
        index=False
    )

    conn.commit()
    conn.close()
    # verify data loaded into table
    print("Successfully loaded crime data into 'raw_crime_by_neighborhood' table.")

def fetch_income_data():
    """
    Fetches median household income by census tract from the US Census.
    Stores in 'raw_tract_data'.
    """
    # confirmation of fetching data
    print("\nFetching income data from Census API")

    # ACS 5 year data, table B19013
    # get estimate 'B19013_001E for all census tracts in SF, CA
    census_api_url = (
        "https://api.census.gov/data/2023/acs/acs5"
        "?get=NAME,B19013_001E"
        "&for=tract:*"
        "&in=state:06&in=county:075"
    )
    response = requests.get(census_api_url)
    data = response.json()

    # convert data to DataFrame with columns name, income, & tract
    income_df = pd.DataFrame(data[1:], columns=data[0])

    # cleaning data
    income_df = income_df.rename(columns={
        'B19013_001E': 'median_income',
    })
    # create full 11 digit FIPS code by combining state + county + tract
    # this give the full tract_id
    income_df['tract_id'] = income_df['state'] + income_df['county'] + income_df['tract']

    # convert income to number with negative values referring to 'no data'.
    income_df['median_income'] = pd.to_numeric(income_df['median_income'])
    # filter out tracts with no income data
    income_df = income_df[income_df['median_income'] > 0]
    # reduce to two columns
    income_df = income_df[['tract_id', 'median_income']]

    # print out how many tracts data is found for
    print(f"Found income data for {len(income_df)} census tracts.")

    # store in the rentals database
    conn = sqlite3.connect('rentals.db')
    income_df.to_sql(
        'tract_data',
        conn,
        if_exists = 'replace',
        index = False,
        dtype = {'tract_id': 'TEXT PRIMARY KEY', 'median_income': 'INTEGER'}
    )
    conn.commit()
    conn.close()
    # confirm data is loaded into table
    print("Successfully loaded tract income data into 'tract_data' table.")


def fetch_tract_to_zip_crosswalk():
    """
    Fetches the Tract-to-Zip crosswalk file from the official HUD API
    """
    print("\nFetching Tract-to-Zip crosswalk from HUD API.")

    print(f"DEBUG: Using key that starts with '{HUD_API_KEY[:5]}' and ends with '{HUD_API_KEY[-5:]}'")

    HUD_API_URL = "https://www.huduser.gov/hudapi/public/usps"

    # params for debugging
    params = {
        "type": 1,
        "query": "All",
        "year": 2025,
        "quarter": 1
    }

    headers = {
        "Authorization": f"Bearer {HUD_API_KEY}"
    }

    try:
        print("Sending API request to HUD.")
        response = requests.get(HUD_API_URL, params=params, headers=headers)
        response.raise_for_status()

        print("API request successful. Reading JSON data.")

        data = response.json()
        crosswalk_df = pd.DataFrame(data['data']['results'])

    # error handling
    except requests.exceptions.HTTPError as e:
        print(f"\n--- HTTP ERROR ---")
        print(f"Failed to download the file. Status code: {e.response.status_code}")
        if e.response.status_code in [401, 403]:
            print("ERROR: Your API Key is wrong, invalid, or expired.")
        print(f"URL: {HUD_API_URL}")
        print("------------------\n")
        return

    print("SUCCESS. Data was downloaded.")


    # API will return 'zip' and 'tract' (columns properly renamed)
    crosswalk_df = crosswalk_df.rename(columns={'zipcode': 'zip'})
    crosswalk_df = crosswalk_df.rename(columns={'geoid': 'tract'})

    # check if the required columns exist
    required_cols = ['tract', 'zip', 'res_ratio']
    if not all(col in crosswalk_df.columns for col in required_cols):
        print("--- ERROR: Missing expected columns ---")
        print(f"Expected: {required_cols}")
        print(f"Got: {crosswalk_df.columns.tolist()}")
        print("Please check the debug output and update the script.")
        return

    crosswalk_df = crosswalk_df[['tract', 'zip', 'res_ratio']]

    # filter for SF tracts only (start with '06075')
    crosswalk_df = crosswalk_df[crosswalk_df['tract'].str.startswith('06075')]

    crosswalk_df['res_ratio'] = pd.to_numeric(crosswalk_df['res_ratio'])
    crosswalk_df = crosswalk_df[crosswalk_df['res_ratio'] > 0]

    print(f"Found {len(crosswalk_df)} Tract-to-Zip records for SF.")

    # store in rentals database
    conn = sqlite3.connect('rentals.db')
    crosswalk_df.to_sql(
        'crosswalk_tract_to_zip',
        conn,
        if_exists='replace',
        index=False,
        dtype={'tract': 'TEXT', 'zip': 'TEXT', 'res_ratio': 'REAL'}
    )
    conn.commit()
    conn.close()
    print("Successfully loaded 'crosswalk_tract_to_zip'.")

def fetch_tract_to_hood_crosswalk():
    """
    Fetches SF-specific Tract-to-Neighborhood mapping file.
    """
    print("\nFetching Tract-to-Neighborhood crosswalk from DataSF.")

    # dataset: "Analysis Neighborhoods - 2020 census tracts assigned to neighborhoods"
    dataSF_url = "https://data.sfgov.org/resource/sevw-6tgi.csv"
    crosswalk_df = pd.read_csv(dataSF_url)

    # only want the tract and neighborhood (renamed)
    crosswalk_df = crosswalk_df[['geoid', 'neighborhoods_analysis_boundaries']]
    crosswalk_df = crosswalk_df.rename(columns={
        'geoid': 'tract',
        'neighborhoods_analysis_boundaries': 'neighborhood'
    })

    # print how many records found
    print(f"Found {len(crosswalk_df)} Tract-to-Neighborhood records.")

    # store in rental database
    conn = sqlite3.connect('rentals.db')
    crosswalk_df.to_sql(
        'crosswalk_tract_to_hood',
        conn,
        if_exists='replace',
        index=False,
        dtype={'tract': 'TEXT PRIMARY KEY', 'neighborhood': 'TEXT'}
    )
    conn.commit()
    conn.close()
    # confirm data is loaded into table
    print("Successfully loaded 'crosswalk_tract_to_hood'.")


def join_all_data():
    """
    Final clean join from crime → neighborhood → tract → zip.
    """

    print("\nStarting Final Data Join.")
    conn = sqlite3.connect('rentals.db')

    # load all tables
    crime_df = pd.read_sql("SELECT * FROM raw_crime_by_neighborhood", conn)
    income_df = pd.read_sql("SELECT * FROM tract_data", conn)
    hood_map  = pd.read_sql("SELECT * FROM crosswalk_tract_to_hood", conn)
    zip_map   = pd.read_sql("SELECT * FROM crosswalk_tract_to_zip", conn)

    # normalize neighborhood names for proper matching
    crime_df['analysis_neighborhood'] = (crime_df['analysis_neighborhood']
                                             .astype(str).str.strip().str.lower())
    hood_map['neighborhood'] = (hood_map['neighborhood']
                                    .astype(str).str.strip().str.lower())
    crime_df = crime_df[crime_df['analysis_neighborhood'] != ""]
    hood_map = hood_map[hood_map['neighborhood'] != ""]

    # normalize ALL census tract IDs to 11-digit GEOIDs
    def normalize_tract(t):
        t = str(t).strip().replace(".0", "")
        return t.zfill(11)
    income_df['tract_id'] = income_df['tract_id'].apply(normalize_tract)
    hood_map['tract'] = hood_map['tract'].apply(normalize_tract)
    zip_map['tract'] = zip_map['tract'].apply(normalize_tract)

    # number of unique items
    print("Unique income tracts:", income_df['tract_id'].nunique())
    print("Unique hood-map tracts:", hood_map['tract'].nunique())
    print("Unique zip-map tracts:", zip_map['tract'].nunique())

    # join crime to tracts using the hood_map
    print("Joining crime_df with hood_map.")
    crime_with_tract = pd.merge(
        crime_df,
        hood_map,
        left_on='analysis_neighborhood',
        right_on='neighborhood',
        how='inner'
    )
    crime_by_tract = crime_with_tract[['tract', 'crime_count']]

    # join income to crime by tract
    print("Joining income_df with crime_by_tract.")

    tract_master = pd.merge(
        income_df,
        crime_by_tract,
        left_on='tract_id',
        right_on='tract',
        how='left'        # do not outer join here!
    )

    # missing crime means 0 crimes, not NaN
    tract_master['crime_count'] = tract_master['crime_count'].fillna(0)

    # group by tract
    tract_grouped = tract_master.groupby('tract_id').agg(
        median_income=('median_income', 'first'),
        crime_count=('crime_count', 'sum')
    ).reset_index()

    # final unique tracts after grouping
    print("  Unique final tracts:", tract_grouped['tract_id'].nunique())

    # join tract data to zip crosswalk
    print("Joining tract_grouped with zip_map.")
    final_join = pd.merge(
        tract_grouped,
        zip_map,
        left_on='tract_id',
        right_on='tract',
        how='inner'
    )
    print(f"Joined {len(final_join)} tract with zip rows.")

    # weight by residential ratio and aggregate to zips
    final_join['income_component'] = final_join['median_income'] * final_join['res_ratio']
    final_join['crime_component']  = final_join['crime_count']   * final_join['res_ratio']

    # final groupings
    zip_grouped = final_join.groupby('zip').agg(
        avg_median_income=('income_component', 'sum'),
        crime_count_2025=('crime_component', 'sum')
    ).reset_index()

    zip_grouped['avg_median_income'] = zip_grouped['avg_median_income'].round(2)
    zip_grouped['crime_count_2025']  = zip_grouped['crime_count_2025'].round(0).astype(int)

    zip_grouped = zip_grouped.rename(columns={'zip': 'zip_code'})
    zip_grouped = zip_grouped[zip_grouped['avg_median_income'] > 0]

    # final output row numbers
    print(f"Final dataset contains {len(zip_grouped)} ZIP codes with income+crime data")

    # save final output
    zip_grouped.to_sql(
        'neighborhood_data',
        conn,
        if_exists='replace',
        index=False,
        dtype={
            'zip_code': 'TEXT PRIMARY KEY',
            'crime_count_2025': 'INTEGER',
            'avg_median_income': 'REAL'
        }
    )

    conn.commit()
    conn.close()
    print("\nSuccessfully saved final joined data to 'neighborhood_data'.")

# main call
if __name__ == "__main__":
    fetch_crime_data()
    fetch_income_data()
    fetch_tract_to_zip_crosswalk()
    fetch_tract_to_hood_crosswalk()
    join_all_data()
