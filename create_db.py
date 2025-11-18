import sqlite3

# creating file 'rentals.db' in project folder
conn = sqlite3.connect('rentals.db')
c = conn.cursor()

# message for confirmation
print("Database rentals.db created.")

# create 'rentals' table with defined columns for webscraping portion
c.execute("""
CREATE TABLE IF NOT EXISTS rentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id TEXT UNIQUE,
    price INTEGER,
    bedrooms REAL,
    bathrooms REAL,
    sqft INTEGER,
    zip_code TEXT,
    neighborhood TEXT,
    full_description TEXT,
    scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
# message for confirmation
print("Table 'rentals' created successfully.")

# table stores data by zip code, the final join key (may add more columns later)
c.execute("""
CREATE TABLE IF NOT EXISTS neighborhood_data (
    zip_code TEXT PRIMARY KEY,
    crime_count_2025 INTEGER,
    avg_median_income REAL 
)
""")
# message for confirmation
print("Table 'neighborhood_data' created.")

# store the raw data from the crime API
c.execute("""
CREATE TABLE IF NOT EXISTS raw_crime_by_neighborhood (
    analysis_neighborhood TEXT PRIMARY KEY,
    crime_count INTEGER
)
""")
# message for confirmation
print("Table 'raw_crime_by_neighborhood' created.")

# temporary table to hold the income data before mapping it to zip codes
c.execute("""
CREATE TABLE IF NOT EXISTS tract_data (
    tract_id TEXT PRIMARY KEY,
    median_income INTEGER
)
""")
# message for confirmation
print("Table 'tract_data' created.")

# 'crosswalk_tract_to_zip' table (to transfer census data to zipcodes)
c.execute("""
CREATE TABLE IF NOT EXISTS crosswalk_tract_to_zip (
    tract TEXT,
    zip TEXT,
    res_ratio REAL, 
    PRIMARY KEY (tract, zip)
)
""")
# message for confirmation
print("Table 'crosswalk_tract_to_zip' created.")

# 'crosswalk_tract_to_hood' table
c.execute("""
CREATE TABLE IF NOT EXISTS crosswalk_tract_to_hood (
    tract TEXT PRIMARY KEY,
    neighborhood TEXT
)
""")
# message for confirmation
print("Table 'crosswalk_tract_to_hood' created.")

conn.commit()
conn.close()
# confirm tables and database created
print("Database rentals.db and all tables are ready.")
