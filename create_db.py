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
print("Table 'neighborhood_data' created.")

# temporary table to hold the income data before mapping it to zip codes
c.execute("""
CREATE TABLE IF NOT EXISTS tract_data (
    tract_id TEXT PRIMARY KEY,
    median_income INTEGER
)
""")
print("Table 'tract_data' created.")

# 'crosswalk' table (to transfer census data to zipcodes)
c.execute("""
CREATE TABLE IF NOT EXISTS crosswalk (
    tract TEXT,
    zip TEXT,
    res_ratio REAL, 
    PRIMARY KEY (tract, zip)
)
""")
print("Table 'crosswalk' created.")

conn.commit()
conn.close()
# confirm tables and database created
print("Database rentals.db and all tables are ready.")