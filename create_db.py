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

# creating table with 'zip_code' as the primary key (one row per zip code)
c.execute("""
CREATE TABLE IF NOT EXISTS zipcodes (
    zip_code TEXT PRIMARY KEY,
    median_income INTEGER,
    crime_count INTEGER
)
""")

# message for confirmation
print("Table 'zipcodes' created.")

conn.commit()
conn.close()

# confirmation of tables and database creation
print("Database and tables ready.")