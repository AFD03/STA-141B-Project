# [City Name] Rental Price Analyzer

**Team Members:** Alexander Davis, Eric Goldman, Manik Sethi, Kesar Sidhu  
**Course:** STA 141B  
**Due Date:** December 12, 2025

## 1. Project Overview

This project is a data acquisition and analysis pipeline built to answer key questions about the rental market in [City Name]. The primary goal is to determine the *true* drivers of rental prices by acquiring, processing, and joining two distinct types of data:

1.  **Scraped Listings Data:** Real-time rental listings scraped from [Name of Rental Site].
2.  **Public API Data:** Neighborhood-level statistics (e.g., crime rates, median income) from the [City Name] Open Data Portal.

By parsing unstructured text from listing descriptions and joining it with quantitative neighborhood data, we build a predictive model to identify which factors—from "in-unit laundry" to "zip code crime rate"—have the most significant impact on price.

## 2. Research Questions

This project seeks to answer the following:

* **Q1:** How do standard features (bedrooms, bathrooms) and neighborhood-level factors (median income, crime rates) correlate with rental prices?
* **Q2:** After controlling for these standard factors, what is the *added dollar value* of specific, unlisted amenities (e.g., "in-unit laundry," "parking," "pet-friendly") that can only be found by processing the unstructured text descriptions?

## 3. Data Architecture & Pipeline

The project is built as a three-stage data pipeline, ensuring a clean separation between data acquisition and analysis.



1.  **Acquisition (API):** The `api/api_fetcher.py` script queries the [City Name] Open Data Portal API for datasets on [crime, median income, etc.] by zip code. This data is cleaned and loaded into the `zipcodes` table in our `rentals.db` SQLite database.
2.  **Acquisition (Scraper):** The `scraper/scraper.py` script scrapes [Number] pages of rental listings from [Rental Site]. It extracts structured data (price, beds, baths) and unstructured data (the full text description) and loads them into the `rentals` table in the same `rentals.db` file.
3.  **Analysis:** The `notebooks/analysis.ipynb` notebook reads the final `rentals.db` file. It joins the `rentals` and `zipcodes` tables, performs feature engineering (parsing the unstructured text), builds the regression model, and generates all final visualizations.

## 4. Tech Stack

* **Data Acquisition:** Python, `BeautifulSoup4`, `Requests`, `sqlite3`
* **Data Analysis:** `pandas`, `scikit-learn` (for Linear Regression)
* **Data Visualization:** `matplotlib`, `plotly` (for choropleth maps)
* **Environment:** PyCharm (for script development), Google Colab (for collaborative analysis)
* **Version Control:** Git & GitHub
