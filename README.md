# Bash Tech Assessment

## Data Engineer Tech Assessment - Ian Spies

---

## Table of Contents
1.⁠ ⁠[Overview](#1-overview)
2.⁠ ⁠[Prerequisites](#2-prerequisites)
3.⁠ ⁠[Installation](#3-installation)
4.⁠ ⁠[Data Extraction](#4-data-extraction)
5.⁠ ⁠[Incremental Loading Logic](#5-incremental-loading-logic)
6.⁠ ⁠[Creating Views](#6-creating-views)
7.⁠ ⁠[Assumptions and Design Decisions](#7-assumptions-and-design-decisions)
8.⁠ ⁠[Conclusion](#8-conclusion)

---

## 1. Overview
This project extracts data from NASA's public API, processes nested JSON data, handles incremental data loading, and integrates the data with Snowflake for analytical purposes.

---

## 2. Prerequisites
Ensure you have the following installed:

•⁠  ⁠*Python 3.6+*
•⁠  ⁠*pandas library*: ⁠ pip install pandas ⁠
•⁠  ⁠*requests library*: ⁠ pip install requests ⁠
•⁠  ⁠*snowflake-connector-python library*: ⁠ pip install snowflake-connector-python ⁠
•⁠  ⁠*A Snowflake account* and appropriate credentials.

---

## 3. Installation

### Step 1: Install Python and Required Libraries
1.⁠ ⁠*Install Python*: Download and install Python from [python.org](https://www.python.org/).
2.⁠ ⁠*Install pandas*: Run ⁠ pip install pandas ⁠ in the terminal.
3.⁠ ⁠*Install requests*: Run ⁠ pip install requests ⁠ in the terminal.
4.⁠ ⁠*Install snowflake-connector-python*: Run ⁠ pip install snowflake-connector-python ⁠ in the terminal.

### Step 2: Set Up Snowflake Account
1.⁠ ⁠*Create a Snowflake Account*: Sign up [here](https://signup.snowflake.com/).
2.⁠ ⁠*Configure Snowflake Connection*: Obtain your Snowflake account credentials (username, password, account name).
3.⁠ ⁠*Database and Schema Access*: Ensure access to the required database and schema or create them as needed.

---

## 4. Data Extraction

### API Utilization
•⁠  ⁠*Endpoint*: NASA's Asteroid - NeoWs API
•⁠  ⁠*Purpose*: Fetch asteroid data for the last 30 days.
•⁠  ⁠*API Key*: Obtain your API key [here](https://api.nasa.gov/).

*API Key used*: ⁠ zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC ⁠.

### Batch Processing
•⁠  ⁠*Date Calculation*: Calculated the start and end dates for the last 30 days using Python's ⁠ datetime ⁠.
•⁠  ⁠*7-Day Chunks*: Data is fetched in 7-day intervals to avoid API rate limits.

### Normalization
•⁠  ⁠*JSON Normalization*: Flattened nested JSON data using ⁠ pandas.json_normalize ⁠.
•⁠  ⁠*File Storage*: Saved normalized data as ⁠ nasa_data_2.csv ⁠ and ⁠ nasa_data_incremental.csv ⁠.

---

## 5. Incremental Loading Logic

### CSV to DataFrame
•⁠  ⁠Loaded CSV data into a pandas DataFrame.
•⁠  ⁠Extracted ⁠ close_approach_date ⁠ from the ⁠ close_approach_data ⁠ field.

### Data Cleanup
•⁠  ⁠*JSON Parsing*: Created a function to extract and format ⁠ close_approach_data ⁠.
•⁠  ⁠*NaN Handling*: Replaced ⁠ NaN ⁠ values with ⁠ None ⁠ for Snowflake compatibility.

### Snowflake Connection
•⁠  ⁠Established a connection using ⁠ snowflake.connector ⁠.

### Data Insertion
•⁠  ⁠Iterated over DataFrame rows to insert records into Snowflake (⁠ nasa_neo_data_2 ⁠).
•⁠  ⁠Ensured the table schema matches the DataFrame structure.

### Load Tracking
•⁠  ⁠Extracted the maximum ⁠ close_approach_date ⁠ and updated ⁠ load_tracking ⁠ and ⁠ load_tracking_unified ⁠ tables.

---

## 6. Creating Views

### Individual Scripts
#### *nasa_neo_close_approach_view*
•⁠  ⁠*Purpose*: Flatten ⁠ close_approach_data ⁠ for analytics.
•⁠  ⁠*Fields*: ⁠ id ⁠, ⁠ neo_reference_id ⁠, ⁠ name ⁠, ⁠ date ⁠, ⁠ velocity ⁠, ⁠ distance ⁠, ⁠ orbiting_body ⁠.

#### *nasa_neo_speed_view*
•⁠  ⁠*Purpose*: Focus on NEOs' relative velocities.
•⁠  ⁠*Fields*: ⁠ id ⁠, ⁠ neo_reference_id ⁠, ⁠ name ⁠, ⁠ relative velocities ⁠, ⁠ orbiting_body ⁠.

#### *nasa_neo_size_view*
•⁠  ⁠*Purpose*: Analyze NEOs' size information.
•⁠  ⁠*Fields*: ⁠ id ⁠, ⁠ neo_reference_id ⁠, ⁠ name ⁠, ⁠ absolute_magnitude ⁠, ⁠ estimated diameters ⁠.

### Unified Script
#### *nasa_neo_close_approach_unified_view*
•⁠  ⁠Combined data with appropriate type conversions.

---

## 7. Assumptions and Design Decisions

### Consistency of JSON Structure
•⁠  ⁠Assumed the JSON structure remains consistent.

### Data Types and Formats
•⁠  ⁠Chose data types (⁠ FLOAT ⁠, ⁠ DATE ⁠, ⁠ TIMESTAMP ⁠, ⁠ STRING ⁠) to ensure accuracy and future compatibility.

### Script Duality
#### Split Scripts
•⁠  ⁠Modularized functions for independent updates.
•⁠  ⁠Easier testing and debugging.

#### Unified Script
•⁠  ⁠Streamlined execution for simpler workflows.

---

## 8. Conclusion
This project successfully extracts, processes, and loads NASA's Near-Earth Object (NEO) data into Snowflake, creating views for easy analysis. It demonstrates efficient ETL practices with both modular and unified script approaches, offering flexibility for various requirements.