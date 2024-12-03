# bash_tech_assessment
DATA ENGINEER TECH ASSESSMENT_IAN SPIES

Table of Contents
1. Overview
2. Prerequisites
3. Installation
4. Data Extraction
5. Incremental Loading Logic
6. Creating Views
7. Assumptions and Design Decisions
8. Conclusion

---------

1.Overview
  This project aims to extract data from NASA’s public API, process the nested JSON, handle incremental data loading, and integrate the data with Snowflake for analytics use.


2.Prerequisites
  Ensure you have the following installed:
  - Python 3.6+
  - pandas library
  - requests library
  - snowflake-connector-python library
  - A Snowflake account and the appropriate credentials


3.Installation
  Step 1: Install Python and Required Libraries
  Install Python: Download and install Python from python.org.

  Install pandas: in the terminal - pip install pandas
  Install requests: in the terminal - pip install requests
  Install snowflake-connector-python: in the terminal - pip install snowflake-connector-python

  Step 2: Set Up Snowflake Account
  Create a Snowflake Account: Sign up for a Snowflake account at Snowflake.
  Configure Snowflake Connection: Obtain your Snowflake account credentials (username, password, account name).
  Ensure you have access to the necessary database and schema where you will load the data. If it doesnt exist, create a database and schema.


4. Data Extraction
 API Utilization
  Endpoint: NASA's Asteroid - NeoWs API
  Purpose: Fetch asteroid data for the last 30 days.
  API Key: Required an API key for access, you can create your own using https://api.nasa.gov/ 
  API used: zEV8WaqAclQozMvV7r8zkXjdX6O5NLcAqvgJgtwC. 

 Batch Processing
  - Date Calculation: Utilized Python’s datetime library to calculate the start and end dates for the last 30 days.
  - 7-Day Chunks: Due to potential API rate limits and large data volume, fetched data in 7-day periods, iterating through the 30-day span.

 Normalization
  - JSON Normalization: Used pandas.json_normalize to convert the nested JSON data into a flat table structure.
  - File Storage: Saved the normalized data into a CSV file (nasa_data_2.csv) and (nasa_data_incremential.csv) for subsequent loading into Snowflake.


5.Incremental Loading Logic
 CSV to DataFrame
  - Loaded the saved CSV data into a pandas DataFrame for manipulation and preparation.
  - Extracted the close_approach_date from the close+_approach_data field (used in incremential loading logic)
 Data Cleanup
  - JSON Parsing: Created a function to extract and format close_approach_data correctly.
  - NaN Handling: Replaced NaN values with None to avoid insertion issues in Snowflake.

 Snowflake Connection
  - Established a connection to Snowflake using snowflake.connector.
 Data Insertion
  - Iteration: Iterated over the DataFrame rows to insert records into the Snowflake table (nasa_neo_data_2).
 Schema: Ensured the table schema in Snowflake matches the DataFrame structure.
 Load Tracking
  - Extracted the maximum close_approach_date from the DataFrame.
  - Updated the load_tracking table and load_tracking_unified table in Snowflake to record the last load date, ensuring no duplicate data loads in subsequent runs.
  - When a new get request is made, the last availble date in the data (from close_approach_data field) is used to determine how much data needs to be retrieved until the current day. 


6.Creating Views
--Individual Scripts--
 View: nasa_neo_close_approach_view
  Purpose: Flatten the close_approach_data JSON to create an analytics-friendly structure.
  Fields Included: ID, neo_reference_id, name, date, velocity, distance, orbiting body, etc. (all fields)
  Data Types: Appropriate conversions to FLOAT, DATE, TIMESTAMP, and STRING.

 View: nasa_neo_speed_view
  Purpose: Focus on the relative velocity/speed information of NEOs.
  Fields Included: ID, neo_reference_id, name, close_approach_date_full, epoch_date_close_approach, relative velocities (km/s, km/h, mph), orbiting body.
  Data Types: Velocity fields converted to FLOAT for numerical analysis.

 View: nasa_neo_size_view
  Purpose: Focus on the size-related information of NEOs.
  Fields Included: ID, neo_reference_id, name, close_approach_date_full, absolute magnitude, estimated diameters (km, meters, miles, feet), orbiting body.
  Data Types: Diameter fields converted to FLOAT for numerical analysis.
  
--Unified Script--
 View: nasa_neo_close_approach_unified_view
  Purpose: Flatten the close_approach_data JSON to create an analytics-friendly structure.
  Fields Included: ID, neo_reference_id, name, date, velocity, distance, orbiting body, etc.
  Data Types: Appropriate conversions to FLOAT, DATE, TIMESTAMP, and STRING.

 View: nasa_neo_speed_view_unified
  Purpose: Focus on the relative velocity information of NEOs.
  Fields Included: ID, neo_reference_id, name, close_approach_date_full, epoch_date_close_approach, relative velocities (km/s, km/h, mph), orbiting body.
  Data Types: Velocity fields converted to FLOAT for numerical analysis.

 View: nasa_neo_size_view_unified
  Purpose: Focus on the size-related information of NEOs.
  Fields Included: ID, neo_reference_id, name, close_approach_date_full, absolute magnitude, estimated diameters (km, meters, miles, feet), orbiting body.
  Data Types: Diameter fields converted to FLOAT for numerical analysis.

  
7.Assumptions and Design Decisions
 Consistency of JSON Structure
  Assumed the structure of the JSON data from NASA’s API remains consistent over time.
 Data Types and Formats
  Chose data types (FLOAT, DATE, TIMESTAMP, STRING) that facilitate accurate representation and ease of analysis. (also for building reports later)
 
 Duality Approach:
  The intent of this approach is to show what performance, maintanence, complexity and logic/understanding differences arise when each script is split according 
  to its function and when all functions are combined into a unified script.

  Both Split scripts (get_incr_data_2.py & load_incr_to_snowwflake_2.py) and the unified script (unified_nasa_loader.py) use the inital 30 day GET script (get_nasa_data_2.py),
  and the initial LOAD script (load_to_snowflake_2.py).
  Thereafter a Delete X-amount of days (int his project it has been set to 3 days) is used as a separate script for both scenarios.

  The scripts are run and split as follows:
  Scripts explained:
     * 1. get_nasa_data_2.py: This script fetches the full data set from NASA via API.
     * 2. load_to_snowflake_2.py: This script loads the fetched data into the Snowflake workspace.
     * 3. delete_x_days_2.py: This script deletes the last 3 days of data from the Snowflake table.
      * 3.1.1 get_incr_data_2.py: This script fetches only new data from NASA since the last load date.
      * 3.1.2 load_incr_to_snowflake_2.py: This script loads the incrementally fetched data and saves it in a separate csv (nasa_data_incrmential.csv) that loads into Snowflake.
      * 3.2 unified_nasa_loader.py: This script does the above mentioned 2 scripts in one swift action. Fetch new data and loads it. 
       (The script queries Snowflake to get the last load date from the load_tracking_unified table. The script fetches new incremental 
      data from the NASA API for the date range from the last load date to the current date.)

  Split Scripts: Created separate scripts for data extraction (get_incr_data_2.py) and data loading (load_incr_to_snowflake_2.py) to 
                 modularize the process and allow independent updates and testing.
  Unified Script: Additionally, created a unified script (unified_nasa_loader.py) that combines extraction and loading for streamlined execution.

  Pros of Split Scripts: Easier maintenance, independent testing, and targeted debugging.
  Pros of Unified Script: Simplified execution, fewer dependencies, and reduced risk of disjointed data handling.
  Choice of Design: The final decision on which approach to use would depend on the specific project requirements, such as the frequency of data updates, 
                    system architecture, and operational workflows.

8.Conclusion
 By following this structured approach, we ensured efficient data extraction, transformation, loading (via Python Scripts), and analytics-friendly view creation in Snowflake. 
 This setup allows for easy querying and comprehensive analysis of the NASA NEO data.
