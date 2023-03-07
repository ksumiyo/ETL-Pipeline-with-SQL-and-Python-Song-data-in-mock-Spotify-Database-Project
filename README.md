# ETL-Pipeline-with-SQL-and-Python-Song-data-in-mock-music-streaming-database
This repository includes Python scripts and a Jupyter Notebook that create and execute an ETL pipeline for a mock music streaming service database.

Short description of files included in the repository:
  - data: braching folder containing log and data files (json format)
  - create_tables.py: Connects to the mock database and creates the necessary tables
  - sql_queries.py: Contains all SQL queries used for the ETL processes
  - etl.py: contains the code necessary for extracting and interpreting the data contained within the json files
  - etl.ipynb: Jupyter Notebook that utilizes all of the above to transfer raw data into the appropriate tables in the mock database
