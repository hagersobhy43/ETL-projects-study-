import numpy as np
import pandas as pd
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup

# Project configuration and global variables
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_name = "Largest_banks"
database_name = "Banks.db"
table_atrributes = ["Name", "MC_USD_Billion"]
csv_path = "exchange_rate.csv"
output_path = "Largest_banks_data.csv"


def log_progress(message):
    """Logs the message with a timestamp into a text file for tracking"""
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + " " + message + "\n")


def extract(url, table_atrributes):
    """Scrapes bank names and Market Cap from Wikipedia and returns a DataFrame"""
    webpage = requests.get(url).text
    data = BeautifulSoup(webpage, "html.parser")

    # Target the first table body and its rows
    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")
    df = pd.DataFrame(columns=table_atrributes)

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            # col[1] is the Bank Name, col[2] is the Market Cap
            data_dict = {
                "Name": col[1].text.strip(),
                "MC_USD_Billion": col[2].text.strip(),
            }
            # Append rows one by one to the DataFrame
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    """Cleans USD values and adds columns for EUR, GBP, and INR using exchange rates"""
    # Load exchange rates and create a lookup dictionary (Currency: Rate)
    exchange_df = pd.read_csv(csv_path)
    exchange_dict = exchange_df.set_index("Currency").to_dict()["Rate"]

    # Remove commas and convert the USD column from String to Float for math
    df["MC_USD_Billion"] = (
        df["MC_USD_Billion"].replace(",", "", regex=True).astype(float)
    )

    # Use list comprehensions to scale the USD values and round to 2 decimals
    df["MC_EUR_Billion"] = [
        np.round(x * exchange_dict["EUR"], 2) for x in df["MC_USD_Billion"]
    ]
    df["MC_GBP_Billion"] = [
        np.round(x * exchange_dict["GBP"], 2) for x in df["MC_USD_Billion"]
    ]
    df["MC_INR_Billion"] = [
        np.round(x * exchange_dict["INR"], 2) for x in df["MC_USD_Billion"]
    ]

    return df


def load_to_csv(df, output_path):
    """Saves the final transformed data to a CSV file"""
    df.to_csv(output_path, index=False)


def load_to_db(df, table_name, sql_connection):
    """Loads the DataFrame into a SQL table, replacing it if it already exists"""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_queries(query_state, sql_connection):
    """Executes a SQL query on the database and prints the result"""
    print(f"Query: {query_state}")
    query_output = pd.read_sql(query_state, sql_connection)
    print(query_output)


# --- ETL Pipeline Execution ---

log_progress("Preliminaries complete. Initiating ETL process")

# Phase 1: Extract
log_progress("Data extraction started")
extracted_data = extract(url, table_atrributes)
log_progress("Data extraction complete")

# Phase 2: Transform
log_progress("Initiating Transformation process")
tranformed_data = transform(extracted_data, csv_path)
log_progress("Data transformation complete")

# Phase 3: Load (CSV)
log_progress("Initiating Loading process (CSV)")
load_to_csv(tranformed_data, output_path)
log_progress("Data saved to CSV file")

# Phase 4: Load (Database)
log_progress("SQL Connection initiated")
sql_connection = sqlite3.connect(database_name)
load_to_db(tranformed_data, table_name, sql_connection)
log_progress("Data loaded to Database")

# Phase 5: Querying the results
# 1. View all data
query_state1 = f"SELECT * FROM {table_name}"
run_queries(query_state1, sql_connection)

# 2. View average Market Cap in GBP
query_state2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_queries(query_state2, sql_connection)

# 3. View only the top 5 bank names
query_state3 = f"SELECT Name FROM {table_name} LIMIT 5"
run_queries(query_state3, sql_connection)

log_progress("Process Complete")
sql_connection.close()
