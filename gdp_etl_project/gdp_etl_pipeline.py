import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
import numpy as np
from datetime import datetime

url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
table_name = "Countries_by_GDP"
table_attribs = ["Country", "GDP_USD_millions"]
db_name = "World_Economies.db"
csv_path = "Countries_by_GDP.csv"


def extract(url):
    webpage = requests.get(url).text
    soup = BeautifulSoup(webpage, "html.parser")
    df = pd.DataFrame(columns=["Country", "GDP_USD_millions"])
    tables = soup.find_all("tbody")
    rows = tables[2].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            if col[0].find("a") is not None and "_" not in col[2]:
                data_dict = {
                    "Country": col[0].text.strip(),
                    "GDP_USD_millions": col[2].text.strip(),
                }

                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df):
    df["GDP_USD_millions"] = (
        df["GDP_USD_millions"].replace({",": "", "—": "0"}, regex=True).astype(float)
    )
    df["GDP_USD_millions"] = round(df["GDP_USD_millions"] / 1000, 2)

    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df


def loadtocsv(df, csv_path):
    df.to_csv(csv_path)


def loadtodb(df, table_name, sql_connection):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_state, sql_connection):
    print(query_state)
    query_output = pd.read_sql(query_state, sql_connection)
    print(query_output)


def log(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("etl_project_log.txt", "a") as f:
        f.write(timestamp + ":" + message + "\n")


log("etl procsess started")


log("extraxt phase started")
extracted_data = extract(url)
log("extract phase ended")

log("transform phase started")
transformed_data = transform(extracted_data)
log("transform phase ended")

log("load phase started")
loadtocsv(transformed_data, csv_path)
log("data saved to csv file")

sql_connection = sqlite3.connect("World_Economies.db")
loadtodb(transformed_data, table_name, sql_connection)
log("data loaded to database")

query_state = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_state, sql_connection)

log("etl process ended")
sql_connection.close()
