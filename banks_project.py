# Banks Data ETL Project
# This script extracts data from a Wikipedia page about the largest banks, transforms the data using currency exchange rates.
# Loads it into a CSV file and SQLite database. It also performs some basic SQL queries.

# Imports
import numpy as np
import pandas as pd
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
import requests

# URLs and File Paths
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
output_path = './Largest_banks_data.csv'
csv_path = './exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
table_attrbs = ['Name', 'MC_USD_Billion']
log_file = 'code_log.txt'

# Function to Log Progress
def log_progress(message):
    """
    Appends timestamped messages to a log file for tracking progress.
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ':' + message +'\n')

log_progress('Preliminaries complete. Initiating ETL process')

# Extract Function
def extract(url, table_attrbs):
    """
    Extracts the bank data from the given Wikipedia URL using BeautifulSoup.
    """
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns= table_attrbs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')    
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'Name': col[1].text.strip(),
                'MC_USD_Billion': float(col[2].text.strip())}
            df1 = pd.DataFrame(data_dict, index = [0])
            df = pd.concat([df, df1], ignore_index = True)

    return df

df = extract(url, table_attrbs)
# print(df)
log_progress('Data extraction complete. Initiating Transformation process')

# Transform Function
def transform(df, csv_path):
    """
    Transforms the data by converting market capitalization from USD to other currencies.
    """
    dataframe = pd.read_csv(csv_path)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x* exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

df = transform(df, csv_path)
# print(df)
log_progress('Data transformation complete. Initiating Loading process')

# Load to CSV
def load_to_csv(df, output_path):
    """
    Saves the transformed dataframe to a CSV file.
    """
    df.to_csv(output_path, index = False)

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

# Load to Database
def load_to_db(df, sql_connection, table_name):
    """
    Loads the dataframe into an SQLite database table.
    """
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

# Run SQL Queries
def run_queries(query_statement, sql_connection):
    """
    Runs SQL queries and prints the results.
    """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Run queries on the database
query_statement = f'SELECT * FROM {table_name}'
run_queries(query_statement, sql_connection)

query_statement = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
run_queries(query_statement, sql_connection)

query_statement = f'SELECT Name FROM {table_name} LIMIT 5'
run_queries(query_statement, sql_connection)


log_progress('Process Complete')

# Close SQL Connection
sql_connection.close()
log_progress('Server Connection closed')


