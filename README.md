# ETL_Data_Engineering_Project
This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline for collecting, processing, and analyzing financial data about the world's largest banks. The data is extracted from a historical snapshot of a Wikipedia page and enriched with currency conversion before being stored in both CSV and SQLite database formats.

---

## Project Overview

- **Extract** data from a public web page using BeautifulSoup.
- **Transform** market capitalization from USD to other currencies (GBP, EUR, INR) using exchange rates from a local CSV file.
- **Load** the transformed data into:
  - A **CSV file**
  - An **SQLite database**
- Execute **SQL queries** on the loaded database to demonstrate data analysis capabilities.

---

## Technologies Used

- `Python 3`
- `Pandas` – data manipulation
- `NumPy` – numerical operations
- `BeautifulSoup` – web scraping
- `Requests` – HTTP requests
- `SQLite3` – local database
- `CSV` – for data storage
- `Datetime` – for logging

---

## 📊 Data Sources

- **Bank Data**:  
  Snapshot from [Wikipedia – List of Largest Banks](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)

- **Exchange Rates**:  
  Provided in `exchange_rate.csv`.
