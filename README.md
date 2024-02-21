# Multinational Retail Data Centralization Project

## Overview

This project aims to centralize sales data from a multinational company into a single, accessible database schema named `sales_data`. The initiative seeks to make sales data easily analyzable and accessible, supporting the organization's goal to become more data-driven.

## Modules

### 1. `data_cleaning.py`

Responsible for cleaning data from various sources, including users, card details, store details, product weights, products data, orders data, and date details. It ensures data integrity and standardizes formats before uploading to the database.

### 2. `database_utils.py`

Manages database connections, including reading credentials, initializing connections, and uploading cleaned data to the `sales_data` schema. It ensures that data is correctly and efficiently stored in the central database.

### 3. `data_extraction.py`

Extracts data from multiple sources, such as RDS systems, PDF files, APIs, and AWS S3 buckets. It converts various data formats into pandas DataFrames for further processing and cleaning.

## Purpose

The scripts are designed to automate the extraction, cleaning, and uploading of sales data into a centralized database. This system acts as a single source of truth for the company's sales data, facilitating easier access and analysis.

## Project Background

With sales data spread across different sources, accessing and analyzing this information has been challenging. By centralizing the data, we aim to enhance the organization's analytical capabilities and decision-making processes.

## Usage

- Ensure database credentials are correctly set in the YAML file referenced by `database_utils.py`.
- Utilize `data_extraction.py` to pull data from various sources into pandas DataFrames.
- Clean the extracted data using `data_cleaning.py` to ensure it meets the database schema requirements.
- Upload the cleaned data to the `sales_data` database schema using `database_utils.py`.

## Requirements

- Python 3.x
- Pandas
- SQLAlchemy
- PyYAML
- Requests (for API calls)
- Additional dependencies for PDF and JSON data handling.

## Conclusion

This system simplifies data management for the multinational company, promoting a data-driven culture by making sales data accessible and analyzable from a centralized location.
