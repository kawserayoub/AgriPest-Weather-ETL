# Agricultural Pest and Weather Data - ETL

This project implements an ETL (Extract, Transform, Load) pipeline to process pest and weather data, specifically focusing on agricultural data for rice crops. The data is extracted from a CSV file, transformed to create features, and loaded into a SQL database for analysis.

## Project Overview

The purpose of this ETL pipeline is to:
- **Extract** historical pest and weather data from a CSV file.
- **Transform** the data by calculating new features like temperature range, average humidity, and pest statistics.
- **Load** the transformed data into a SQL database for further analysis.

The data used in this project focuses on rice pests, such as the Brown Planthopper, and various weather conditions like temperature, humidity, rainfall, and more. This data can be valuable for agricultural analysis, helping farmers and researchers make informed decisions about pest control and climate conditions.

## Data Context

The data, stored in `RICE.csv`, includes the following columns:
- **Pest Value**: The number of pests observed (Brown Planthopper in this case).
- **Weather Variables**: Includes maximum and minimum temperatures, humidity, rainfall, wind speed, and sunshine hours.
- **Observation Year and Week**: The year and week of observation.
- **Location**: The geographical location of the observations (e.g., Cuttack).

This dataset provides a combination of weather and pest observations that can be used to explore patterns between climate conditions and pest outbreaks, which is crucial in agriculture.

## Project Structure

```plaintext
AgriPest-Weather-ETL/
│
├── scripts/               # Python scripts for ETL processes
│   ├── extract.py         # Handles data extraction from CSV
│   ├── transform.py       # Transforms data and creates new features
│   ├── load.py            # Loads data into a SQL database
│   ├── etl_test.py        # Contains a sample to test the ETL
│   └── main.py            # Sets up the entire ETL pipeline
│
├── data/
│   └── RICE.csv           # The dataset used in the ETL process
│
├── assets/
│   └── task_scheduler.png # Image of the task scheduler setup
│
├── .gitignore             # Ignores sensitive and unnecessary files
├── .gitattributes         # Manages file attributes like line endings
│
├── etl.log                # Logfile
├── README.md              # Project documentation
└── requirements.txt       # Python dependencies
```
# How to Use This Project
## 1. Clone the Repository
```bash
git clone https://github.com/your-repo-url/AgriPest-Weather-ETL.git
cd AgriPest-Weather-ETL 
```
## 2. Set Up the Environment
Install the rquired dependencies listed in requirments.txt:
```bash 
pip install -r requirements.txt
```

Create a .env file with your database credentials. For example:
```bash
DB_HOST=your_database_host
DB_NAME=your_database_name
```
## 3. Run the ETL Pipeline
You can run the entire ETL pipeline by executing the main.py script:
```bash
python scripts/main.py
```
This will:

- Extract the pest and weather data from RICE.csv.
- Validate the data to ensure all required columns and values are present.
- Transform the data to calculate new features such as temperature range and average humidity.
- Load the transformed data into a SQL database.


