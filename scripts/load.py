import os
from dotenv import load_dotenv
import pandas as pd
import logging
from sqlalchemy import create_engine

load_dotenv()

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

logging.basicConfig(
    filename='etl.log', 
    filemode='w',
    level=logging.INFO, 
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def load_data(df, table_name):
    """
    Loading the transformed pest and weather data into a SQL database.
    """
    try:
        engine = create_engine(f"mssql+pyodbc://{db_host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
        connection = engine.connect()

        # Load the dataframe into the specified SQL table
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Data loaded successfully into the {table_name} table.")
    
    except Exception as e:
        logging.error(f"Error loading data into SQL: {e}")
        raise

if __name__ == "__main__":
    try:
        # Load all the dataframes
        df_transformed = pd.read_csv("transformed_data.csv")
        pest_avg = pd.read_csv("pest_avg.csv")
        weekly_weather = pd.read_csv("weekly_weather.csv")

        # Load each table into the SQL database
        load_data(df_transformed, 'pest_weather_data')
        load_data(pest_avg, 'pest_avg_data')
        load_data(weekly_weather, 'weekly_weather_data')

    except Exception as e:
        logging.error(f"Loading process failed: {e}")
        raise