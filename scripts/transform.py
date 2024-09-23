import pandas as pd
import logging

from extract import validate_data

logging.basicConfig(
    filename='etl.log', 
    filemode= 'w',
    level=logging.INFO, 
    format='%(asctime)s:%(levelname)s:%(message)s')

def transform_data(df):
    """
    Cleans and transforms the pest and weather data by adding new features and 
    performing aggregations.
    """
    try:
        # Clean the data by replacing missing values with 0
        df.fillna(0, inplace=True)
        
        # Convert 'Observation Year' and 'Standard Week' to a datetime-like object
        df['Observation Date'] = pd.to_datetime(df['Observation Year'].astype(str) + df['Standard Week'].astype(str) + '1', format='%G%V%u')

        # Calculate the temperature range (difference between max and min temperature)
        df['Temp Range'] = df['MaxT'] - df['MinT']

        # Calculate average humidity
        df['Avg Humidity'] = (df['RH1(%)'] + df['RH2(%)']) / 2

        logging.info("Data cleaned, and new features added successfully.")

        # Average pest value grouped by year and pest name
        pest_avg = df.groupby(['Observation Year', 'PEST NAME'])['Pest Value'].mean().reset_index()

        # Group by year and week to get average weather conditions
        weekly_weather = df.groupby(['Observation Year', 'Standard Week']).agg({
            'MaxT': 'mean',
            'MinT': 'mean',
            'Temp Range': 'mean',
            'Avg Humidity': 'mean',
            'RF(mm)': 'sum',  # Sum of rainfall
            'WS(kmph)': 'mean',
            'SSH(hrs)': 'mean',
            'EVP(mm)': 'mean'
        }).reset_index()

        logging.info("Aggregations completed successfully.")
        
        return df, pest_avg, weekly_weather

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise

if __name__ == "__main__":
    try:
        file_path = r"C:\RICE.csv"
        df = pd.read_csv(file_path)
        
        validate_data(df)

        df_transformed, pest_avg, weekly_weather = transform_data(df)

        # Export the transformed data and aggregated data to CSV files
        df_transformed.to_csv("transformed_data.csv", index=False)
        pest_avg.to_csv("pest_avg.csv", index=False)
        weekly_weather.to_csv("weekly_weather.csv", index=False)

        logging.info("Data transformation process completed successfully.")
    
    except Exception as e:
        logging.error(f"Data transformation process failed: {e}")
        raise
