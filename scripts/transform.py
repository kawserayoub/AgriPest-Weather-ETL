import pandas as pd
import logging
from extract import extract_data, validate_data

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
        # Logging missing values for key columns
        if df['Pest Value'].isna().any():
            logging.warning('Missing values detected in pest_avg after aggregation.')
        if df[['MaxT', 'MinT', 'RH1(%)', 'RH2(%)']].isna().any().any():
            logging.warning('Missing values detected in weekly_weather after aggregation.')
            
        #Fill missing values with default values for critical numeric columns
        df.fillna({'Pest Value': 0, 'MaxT': 0, 'MinT': 0, 'RH1(%)': 0, 'RH2(%)': 0}, inplace=True)
            
        # Calculate the temperature range (difference between max and min temperature)
        df['Temp Range'] = df['MaxT'] - df['MinT']

        # Calculate average humidity
        df['Avg Humidity'] = (df['RH1(%)'] + df['RH2(%)']) / 2

        logging.info("Data cleaned, and new features added successfully.")

        # Average pest value grouped by year and pest name
        pest_avg = df.groupby(['Observation Year', 'PEST NAME'])['Pest Value'].mean().reset_index()

        # Aggregate weather data by year and week
        weekly_weather = df.groupby(['Observation Year', 'Standard Week']).agg({
            'MaxT': 'mean',           # Avg of max temp
            'MinT': 'mean',           # Avg of min temp
            'Temp Range': 'mean',     # Avg of temp range
            'Avg Humidity': 'mean',   # Avg of calculated humidity
            'RF(mm)': 'sum',          # Sum of rainfall in millimeters
            'WS(kmph)': 'mean',       # Avg wind speed in km/h
            'SSH(hrs)': 'mean',       # Avg sunshine hours
            'EVP(mm)': 'mean'         # Avg evaporation in millimeters
        }).reset_index()

        logging.info("Aggregations completed successfully.")
        
        return df, pest_avg, weekly_weather

    except Exception as e:
        logging.error(f"Error during data transformation: {e}", exc_info=True)
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