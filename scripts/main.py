import time
import logging
from extract import extract_data, validate_data
from transform import transform_data
from load import load_data

def main():
    start_time = time.time() 

    try:
        logging.basicConfig(
            filename='etl.log',
            filemode='w',
            level=logging.INFO,
            format='%(asctime)s:%(levelname)s:%(message)s'
        )
        
        logging.info("Starting ETL pipeline...")

        # Step 1: Extract and validate data
        file_path = r"C:\RICE.csv"  
        extract_start = time.time()
        df = extract_data(file_path)
        validate_data(df)  
        logging.info(f"Extraction and validation took: {time.time() - extract_start:.2f} seconds")

        # Step 2: Transform data
        transform_start = time.time()
        df_transformed, pest_avg, weekly_weather = transform_data(df)  
        logging.info(f"Transformation took: {time.time() - transform_start:.2f} seconds")

        # Step 3: Load data to SQL
        load_start = time.time()

        # Load each of the tables into the SQL database
        load_data(df_transformed, 'pest_weather_data')
        logging.info("Loaded data into pest_weather_data table.")
        
        load_data(pest_avg, 'pest_avg_data')
        logging.info("Loaded data into pest_avg_data table.")
        
        load_data(weekly_weather, 'weekly_weather_data')
        logging.info("Loaded data into weekly_weather_data table.")
        
        logging.info(f"Loading took: {time.time() - load_start:.2f} seconds")

        logging.info(f"ETL pipeline completed in: {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()