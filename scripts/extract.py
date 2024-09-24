import pandas as pd
import logging

logging.basicConfig(
    filename='etl.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def extract_data(file_path):
    """
    Extracts data from the CSV file and logs the process.
    """
    try:
        data = pd.read_csv(file_path, encoding='utf-8')
        logging.info(f"Successfully extracted data from {file_path}.")
        return data
    except FileNotFoundError as fnf_error:
        logging.error(f"File not found: {fnf_error}")
        raise
    except Exception as e:
        logging.error(f"Error extracting data: {e}", exc_info=True)
        raise

def validate_data(df):
    """
    Validates the extracted data by checking for missing columns, 
    null values, data types, and valid ranges specific to the pest and weather dataset.
    """
    required_columns = [
        'Observation Year', 'Standard Week', 'Pest Value', 'MaxT', 'MinT', 
        'RH1(%)', 'RH2(%)', 'RF(mm)', 'WS(kmph)', 'SSH(hrs)', 'EVP(mm)', 
        'PEST NAME', 'Location'
    ]
    
    # Check for missing columns
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")
    
    # Check for missing values 
    if df[required_columns].isna().any().any():
        raise ValueError("There are missing values in required columns.")
    
    # Check that numeric columns have valid values
    numeric_columns = ['Pest Value', 'MaxT', 'MinT', 'RH1(%)', 'RH2(%)', 'RF(mm)', 'WS(kmph)', 'SSH(hrs)', 'EVP(mm)']
    if (df[numeric_columns] < 0).any().any():
        raise ValueError("Numeric columns contain negative values.")

    logging.info("Data validation successful.")
    return True

def main():
    try:
        file_path = r"C:\RICE.csv"  
        df = extract_data(file_path)
        validate_data(df)
        logging.info("Extraction and validation completed successfully.")
        return df
    
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()