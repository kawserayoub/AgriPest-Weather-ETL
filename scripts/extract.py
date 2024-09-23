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
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_message = f"Missing columns: {', '.join(missing_columns)}"
        logging.error(error_message)
        raise ValueError(error_message)
    
    # Check for missing values in critical columns
    for col in required_columns:
        if df[col].isnull().any():
            error_message = f"Missing values in column: {col}"
            logging.error(error_message)
            raise ValueError(error_message)
    
    # Check that numeric columns have valid values
    numeric_columns = ['Pest Value', 'MaxT', 'MinT', 'RH1(%)', 'RH2(%)', 'RF(mm)', 'WS(kmph)', 'SSH(hrs)', 'EVP(mm)']
    for col in numeric_columns:
        if (df[col] < 0).any():
            error_message = f"{col} contains negative values."
            logging.error(error_message)
            raise ValueError(error_message)

    # Validate 'Observation Year' and 'Standard Week' for realistic values
    if (df['Observation Year'] < 1900).any() or (df['Observation Year'] > pd.Timestamp.now().year).any():
        error_message = "Invalid year values found."
        logging.error(error_message)
        raise ValueError(error_message)
    
    if (df['Standard Week'] < 1).any() or (df['Standard Week'] > 52).any():
        error_message = "Invalid week values found."
        logging.error(error_message)
        raise ValueError(error_message)
    
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
