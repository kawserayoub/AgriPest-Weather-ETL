import pandas as pd
from extract import extract_data, validate_data
from transform import transform_data
from load import load_data

def get_sample_data():
    """Return sample pest and weather data used for testing as a DataFrame."""
    sample_data = {
        'Observation Year': [2023, 2023],
        'Standard Week': [1, 2],
        'Pest Value': [10, 15],
        'MaxT': [35.5, 32.0],
        'MinT': [22.0, 20.5],
        'RH1(%)': [85, 80],
        'RH2(%)': [75, 70],
        'RF(mm)': [5, 0],
        'WS(kmph)': [12.5, 8.0],
        'SSH(hrs)': [7, 6.5],
        'EVP(mm)': [4, 3.2],
        'PEST NAME': ['Brownplanthopper', 'Brownplanthopper'],
        'Location': ['Cuttack', 'Cuttack']
    }
    return pd.DataFrame(sample_data)

def test_extract_data():
    """Test the extract function."""
    file_path = r"C:\RICE.csv"
    df_extracted = extract_data(file_path)
    assert isinstance(df_extracted, pd.DataFrame), "Data extraction failed to return a DataFrame."

def test_validate_data():
    """Test the validate function."""
    df = get_sample_data()
    validate_result = validate_data(df)
    assert validate_result == True, "Data validation failed."

def test_transform_data():
    """Test the transform function."""
    df = get_sample_data()
    df_transformed, pest_avg, weekly_weather = transform_data(df)
    
    # Check if new columns were added
    assert 'Temp Range' in df_transformed.columns, "Temp Range column missing."
    assert 'Avg Humidity' in df_transformed.columns, "Avg Humidity column missing."
    
    # Check if aggregations were created
    assert len(pest_avg) > 0, "No pest data aggregated."
    assert len(weekly_weather) > 0, "No weekly weather data aggregated."

def test_load_data():
    """Test the load function."""
    df = get_sample_data()
    try:
        load_data(df, 'test_table')
        assert True, "Data loaded successfully."
    except Exception as e:
        assert False, f"Loading data to SQL failed: {e}"

if __name__ == "__main__":
    test_extract_data()
    test_validate_data()
    test_transform_data()
    test_load_data()
