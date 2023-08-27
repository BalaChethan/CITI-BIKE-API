import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Function to extract data from Citi Bike API and transform it
def extract_transform_citi_bike(api_url):
    response = requests.get(api_url)
    data = response.json()
    
    # Extract station information
    station_data = data.get('data', {}).get('stations', [])
    
    # Transform station data into a DataFrame
    df = pd.DataFrame(station_data)
    
    return df

# Function to load data into PostgreSQL
def load_to_postgres(data_frame, postgres_config):
    engine = create_engine(
        f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['dbname']}"
    )
    
    data_frame.to_sql('citi_bike_stations', engine, if_exists='replace', index=False)
    
# Main function
def main():
    api_url = 'CITI_BIKE_API_URL'  # Replace with the actual Citi Bike API URL
    postgres_config = {
        'host': 'POSTGRES_HOST',
        'port': 'POSTGRES_PORT',
        'user': 'POSTGRES_USER',
        'password': 'POSTGRES_PASSWORD',
        'dbname': 'POSTGRES_DBNAME'
    }
    
    extracted_data = extract_transform_citi_bike(api_url)
    load_to_postgres(extracted_data, postgres_config)
    
    print("Data extraction, transformation, and loading completed.")

if __name__ == "__main__":
    main()
