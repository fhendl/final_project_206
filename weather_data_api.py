import openmeteo_requests
import requests_cache
import pandas as pd
import sqlite3
from retry_requests import retry

# Setup Open-Meteo API client
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def fetch_weather_data_for_months():
    """
    Fetch weather data for 4 specific months.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    months = [
        ("2023-01-01", "2023-01-31"),  # January
        ("2023-02-01", "2023-02-30"),  # April
        ("2023-03-01", "2023-03-31"),  # July
        ("2023-10-01", "2023-10-31"),  # October
    ]
    combined_data = []

    for start_date, end_date in months:
        params = {
            "latitude": 40.7143,  # Example: New York
            "longitude": -74.006,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "snowfall_sum"],
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timezone": "America/New_York"
        }
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        # Process daily data for the month
        daily = response.Daily()
        daily_temperature_max = daily.Variables(0).ValuesAsNumpy()
        daily_temperature_min = daily.Variables(1).ValuesAsNumpy()
        daily_precipitation = daily.Variables(2).ValuesAsNumpy()
        daily_snowfall = daily.Variables(3).ValuesAsNumpy()

        # Create date range
        date_range = pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq="D"
        )

        # Ensure all arrays are the same length
        min_length = min(len(date_range), len(daily_temperature_max), len(daily_temperature_min), len(daily_precipitation), len(daily_snowfall))
        date_range = date_range[:min_length]
        daily_data = {
            "date": date_range,
            "temperature_max": daily_temperature_max[:min_length],
            "temperature_min": daily_temperature_min[:min_length],
            "precipitation_sum": daily_precipitation[:min_length],
            "snowfall_sum": daily_snowfall[:min_length]
        }

        # Append to combined data
        combined_data.append(pd.DataFrame(daily_data))

    # Concatenate all months into a single DataFrame
    return pd.concat(combined_data, ignore_index=True)

def insert_weather_data(dataframe):
    """
    Insert weather data into the SQLite database, refreshing the table each time.
    """
    conn = sqlite3.connect('project_data.db')
    cur = conn.cursor()

    # Drop the existing weather table if it exists
    print("Dropping existing weather table (if it exists)...")
    cur.execute('DROP TABLE IF EXISTS weather')
    print("Weather table dropped and recreated.")

    # Recreate the weather table
    cur.execute('''
        CREATE TABLE weather (
            date TEXT PRIMARY KEY,
            temperature_max REAL,
            temperature_min REAL,
            precipitation_sum REAL,
            snowfall_sum REAL
        )
    ''')

    # Check for duplicates in the DataFrame
    if dataframe.duplicated(subset='date').any():
        print("Duplicates found in the data! Removing duplicates...")
        dataframe = dataframe.drop_duplicates(subset='date')

    # Insert daily weather data
    for _, row in dataframe.iterrows():
        cur.execute('''
            INSERT INTO weather (date, temperature_max, temperature_min, precipitation_sum, snowfall_sum)
            VALUES (?, ?, ?, ?, ?)
        ''', (row['date'].strftime('%Y-%m-%d'), row['temperature_max'], row['temperature_min'], row['precipitation_sum'], row['snowfall_sum']))
        print(f"Inserted weather data for {row['date']}")

    conn.commit()
    conn.close()
    print("Weather data inserted into the database.")



if __name__ == "__main__":
    weather_df = fetch_weather_data_for_months()
    if weather_df.empty:
        print("No weather data available. Check API response.")
    else:
        print(weather_df.head())  # Preview data
        insert_weather_data(weather_df)
