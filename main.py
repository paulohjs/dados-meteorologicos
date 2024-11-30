from datetime import datetime, timedelta
from meteostat import Point, Hourly
import pandas as pd
import logging
import json


logging.basicConfig(filename='pipeline.log', level=logging.INFO, format="%(asctime)s - %(funcName)s - %(levelname)s - %(message)s ")


def load_parquet_weather_data(parquet_file_path: str) -> pd.DataFrame:
    """
    Loads weather data from a Parquet file into a DataFrame.

    Args:
        parquet_file_path (str): Path to the Parquet file containing the weather data.

    Returns:
        pd.DataFrame: A DataFrame containing the weather data loaded from the Parquet file.
    """
    try:
        logging.info(f'Loading weather data from parquet file: {parquet_file_path}')
        parquet_weather_data = pd.read_parquet(parquet_file_path)
        logging.info(f'Loaded {len(parquet_weather_data)} rows of weather data.')
    except Exception as e:
        logging.error(f'Error loading parquet file {parquet_file_path}: {e}')
        parquet_weather_data = pd.DataFrame # Return empty DataFrame on failure
        logging.warning(f'Create a new empty Dataframe to save as parquet file')
    return parquet_weather_data


def get_last_datetime_by_city(parquet_weather_data: pd.DataFrame, city_name: str) -> datetime:
    """
    Gets the datetime of the most recent weather measurement for a specific city.

    Args:
        parquet_weather_data (pd.DataFrame): DataFrame containing weather data for multiple cities.
        city_name (str): Name of the city for which we want the latest datetime of weather measurement.

    Returns:
        datetime: A datetime object representing the most recent weather measurement for the specified city.
    """
    logging.info(f'Geting last weather data time registered for {city_name}')
    city_weather_data = parquet_weather_data[parquet_weather_data['city'] == city_name]
    if not city_weather_data.empty:
        try:
            last_datetime_by_city = str(city_weather_data['time'].max())
            last_datetime_by_city = datetime.strptime(last_datetime_by_city, "%Y-%m-%d %H:%M:%S")
            logging.info(f'The last weather data registered for {city_name} was {last_datetime_by_city}')
        except Exception as e:
            logging.error(f'An error eccurred while geting last time data: {e}')
            raise
    else:
        # Return now datetime
        last_datetime_by_city = datetime(2021,1,1)
    return last_datetime_by_city


def load_cities_info_from_json(cities_geocode_file: str) -> dict:
    """
    Loads city geocode information (city name, latitude and longitude) from a JSON file.

    Args:
        cities_geocode_file (str): Path to the JSON file containing geocode information for cities.

    Returns:
        dict: A dictionary with city names as keys and their respective geocode information (latitude and longitude) as values.
    """
    logging.info(f"Loading city geocode information from file: {cities_geocode_file}")
    try:
        with open(cities_geocode_file, 'r', encoding='utf-8') as cities_geocode_json:
            cities_geocode = json.load(cities_geocode_json)
        logging.info(f"Loaded geocode information for {len(cities_geocode)} cities.")
    except Exception as e:
        logging.error(f"Error loading geocode file {cities_geocode_file}: {e}")
        raise
    return cities_geocode


def fetch_hourly_data_from_meteostat_by_city(start_datetime: datetime, end_datetime: datetime, city_name: str, latitude: float, longitude: float) -> pd.DataFrame:
    """
    Fetches the latest weather data for a specific city from the Meteostat API.

    Args:
        start_datetime (datetime): Start datetime for the weather data query.
        end_datetime (datetime): End datetime for the weather data query.
        city_name (str): Name of the city for which the weather data will be fetched.
        latitude (float): Latitude of the city.
        longitude (float): Longitude of the city.

    Returns:
        pd.DataFrame: A DataFrame containing the weather data fetched from the Meteostat API.
    """
    # Replacing minutes seconds and microseconds to zero
    end_datetime = end_datetime.replace(minute=0, second=0, microsecond=0)
    # Adding 1 hour on last date from parquet
    start_datetime = start_datetime + timedelta(hours=1)

    if not start_datetime == end_datetime:
        logging.info(f"Fetching weather data for {city_name} from {start_datetime} to {end_datetime}.")
        try:
            point = Point(latitude, longitude)
            meteostat_data_by_city = Hourly(point, start_datetime, end_datetime)
            meteostat_data_by_city = meteostat_data_by_city.fetch()
            if meteostat_data_by_city.empty:
                logging.warning(f"No weather data returned for {city_name} in the specified period.")
            else:
                logging.info(f"Fetched {len(meteostat_data_by_city)} rows of weather data for {city_name}.")
                # Column with city name created on dataframe
                meteostat_data_by_city['city'] = city_name
                logging.info(f'Column with city name created on dataframe')
                # Reset index (time) as a simple column
                meteostat_data_by_city = meteostat_data_by_city.reset_index()
                logging.info(f'Dataframe index reseted')
                # Select only the usual columns
                meteostat_data_by_city = meteostat_data_by_city[['city', 'time', 'temp', 'rhum', 'prcp', 'wspd']]
                logging.info(f'Columns selected from dataframe')
        except Exception as e:
            logging.error(f"Failed to fetch weather data for {city_name}: {e}")
            meteostat_data_by_city = pd.DataFrame()  # Return empty DataFrame on failure
        return meteostat_data_by_city


def concat_weather_data_by_city(meteostat_data_by_city: pd.DataFrame, parquet_weather_data: pd.DataFrame) -> pd.DataFrame:
    """
    Concatenates the latest weather data for a city into the main weather data DataFrame.

    Args:
        meteostat_data_by_city (pd.DataFrame): DataFrame containing the latest weather data fetched from the Meteostat API.
        parquet_weather_data (pd.DataFrame): DataFrame containing the accumulated weather data for all cities.

    Returns:
        pd.DataFrame: An updated DataFrame with the new weather data for the city concatenated.
    """
    try:
        if not meteostat_data_by_city.empty:
            logging.info(f"Concatenating new weather data for city {meteostat_data_by_city['city'].iloc[0]}.")
            # Concatenate the city's weather data into the main DataFrame
            if parquet_weather_data.empty:
                parquet_weather_data = meteostat_data_by_city
            else:
                parquet_weather_data = pd.concat([parquet_weather_data, meteostat_data_by_city])
                parquet_weather_data = parquet_weather_data.reset_index(drop=True)
            logging.info(f"Added {len(meteostat_data_by_city)} new rows to the weather data.")
    except Exception as e:
        logging.error(f'An error eccurred while concatenate data into the main DataFrame: {e}')
        raise
    return parquet_weather_data


def save_concatenated_data_to_parquet(parquet_weather_data: pd.DataFrame, parquet_file_name: str) -> None:
    """
    Saves the concatenated weather data to a Parquet file.

    Args:
        parquet_weather_data (pd.DataFrame): DataFrame containing the concatenated weather data.
        parquet_file_name (str): Path to the Parquet file where the data will be saved.
    """
    logging.info(f"Saving concatenated weather data to Parquet file: {parquet_file_name}")
    try:
        parquet_weather_data.to_parquet(parquet_file_name)
    except Exception as e:
        logging.error(f'An error eccurred while save main DataFrame as parquet: {e}')
        raise
    logging.info(f"Successfully saved {len(parquet_weather_data)} rows to {parquet_file_name}.")


def main():
    """
    Main function that runs the weather data pipeline.

    The pipeline loads existing weather data from a Parquet file, queries the latest weather data
    for cities using the Meteostat API, and concatenates the new data into the main Parquet file. 
    After processing, the updated data is saved back to the Parquet file.
    """
    logging.info('Start pipeline')
    parquet_file_name = 'weather_data.parquet'
    cities_geocode_file = 'cities_geocode.json'
    # Load existing weather data from the Parquet file
    parquet_weather_data = load_parquet_weather_data(parquet_file_name)
    # Load city geocode information from the JSON file
    cities_infos = load_cities_info_from_json(cities_geocode_file)
    # For each city, fetch the latest weather data and concatenate it into the main DataFrame
    for name, infos in cities_infos.items():
        latitude = infos['latitude']
        longitude = infos['longitude']
        end_datetime = datetime.now()
        # Get the last datetime of weather measurement for the city
        last_datetime_by_city = get_last_datetime_by_city(parquet_weather_data, name)
        # Fetch new weather data from Meteostat API
        meteostat_data_by_city = fetch_hourly_data_from_meteostat_by_city(last_datetime_by_city, end_datetime, name, latitude, longitude)
        if meteostat_data_by_city.empty:
            continue
        # Concatenate the new data into the main DataFrame
        parquet_weather_data = concat_weather_data_by_city(meteostat_data_by_city, parquet_weather_data)
        # Save the concatenated data back to the Parquet file
        save_concatenated_data_to_parquet(parquet_weather_data, parquet_file_name)
    logging.info('End pipeline')


if __name__ == '__main__':
    main()
