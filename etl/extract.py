import requests
from datetime import datetime
import json
import logging
from config.config import Config
from db.connection import DatabaseConnection as DBConnection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
class WeatherExtractor:
    
    def __init__(self, config: Config):
        self.config = config
        self.api_key = config.get_api_key()
        self.cities = config.get_cities()
        self.api_url = config.get_api_url()

    def fetch_weather_for_city(self):
        weather_data = []
        for city in self.cities:
            try:
                response = requests.get(
                    self.api_url,
                    params={
                        'q': city,
                        'appid': self.api_key,
                        'units': 'metric'
                    }
                )
                response.raise_for_status()
                data = response.json()
                
                if data.get('cod') != 200:
                    logger.warning(f"Error fetching data for {city}: {data.get('message')}")
                    continue
                
                weather_info = {
                    'city': city,
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'weather': data['weather'][0]['description'],
                    'timestamp': datetime.now()
                }
                weather_data.append(weather_info)
            except requests.RequestException as e:
                print(f"Request error for {city}: {e}")
            except Exception as e:
                print(f"Error processing data for {city}: {e}")
        
        return weather_data

    def fetch_and_store_raw(self):
        weather_data = self.fetch_weather_for_city()
        target_cities = [city.lower() for city in self.cities]
        json_data = json.dumps(weather_data, default=str)
        if not weather_data:
            print("No data fetched.")
            return
        
        try:
            with DBConnection(self.config) as db:
                for data in weather_data:
                    city = data['city'].lower()
                    if city in target_cities:
                        # Convert the data to a JSON string for storage
                        json_data = json.dumps(data, default=str)
                        
                        # Insert the raw weather data into the database
                    query = """
                    INSERT INTO weather_raw (city, raw_json)
                    VALUES (%s, %s)
                    """
                    db.execute(query, (
                        data['city'], 
                        json_data  # Store as string representation of the dict
                    ))
                logger.info("Successfully Inserted Data to Database")
        except Exception as e:
            logger.error(f"Error storing data: {e}")