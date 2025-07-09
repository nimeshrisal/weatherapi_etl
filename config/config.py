import os 
from dotenv import load_dotenv

class Config:

    def __init__(self):
        load_dotenv()

        self.api_key = os.getenv('OPENWEATHER_API_KEY') 
        self.cities = ['kathmandu', 'Delhi', 'London', 'New York', 'Tokyo']
        self.api_url = os.getenv('API_URL')
        self.db_params = {
            "dbname": os.getenv('DB_NAME', 'weather_db'), 
            "user": os.getenv('DB_USER', 'admin'),
            "password": os.getenv('DB_PASSWORD', 'admin'),      
            "host": os.getenv('DB_HOST'),
            "port": os.getenv('DB_PORT', '5432')
        }

    def get_api_key(self):
        return self.api_key
    
    def get_cities(self):
        return self.cities
    
    def get_db_params(self):
        return self.db_params 
      
    def get_api_url(self):
        return self.api_url