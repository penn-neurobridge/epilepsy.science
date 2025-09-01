import os
from dotenv import load_dotenv

load_dotenv('pennsieve.env')

class Config:
    def __init__(self):
        self.API_KEY = os.getenv('PENNSIEVE_API_KEY')
        self.API_SECRET = os.getenv('PENNSIEVE_API_SECRET')
        self.API_HOST = os.getenv('PENNSIEVE_API_HOST', 'https://api.pennsieve.net')
        self.API_HOST2 = os.getenv('PENNSIEVE_API_HOST2', 'https://api2.pennsieve.net')