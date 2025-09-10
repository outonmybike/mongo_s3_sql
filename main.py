import os 
from dotenv import load_dotenv

load_dotenv()

DOMAIN = os.getenv("DOMAIN")

print(DOMAIN)