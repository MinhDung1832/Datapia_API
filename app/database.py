import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    conn = pyodbc.connect(
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};ENCRYPT=no;TrustServerCertificate=yes;"
    )
    return conn

def get_db_gw_connection():
    conn = pyodbc.connect(
        f"DRIVER={os.getenv('DB_DRIVER_GW')};"
        f"SERVER={os.getenv('DB_SERVER_GW')};"
        f"DATABASE={os.getenv('DB_NAME_GW')};"
        f"UID={os.getenv('DB_USER_GW')};"
        f"PWD={os.getenv('DB_PASSWORD_GW')};ENCRYPT=no;Trusted_Connection=yes"
    )
    return conn
