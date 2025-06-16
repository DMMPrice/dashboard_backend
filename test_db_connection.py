import mysql.connector
import sys
print("Python version:", sys.version)
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection(db_name):
    config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': db_name
    }
    
    try:
        conn = mysql.connector.connect(**config)
        if conn.is_connected():
            print(f"Successfully connected to {db_name}")
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"Tables in {db_name}:", [table[0] for table in tables])
        conn.close()
    except Exception as e:
        print(f"Error connecting to {db_name}:", str(e))

# Test both databases
for db in os.getenv('DB_NAMES').split(','):
    test_connection(db.strip())