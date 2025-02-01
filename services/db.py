# db.py

import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Load environment variables from .env
load_dotenv()

def get_db_connection():
    """
    Establish a connection to the MySQL database using credentials from .env.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            user=os.getenv('MYSQL_USER', 'root'),
            password=os.getenv('MYSQL_PASSWORD', 'root12345'),
            database=os.getenv('MYSQL_DATABASE', 'movie_db')
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print("Error while connecting to MySQL", e)
        return None

def initialize_database():
    """
    Create the MySQL database if it doesn't exist.
    This function connects without specifying a database, creates the database,
    and then closes the connection.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            user=os.getenv('MYSQL_USER', 'root'),
            password=os.getenv('MYSQL_PASSWORD', 'root12345')
        )
        connection.autocommit = True
        cursor = connection.cursor()
        db_name = os.getenv('MYSQL_DATABASE', 'movie_db')
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' is ready (created or already exists).")
    except Error as e:
        print("Error while initializing the database", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
