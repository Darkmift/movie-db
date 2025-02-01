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
        print("Error while connecting to MySQL:", e)
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
        print("Error while initializing the database:", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def create_tables():
    """
    Create the necessary tables in the movie_db database if they don't exist.
    """
    connection = get_db_connection()
    if connection is None:
        print("Cannot create tables because the connection failed.")
        return

    try:
        cursor = connection.cursor()

        # Create movies table with an additional unique movieId column.
        print("Creating 'movies' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                movieId INT UNIQUE,
                adult BOOLEAN,
                backdrop_path VARCHAR(255),
                original_language VARCHAR(10),
                original_title VARCHAR(255),
                overview TEXT,
                popularity DECIMAL(8,3),
                poster_path VARCHAR(255),
                release_date DATE,
                title VARCHAR(255),
                video BOOLEAN,
                vote_average DECIMAL(3,1),
                vote_count INT
            );
        """)
        print("'movies' table created or already exists.")

        # Create genres table.
        print("Creating 'genres' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            );
        """)
        print("'genres' table created or already exists.")

        # Create join table for movies and genres.
        print("Creating 'movie_genres' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_genres (
                movie_id INT,
                genre_id INT,
                PRIMARY KEY (movie_id, genre_id),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
            );
        """)
        print("'movie_genres' table created or already exists.")

        # Create images table.
        print("Creating 'images' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id INT AUTO_INCREMENT PRIMARY KEY,
                movie_id INT,
                type ENUM('backdrop', 'logo', 'poster') NOT NULL,
                file_path VARCHAR(255),
                aspect_ratio DECIMAL(5,3),
                height INT,
                width INT,
                vote_average DECIMAL(3,1),
                vote_count INT,
                iso_639_1 VARCHAR(10),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
            );
        """)
        print("'images' table created or already exists.")

        # Create movie_cast table.
        # Use the API's unique credit_id as the primary key.
        print("Creating 'movie_cast' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_cast (
                credit_id VARCHAR(50) PRIMARY KEY,
                movie_id INT,
                person_id INT,
                cast_order INT,
                character_name VARCHAR(255),  -- Renamed from 'character'
                name VARCHAR(255),
                original_name VARCHAR(255),
                gender INT,
                popularity DECIMAL(8,3),
                profile_path VARCHAR(255),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
            );
        """)
        print("'movie_cast' table created or already exists.")

        # Create movie_crew table.
        # Similarly, use the API's unique credit_id as the primary key.
        print("Creating 'movie_crew' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_crew (
                credit_id VARCHAR(50) PRIMARY KEY,
                movie_id INT,
                person_id INT,
                department VARCHAR(100),
                job VARCHAR(100),
                name VARCHAR(255),
                original_name VARCHAR(255),
                gender INT,
                popularity DECIMAL(8,3),
                profile_path VARCHAR(255),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
            );
        """)
        print("'movie_crew' table created or already exists.")

        connection.commit()
        print("All tables have been created successfully.")
    except Error as e:
        print("Error while creating tables:", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")

def execute_query(query, params=None, commit=False, fetchone=False, fetchall=False):
    """
    A helper function to execute a given SQL query using the DB connection.
    
    :param query: SQL query to be executed.
    :param params: Optional parameters to use with the query.
    :param commit: If True, commit the transaction.
    :param fetchone: If True, return a single row.
    :param fetchall: If True, return all rows.
    :return: Result of the query if fetching, otherwise None.
    """
    connection = get_db_connection()
    if connection is None:
        print("Database connection failed.")
        return None

    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(query, params or ())
        if commit:
            connection.commit()
            print("Transaction committed.")
        if fetchone:
            result = cursor.fetchone()
            return result
        if fetchall:
            result = cursor.fetchall()
            return result
    except Error as e:
        print("Error while executing query:", e)
        return None
    finally:
        cursor.close()
        connection.close()
        print("Database connection closed.")

# For quick testing when running this module directly
if __name__ == '__main__':
    print("Initializing database...")
    initialize_database()
    print("Creating tables...")
    create_tables()
