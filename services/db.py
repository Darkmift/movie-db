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
    This version uses a 'persons' table and a unified 'movie_credits' table to
    capture both cast and crew credit lines.
    """
    connection = get_db_connection()
    if connection is None:
        print("Cannot create tables because the connection failed.")
        return

    try:
        cursor = connection.cursor()

        # Movies table
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

        # Genres table
        print("Creating 'genres' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            );
        """)
        print("'genres' table created or already exists.")

        # Join table for movies and genres.
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

        # Images table
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

        # Persons table: stores unique information about individuals.
        print("Creating 'persons' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS persons (
                id INT PRIMARY KEY, -- API's person ID.
                name VARCHAR(255),
                original_name VARCHAR(255),
                gender INT,
                popularity DECIMAL(8,3),
                profile_path VARCHAR(255),
                known_for_department VARCHAR(100)
            );
        """)
        print("'persons' table created or already exists.")

        # Movie credits table: unifies cast and crew credit lines.
        print("Creating 'movie_credits' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_credits (
                credit_id VARCHAR(50) PRIMARY KEY,  -- Unique credit line ID from the API.
                movie_id INT,
                person_id INT,
                type ENUM('cast', 'crew') NOT NULL,
                cast_order INT,           -- For cast records; NULL for crew.
                character_name VARCHAR(255),   -- Renamed from 'character'; for cast records; NULL for crew.
                department VARCHAR(100),  -- For crew records; NULL for cast.
                job VARCHAR(100),         -- For crew records; NULL for cast.
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
            );
        """)
        print("'movie_credits' table created or already exists.")

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
