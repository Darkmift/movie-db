import dotenv
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv(verbose=True, override=True)


def create_db():
    """
    Connects to MySQL, creates (if needed) a database, and then creates the required tables.
    """

    print("Creating the database and tables...")

    db_host = os.getenv('MYSQL_HOST')
    db_user = os.getenv('MYSQL_USER')
    db_password = os.getenv('MYSQL_PASSWORD')
    db_name = os.getenv('MYSQL_DATABASE')

    print(f"Creating database {db_name}...")

    conn = None
    cursor = None
    try:
        # 1) Connect to the server without specifying the database
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password
        )
        if conn.is_connected():
            print("Successfully connected to the MySQL server.")

        # 2) Create the database if it does not exist
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        print(f"Database '{db_name}' is ready (created or already exists).")

        # 3) Use the newly created or existing database
        cursor.execute(f"USE {db_name};")

        # 4) Create tables inside the chosen database
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
            ) ENGINE=InnoDB;
        """)
        print("'movies' table created or already exists.")

        print("Creating 'genres' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            ) ENGINE=InnoDB;
        """)
        print("'genres' table created or already exists.")

        print("Creating 'movie_genres' table (join table)...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_genres (
                movie_id INT,
                genre_id INT,
                PRIMARY KEY (movie_id, genre_id),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)
        print("'movie_genres' table created or already exists.")

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
            ) ENGINE=InnoDB;
        """)
        print("'images' table created or already exists.")

        print("Creating 'persons' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS persons (
                id INT PRIMARY KEY, 
                name VARCHAR(255),
                original_name VARCHAR(255),
                gender INT,
                popularity DECIMAL(8,3),
                profile_path VARCHAR(255),
                known_for_department VARCHAR(100)
            ) ENGINE=InnoDB;
        """)
        print("'persons' table created or already exists.")

        print("Creating 'movie_credits' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_credits (
                credit_id VARCHAR(50) PRIMARY KEY,
                movie_id INT,
                person_id INT,
                type ENUM('cast', 'crew') NOT NULL,
                cast_order INT,
                character_name VARCHAR(255),
                department VARCHAR(100),
                job VARCHAR(100),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)
        print("'movie_credits' table created or already exists.")

        # Example of creating an index (optional):
        try:
            cursor.execute(
                "CREATE INDEX idx_popularity ON movies (popularity);")
        except mysql.connector.Error as e:
            if e.errno == 1061:  # 1061 = "duplicate key name" in MySQL
                print("Index already exists, skipping.")
            else:
                raise

        conn.commit()  # Commit DDL changes
        print("All tables have been created successfully.")

    except Error as e:
        print(f"Database error: {e}")
    except Exception as ex:
        print(f"General error: {ex}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()
            print("MySQL connection is closed.")


if __name__ == '__main__':
    create_db()
