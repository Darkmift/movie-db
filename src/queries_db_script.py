import os
import time
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Import your API data methods (adjust name to your actual file/module)
from api_data_retrieve import (
    get_all_genres,
    get_movies_by_page,
    get_movie_credits,
    get_movie_images,
)

# If you have a separate DB connection module, import it here
# Otherwise, define your DB connection inline
# e.g. from db import get_db_connection, initialize_database, create_tables
# For this example, we'll define a simple connector inline.

load_dotenv()


def get_db_connection():
    """
    Creates and returns a MySQL database connection.
    Adjust host, user, password, and db name as needed.
    """
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
        if conn.is_connected():
            return conn
    except Error as e:
        print(f"Error connecting to DB: {e}")
    return None


#
# QUERY 1: SEED MOVIES
#
def query_1_seed_movies():
    """
    Inserts popular movies into the 'movies' table by fetching pages
    from the TMDb API. Demonstrates a 'seeding' query function.
    """
    connection = get_db_connection()
    if not connection:
        print("Failed to connect to DB for seeding movies.")
        return

    cursor = connection.cursor()
    try:
        # Check if the table already has records
        cursor.execute("SELECT COUNT(*) FROM movies")
        existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            print(f"Movies table already contains {
                  existing_count} records. Skipping seeding.")
            return

        print("Seeding 'movies' data from TMDb API...")
        total_pages = 5  # example: fetch 5 pages => ~100 movies
        insert_query = """
            INSERT IGNORE INTO movies
            (movieId, adult, backdrop_path, original_language, original_title,
            overview, popularity, poster_path, release_date, title, video,
            vote_average, vote_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for page in range(1, total_pages + 1):
            try:
                data = get_movies_by_page(page)
                movies = data.get("results", [])
                batch_values = []
                for movie in movies:
                    batch_values.append((
                        movie.get("id"),
                        movie.get("adult"),
                        movie.get("backdrop_path"),
                        movie.get("original_language"),
                        movie.get("original_title"),
                        movie.get("overview"),
                        movie.get("popularity"),
                        movie.get("poster_path"),
                        movie.get("release_date"),
                        movie.get("title"),
                        movie.get("video"),
                        movie.get("vote_average"),
                        movie.get("vote_count")
                    ))
                cursor.executemany(insert_query, batch_values)
                connection.commit()
                print(f"Seeded page {page} with {len(movies)} movies.")

                # Simple rate-limit: sleep after each page
                time.sleep(0.5)

            except Exception as e:
                print(f"Error fetching page {page}: {e}")

    except Exception as e:
        print("Error seeding movies:", e)
    finally:
        cursor.close()
        connection.close()
        print("Finished seeding 'movies'.")


#
# QUERY 2: SEED GENRES
#
def query_2_seed_genres():
    """
    Inserts genre data into the 'genres' table by fetching from the TMDb API.
    """
    connection = get_db_connection()
    if not connection:
        print("Failed to connect to DB for seeding genres.")
        return

    cursor = connection.cursor()
    try:
        # Check if the table already has records
        cursor.execute("SELECT COUNT(*) FROM genres")
        existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            print(f"Genres table already contains {
                  existing_count} records. Skipping seeding.")
            return

        print("Seeding 'genres' data from TMDb API...")
        data = get_all_genres()
        genres = data.get("genres", [])

        insert_query = """
            INSERT INTO genres (id, name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name)
        """
        batch_values = [(g["id"], g["name"]) for g in genres]
        cursor.executemany(insert_query, batch_values)
        connection.commit()
        print(f"Seeded {len(genres)} genres.")
    except Exception as e:
        print("Error seeding genres:", e)
    finally:
        cursor.close()
        connection.close()
        print("Finished seeding 'genres'.")


#
# QUERY 3: SEED CAST & CREW
#
def query_3_seed_cast_and_crew():
    """
    For each movie in 'movies', fetch the cast & crew from the TMDb API
    and insert them into 'persons' and 'movie_credits' tables.
    """
    connection = get_db_connection()
    if not connection:
        print("Failed to connect to DB for seeding cast and crew.")
        return

    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id, movieId FROM movies")
        movie_rows = cursor.fetchall()
        total_movies = len(movie_rows)
        print(f"Seeding cast and crew for {total_movies} movies...")

        # Prepare SQL for inserting persons
        persons_insert_query = """
            INSERT IGNORE INTO persons
            (id, name, original_name, gender, popularity, profile_path, known_for_department)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # Prepare SQL for inserting into movie_credits
        credits_insert_query = """
            INSERT IGNORE INTO movie_credits
            (credit_id, movie_id, person_id, type, cast_order, character_name, department, job)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Rate-limiting counters
        requests_count = 0

        for idx, movie in enumerate(movie_rows, start=1):
            db_movie_id = movie["id"]       # local DB ID
            api_movie_id = movie["movieId"]  # TMDb ID

            try:
                credits_data = get_movie_credits(api_movie_id)

                # Cast batch
                persons_batch = []
                credits_batch = []

                # Process cast
                for cast_member in credits_data.get("cast", []):
                    person_id = cast_member.get("id")
                    credit_id = cast_member.get("credit_id")

                    # Person record
                    persons_batch.append((
                        person_id,
                        cast_member.get("name"),
                        cast_member.get("original_name"),
                        cast_member.get("gender"),
                        cast_member.get("popularity"),
                        cast_member.get("profile_path"),
                        cast_member.get("known_for_department", "Acting")
                    ))

                    # Credit record
                    credits_batch.append((
                        credit_id,
                        db_movie_id,
                        person_id,
                        "cast",
                        cast_member.get("order"),
                        cast_member.get("character"),
                        None,  # department (NULL for cast)
                        None   # job (NULL for cast)
                    ))

                # Process crew
                for crew_member in credits_data.get("crew", []):
                    person_id = crew_member.get("id")
                    credit_id = crew_member.get("credit_id")

                    persons_batch.append((
                        person_id,
                        crew_member.get("name"),
                        crew_member.get("original_name"),
                        crew_member.get("gender"),
                        crew_member.get("popularity"),
                        crew_member.get("profile_path"),
                        crew_member.get("known_for_department", "Production")
                    ))

                    credits_batch.append((
                        credit_id,
                        db_movie_id,
                        person_id,
                        "crew",
                        None,
                        None,
                        crew_member.get("department"),
                        crew_member.get("job")
                    ))

                # Execute batch inserts
                if persons_batch:
                    cursor.executemany(persons_insert_query, persons_batch)
                if credits_batch:
                    cursor.executemany(credits_insert_query, credits_batch)
                connection.commit()

                requests_count += 1
                # Simple rate limit
                if requests_count % 10 == 0:
                    time.sleep(1)

                print(
                    f"Seeded cast/crew for movie {api_movie_id} ({idx}/{total_movies}).")

            except Exception as ex:
                print(
                    f"Error seeding cast/crew for movie {api_movie_id}: {ex}")

    except Exception as e:
        print("Error fetching movies from DB:", e)
    finally:
        cursor.close()
        connection.close()
        print("Finished seeding 'cast and crew'.")


#
# QUERY 4: SEED IMAGES
#
def query_4_seed_images():
    """
    For each movie in 'movies', fetch images from the TMDb API
    and insert them into the 'images' table.
    """
    connection = get_db_connection()
    if not connection:
        print("Failed to connect to DB for seeding images.")
        return

    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id, movieId FROM movies")
        movie_rows = cursor.fetchall()
        total_movies = len(movie_rows)
        print(f"Seeding images for {total_movies} movies...")

        insert_query = """
            INSERT IGNORE INTO images
            (movie_id, type, file_path, aspect_ratio, height, width,
            vote_average, vote_count, iso_639_1)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        requests_count = 0

        for idx, movie in enumerate(movie_rows, start=1):
            db_movie_id = movie["id"]
            api_movie_id = movie["movieId"]

            try:
                images_data = get_movie_images(api_movie_id)

                batch_values = []
                for image_type in ['backdrops', 'logos', 'posters']:
                    for img in images_data.get(image_type, []):
                        # remove trailing 's' to get singular type
                        normalized_type = (image_type[:-1]
                                            if image_type.endswith('s')
                                            else image_type)
                        batch_values.append((
                            db_movie_id,
                            normalized_type,
                            img.get("file_path"),
                            img.get("aspect_ratio"),
                            img.get("height"),
                            img.get("width"),
                            img.get("vote_average"),
                            img.get("vote_count"),
                            img.get("iso_639_1")
                        ))

                if batch_values:
                    cursor.executemany(insert_query, batch_values)
                    connection.commit()

                requests_count += 1
                if requests_count % 10 == 0:
                    time.sleep(1)

                print(f"Seeded images for movie {
                      api_movie_id} ({idx}/{total_movies}).")

            except Exception as ex:
                print(f"Error seeding images for movie {api_movie_id}: {ex}")

    except Exception as e:
        print("Error fetching movies from DB:", e)
    finally:
        cursor.close()
        connection.close()
        print("Finished seeding 'images'.")


#
# QUERY 5: (EXAMPLE) A READ-ONLY QUERY
#          This is a placeholder for your final "report" or "analysis" query.
#
def query_5_example_report():
    """
    Example read-only query that might do something interesting:
    e.g., find the top 5 most popular movies. 
    This isn't a 'seed' function; it's a typical SELECT query.
    """
    connection = get_db_connection()
    if not connection:
        print("Failed to connect for query_5_example_report.")
        return []

    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT id, title, popularity, vote_count
            FROM movies
            ORDER BY popularity DESC
            LIMIT 5
        """)
        results = cursor.fetchall()
        return results
    except Error as e:
        print(f"Error running query_5_example_report: {e}")
        return []
    finally:
        cursor.close()
        connection.close()


def main():
    """
    Demonstrates calling each query (i.e., each 'seeding' routine)
    in sequence, plus an example read-only query.
    """
    print("Starting DB seeding / queries...")

    # 1) Seed the 'movies' table
    query_1_seed_movies()

    # 2) Seed the 'genres' table
    query_2_seed_genres()

    # 3) Seed cast & crew
    query_3_seed_cast_and_crew()

    # 4) Seed images
    query_4_seed_images()

    # 5) Run an example read query
    top_5 = query_5_example_report()
    print("\nTop 5 Most Popular Movies in DB:")
    for row in top_5:
        print(f" - {row['title']} (popularity: {row['popularity']}, vote_count: {row['vote_count']})")

    print("All queries finished.")


if __name__ == '__main__':
    main()
