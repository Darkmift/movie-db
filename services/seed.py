import time
import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Import API functions from your movie_api module.
from movie_api import (
    get_all_genres,
    get_movies_by_page,
    get_movie_credits,
    get_movie_images
)
# Import database connection function.
from db import get_db_connection

# Load environment variables
load_dotenv()

def seed_movies():
    """
    Seed the movies table by fetching 250 pages of popular movies
    from the TMDb API if the table is empty.
    Rate limit: 10 pages per second.
    """
    connection = get_db_connection()
    if connection is None:
        print("Failed to connect to DB for seeding movies.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM movies")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Movies table already seeded with {count} records.")
            return

        print("Seeding movies data from API...")
        total_pages = 250
        pages_processed = 0
        for page in range(1, total_pages + 1):
            try:
                data = get_movies_by_page(page)
                movies = data.get("results", [])
                for movie in movies:
                    # Insert movie data.
                    # We use the API's movie id as movieId and let our internal id be auto-generated.
                    insert_query = """
                        INSERT IGNORE INTO movies 
                        (movieId, adult, backdrop_path, original_language, original_title,
                        overview, popularity, poster_path, release_date, title, video, vote_average, vote_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
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
                    )
                    cursor.execute(insert_query, values)
                connection.commit()
                pages_processed += 1
                print(f"Seeded page {page} with {len(movies)} movies.")
            except Exception as e:
                print(f"Error seeding page {page}: {e}")
            # After every 10 pages, pause for 1 second.
            if pages_processed % 10 == 0:
                time.sleep(1)
    except Exception as e:
        print("Error seeding movies:", e)
    finally:
        cursor.close()
        connection.close()
        print("Movies seeding completed.")

def seed_genres():
    """
    Seed the genres table by querying the TMDb API.
    """
    connection = get_db_connection()
    if connection is None:
        print("Failed to connect to DB for seeding genres.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM genres")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"Genres table already seeded with {count} records.")
            return

        print("Seeding genres data from API...")
        data = get_all_genres()
        genres = data.get("genres", [])
        for genre in genres:
            insert_query = """
                INSERT INTO genres (id, name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE name = VALUES(name)
            """
            values = (genre.get("id"), genre.get("name"))
            cursor.execute(insert_query, values)
        connection.commit()
        print(f"Seeded {len(genres)} genres.")
    except Exception as e:
        print("Error seeding genres:", e)
    finally:
        cursor.close()
        connection.close()

def seed_cast_and_crew():
    """
    For each movie in the DB, query the TMDb API for cast and crew,
    and insert the data into movie_cast and movie_crew tables.
    Rate limit: 10 requests per second.
    """
    connection = get_db_connection()
    if connection is None:
        print("Failed to connect to DB for seeding cast and crew.")
        return

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id, movieId FROM movies")
        movies = cursor.fetchall()
        total_movies = len(movies)
        print(f"Seeding cast and crew for {total_movies} movies...")
        requests_count = 0
        for index, movie in enumerate(movies, start=1):
            movie_db_id = movie["id"]
            movie_api_id = movie["movieId"]
            try:
                credits = get_movie_credits(movie_api_id)
                # Seed cast records.
                cast_list = credits.get("cast", [])
                for cast in cast_list:
                    insert_query = """
                        INSERT IGNORE INTO movie_cast 
                        (credit_id, movie_id, person_id, cast_order, character_name, name, original_name, gender, popularity, profile_path)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                        cast.get("credit_id"),
                        movie_db_id,
                        cast.get("id"),
                        cast.get("order"),
                        cast.get("character"),
                        cast.get("name"),
                        cast.get("original_name"),
                        cast.get("gender"),
                        cast.get("popularity"),
                        cast.get("profile_path")
                    )
                    cursor.execute(insert_query, values)
                # Seed crew records.
                crew_list = credits.get("crew", [])
                for crew in crew_list:
                    insert_query = """
                        INSERT IGNORE INTO movie_crew 
                        (credit_id, movie_id, person_id, department, job, name, original_name, gender, popularity, profile_path)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (
                        crew.get("credit_id"),
                        movie_db_id,
                        crew.get("id"),
                        crew.get("department"),
                        crew.get("job"),
                        crew.get("name"),
                        crew.get("original_name"),
                        crew.get("gender"),
                        crew.get("popularity"),
                        crew.get("profile_path")
                    )
                    cursor.execute(insert_query, values)
                connection.commit()
                print(f"Seeded cast and crew for movie {movie_api_id} ({index}/{total_movies}).")
            except Exception as e:
                print(f"Error seeding cast/crew for movie {movie_api_id}: {e}")
            requests_count += 1
            if requests_count % 10 == 0:
                time.sleep(1)
    except Exception as e:
        print("Error seeding cast and crew:", e)
    finally:
        cursor.close()
        connection.close()

def seed_images():
    """
    For each movie in the DB, query the TMDb API for images and insert them
    into the images table.
    Rate limit: 10 requests per second.
    """
    connection = get_db_connection()
    if connection is None:
        print("Failed to connect to DB for seeding images.")
        return

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id, movieId FROM movies")
        movies = cursor.fetchall()
        total_movies = len(movies)
        print(f"Seeding images for {total_movies} movies...")
        requests_count = 0
        for index, movie in enumerate(movies, start=1):
            movie_db_id = movie["id"]
            movie_api_id = movie["movieId"]
            try:
                images_data = get_movie_images(movie_api_id)
                # For each image type: backdrops, logos, posters.
                for image_type in ['backdrops', 'logos', 'posters']:
                    for image in images_data.get(image_type, []):
                        insert_query = """
                            INSERT IGNORE INTO images 
                            (movie_id, type, file_path, aspect_ratio, height, width, vote_average, vote_count, iso_639_1)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        # Normalize the type value (remove trailing 's' if present)
                        type_value = image_type[:-1] if image_type.endswith('s') else image_type
                        values = (
                            movie_db_id,
                            type_value,
                            image.get("file_path"),
                            image.get("aspect_ratio"),
                            image.get("height"),
                            image.get("width"),
                            image.get("vote_average"),
                            image.get("vote_count"),
                            image.get("iso_639_1")
                        )
                        cursor.execute(insert_query, values)
                connection.commit()
                print(f"Seeded images for movie {movie_api_id} ({index}/{total_movies}).")
            except Exception as e:
                print(f"Error seeding images for movie {movie_api_id}: {e}")
            requests_count += 1
            if requests_count % 10 == 0:
                time.sleep(1)
    except Exception as e:
        print("Error seeding images:", e)
    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    print("Starting DB seeding process...")
    seed_movies()
    seed_genres()
    seed_cast_and_crew()
    seed_images()
    print("DB seeding completed.")
