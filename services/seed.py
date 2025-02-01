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
from db import get_db_connection, initialize_database, create_tables

# Load environment variables
load_dotenv()


def seed_movies():
    """
    Seed the movies table by fetching 250 pages of popular movies
    from the TMDb API if the table is empty.
    Rate limit: 10 pages per second.
    Uses executemany to batch insert movie rows.
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
        batch_rows = []  # collect rows for batch insert

        insert_query = """
            INSERT IGNORE INTO movies 
            (movieId, adult, backdrop_path, original_language, original_title,
            overview, popularity, poster_path, release_date, title, video, vote_average, vote_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for page in range(1, total_pages + 1):
            try:
                data = get_movies_by_page(page)
                movies = data.get("results", [])
                for movie in movies:
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
                    batch_rows.append(values)
                # Batch insert for each page
                if batch_rows:
                    cursor.executemany(insert_query, batch_rows)
                    connection.commit()
                    batch_rows = []  # clear the batch

                pages_processed += 1
                print(f"Seeded page {page} with {len(movies)} movies.")
            except Exception as e:
                print(f"Error seeding page {page}: {e}")
            # Rate limit: after every 10 pages, pause for 1 second.
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
    Uses executemany to batch insert genres.
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
        batch_rows = []
        insert_query = """
            INSERT INTO genres (id, name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name)
        """
        for genre in genres:
            values = (genre.get("id"), genre.get("name"))
            batch_rows.append(values)
        if batch_rows:
            cursor.executemany(insert_query, batch_rows)
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
    deduplicate entries, and insert the data into the persons and movie_credits tables.
    Uses batch inserts and rate limits to 10 requests per second.
    """
    connection = get_db_connection()
    if connection is None:
        print("Failed to connect to DB for seeding cast and crew.")
        return

    try:
        cursor = connection.cursor(dictionary=True)
        # Get all movies from the database.
        cursor.execute("SELECT id, movieId FROM movies")
        movies = cursor.fetchall()
        total_movies = len(movies)
        print(f"Seeding cast and crew for {total_movies} movies...")

        # Prepare SQL for inserting persons.
        persons_insert_query = """
            INSERT IGNORE INTO persons 
            (id, name, original_name, gender, popularity, profile_path, known_for_department)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # Prepare SQL for inserting into movie_credits.
        credits_insert_query = """
            INSERT IGNORE INTO movie_credits 
            (credit_id, movie_id, person_id, type, cast_order, character_name, department, job)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        persons_batch = []
        credits_batch = []
        seen_persons = set()   # To avoid duplicate person insertions.
        seen_credits = set()   # To avoid duplicate credit insertions.

        requests_count = 0
        for index, movie in enumerate(movies, start=1):
            movie_db_id = movie["id"]
            movie_api_id = movie["movieId"]
            try:
                credits = get_movie_credits(movie_api_id)
                # Process cast records.
                for cast in credits.get("cast", []):
                    credit_id = cast.get("credit_id")
                    person_id = cast.get("id")
                    if credit_id in seen_credits:
                        continue
                    seen_credits.add(credit_id)
                    # Insert person if not already inserted.
                    if person_id not in seen_persons:
                        # Some cast entries might not have a 'known_for_department' field;
                        # if missing, default to "Acting".
                        known_for = cast.get(
                            "known_for_department") or "Acting"
                        persons_batch.append((
                            person_id,
                            cast.get("name"),
                            cast.get("original_name"),
                            cast.get("gender"),
                            cast.get("popularity"),
                            cast.get("profile_path"),
                            known_for
                        ))
                        seen_persons.add(person_id)
                    # Prepare credit record for a cast entry.
                    credits_batch.append((
                        credit_id,
                        movie_db_id,
                        person_id,
                        "cast",
                        cast.get("order"),
                        cast.get("character"),
                        None,  # department (NULL for cast)
                        None   # job (NULL for cast)
                    ))
                # Process crew records.
                for crew in credits.get("crew", []):
                    credit_id = crew.get("credit_id")
                    person_id = crew.get("id")
                    if credit_id in seen_credits:
                        continue
                    seen_credits.add(credit_id)
                    if person_id not in seen_persons:
                        persons_batch.append((
                            person_id,
                            crew.get("name"),
                            crew.get("original_name"),
                            crew.get("gender"),
                            crew.get("popularity"),
                            crew.get("profile_path"),
                            crew.get("known_for_department")
                        ))
                        seen_persons.add(person_id)
                    # Prepare credit record for a crew entry.
                    credits_batch.append((
                        credit_id,
                        movie_db_id,
                        person_id,
                        "crew",
                        None,  # cast_order (NULL for crew)
                        None,  # character (NULL for crew)
                        crew.get("department"),
                        crew.get("job")
                    ))
                # Batch insert persons and credits for this movie.
                if persons_batch:
                    cursor.executemany(persons_insert_query, persons_batch)
                    persons_batch = []  # Clear after insertion.
                if credits_batch:
                    cursor.executemany(credits_insert_query, credits_batch)
                    credits_batch = []  # Clear after insertion.
                connection.commit()
                print(f"Seeded cast and crew for movie {
                      movie_api_id} ({index}/{total_movies}).")
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
    Uses batch inserts and rate limits to 10 requests per second.
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
        image_insert_query = """
            INSERT IGNORE INTO images 
            (movie_id, type, file_path, aspect_ratio, height, width, vote_average, vote_count, iso_639_1)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        image_batch = []
        requests_count = 0
        for index, movie in enumerate(movies, start=1):
            movie_db_id = movie["id"]
            movie_api_id = movie["movieId"]
            try:
                images_data = get_movie_images(movie_api_id)
                # Process each image type: backdrops, logos, posters.
                for image_type in ['backdrops', 'logos', 'posters']:
                    for image in images_data.get(image_type, []):
                        # Normalize type: remove trailing 's' if present.
                        type_value = image_type[:-
                                                1] if image_type.endswith('s') else image_type
                        image_values = (
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
                        image_batch.append(image_values)
                if image_batch:
                    cursor.executemany(image_insert_query, image_batch)
                    connection.commit()
                    image_batch = []
                print(f"Seeded images for movie {
                      movie_api_id} ({index}/{total_movies}).")
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
    initialize_database()
    create_tables()
    seed_movies()
    seed_genres()
    seed_cast_and_crew()
    seed_images()
    print("DB seeding completed.")
