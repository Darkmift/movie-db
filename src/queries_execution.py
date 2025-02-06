import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from contextlib import redirect_stdout


load_dotenv(verbose=True, override=True)


def get_db_connection():
    """
    Returns a MySQL database connection using env variables:
    MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE.
    """
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
        return conn
    except Error as e:
        print(f"Error connecting to DB: {e}")
        return None

# ------------------------------------------------------------------------------
# QUERY #1 (Complex):
#  Find the single most popular movie (highest popularity),
#  then list the top 10 most popular actors (type='cast') in that movie,
#  sorted by the actors' popularity (descending).
#
#  Output columns: movie_title, actor_name, actor_popularity
# ------------------------------------------------------------------------------


def query_1_most_popular_movie_with_top_actors():
    """
    1) Find the most popular movie.
    2) Return up to 10 most popular actors in that movie, along with actor details.

    Returns a list of dictionaries with keys:
      - movie_title
      - actor_name
      - actor_popularity
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT m.title AS movie_title,
                   p.name AS actor_name,
                   p.popularity AS actor_popularity
            FROM movies m
            JOIN movie_credits mc ON m.id = mc.movie_id
            JOIN persons p ON mc.person_id = p.id
            WHERE mc.type = 'cast'
              AND m.id = (
                  SELECT id
                  FROM movies
                  ORDER BY popularity DESC
                  LIMIT 1
              )
            ORDER BY p.popularity DESC
            LIMIT 10;
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_1: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ------------------------------------------------------------------------------
# QUERY #2 (Complex):
#  For each genre, return:
#    - the total number of movies in that genre
#    - the single most popular movie (by popularity) in that genre
#
#  Output columns: genre_name, movie_count, most_popular_movie
# ------------------------------------------------------------------------------


def query_2_genre_movie_counts_and_top_movie():
    """
    Returns, for each genre:
      1) Genre name
      2) Number of movies of that genre
      3) The title of the most popular movie in that genre

    Example columns in the returned dictionaries:
      - genre_name
      - movie_count
      - most_popular_movie
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT g.name AS genre_name,
                   COUNT(m.id) AS movie_count,
                   (
                     SELECT m2.title
                     FROM movie_genres mg2
                     JOIN movies m2 ON m2.id = mg2.movie_id
                     WHERE mg2.genre_id = g.id
                     ORDER BY m2.popularity DESC
                     LIMIT 1
                   ) AS most_popular_movie
            FROM genres g
            JOIN movie_genres mg ON mg.genre_id = g.id
            JOIN movies m ON m.id = mg.movie_id
            GROUP BY g.id
            ORDER BY movie_count DESC;
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_2: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ------------------------------------------------------------------------------
# QUERY #3 (Full-Text):
#  Search movie titles for a given substring (e.g., "באטמן") using FULLTEXT.
#  Return matching movies, sorted by popularity (descending).
#  Limit to top 10 for demonstration.
#
#  Make sure you have a FULLTEXT index on movies.title!
#
#  Output columns: id, title, popularity
# ------------------------------------------------------------------------------


def query_3_fulltext_search_in_title(substring):
    """
    Full-text search in the 'movies.title' column.
    For best substring matching, we can use a Boolean mode with an asterisk:
      e.g. substring + '*'
    Then order by popularity desc, limit 10.

    Returns a list of dicts with:
      - id
      - title
      - popularity
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        # We'll use Boolean mode for a more flexible match.
        # If you want an exact token match, use NATURAL LANGUAGE MODE instead.
        sql = """
            SELECT id, title, popularity
            FROM movies
            WHERE MATCH(title) AGAINST (%s IN BOOLEAN MODE)
            ORDER BY popularity DESC
            LIMIT 10;
        """
        # Example: if substring = "באטמן", pass "באטמן*" to catch partial matches
        search_term = substring.strip() + "*"

        cursor.execute(sql, (search_term,))
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_3: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ------------------------------------------------------------------------------
# QUERY #4 (Complex):
#  "Divide all persons in the DB and display their roles"
#  Example categories:
#    - שחקנים גברים
#    - שחקניות נשים
#    - במאים גברים
#    - במאיות נשים
#    - ... etc.
#
#  We'll implement this with a CASE expression on (mc.type, mc.job, p.gender).
#  The final grouping will show how many individuals fall under each category.
# ------------------------------------------------------------------------------


def query_4_group_roles_by_gender():
    """
    Returns aggregated counts of persons by role (cast vs crew),
    gender, and specific job (e.g., Director).
    The CASE expression translates them into Hebrew category names.

    Example returned columns:
      - category (str)
      - total (int)
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        # Adjust gender=1,2 mapping if your DB differs (1=female, 2=male is typical from TMDb).
        # We'll categorize only a few roles explicitly; everything else falls into 'Other ...'
        sql = """
            SELECT 
                CASE 
                  WHEN mc.type = 'cast' AND p.gender = 2 THEN 'שחקנים גברים'
                  WHEN mc.type = 'cast' AND p.gender = 1 THEN 'שחקניות נשים'
                  WHEN mc.type = 'crew' AND p.gender = 2 AND mc.job = 'Director' THEN 'במאים גברים'
                  WHEN mc.type = 'crew' AND p.gender = 1 AND mc.job = 'Director' THEN 'במאיות נשים'
                  ELSE CONCAT('Other (', IFNULL(mc.job,'No Job'), ' / Gender=', IFNULL(p.gender,'N/A'), ' / Type=', mc.type, ')')
                END AS category,
                COUNT(*) AS total
            FROM movie_credits mc
            JOIN persons p ON p.id = mc.person_id
            GROUP BY category
            ORDER BY total DESC;
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_4: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ------------------------------------------------------------------------------
# MAIN DEMO
# ------------------------------------------------------------------------------


def main():
    """
    Demonstrates calling each query in sequence and printing the results.
    Adjust or remove as needed for your assignment submission.
    """
    print("\n--- Query #1: Most popular movie & top 10 actors ---")
    q1_results = query_1_most_popular_movie_with_top_actors()
    for row in q1_results:
        print(f"Movie: {row['movie_title']}, Actor: {
              row['actor_name']}, Actor Popularity: {row['actor_popularity']}")

    print("\n--- Query #2: Genre counts & top movie in each genre ---")
    q2_results = query_2_genre_movie_counts_and_top_movie()
    for row in q2_results:
        print(f"Genre: {row['genre_name']}, Total Movies: {
              row['movie_count']}, Most Popular: {row['most_popular_movie']}")

    print("\n--- Query #3: Full-text search in movie title (example: 'באטמן') ---")
    q3_results = query_3_fulltext_search_in_title("באטמן")
    for row in q3_results:
        print(f"Movie ID: {row['id']}, Title: {
              row['title']}, Popularity: {row['popularity']}")

    print("\n--- Query #4: Group persons by role & gender ---")
    q4_results = query_4_group_roles_by_gender()
    for row in q4_results:
        print(f"{row['category']}: {row['total']}")


def main_to_file():
    """
    Demonstrates calling each query in sequence and printing the results
    to 'results.txt'.
    """
    # Open 'results.txt' in write mode
    with open("results.txt", "w", encoding="utf-8") as f:
        # Temporarily redirect stdout to the file
        with redirect_stdout(f):

            print("\n--- Query #1: Most popular movie & top 10 actors ---")
            q1_results = query_1_most_popular_movie_with_top_actors()
            for row in q1_results:
                print(f"Movie: {row['movie_title']}, "
                      f"Actor: {row['actor_name']}, "
                      f"Actor Popularity: {row['actor_popularity']}")

            print("\n--- Query #2: Genre counts & top movie in each genre ---")
            q2_results = query_2_genre_movie_counts_and_top_movie()
            for row in q2_results:
                print(f"Genre: {row['genre_name']}, "
                      f"Total Movies: {row['movie_count']}, "
                      f"Most Popular: {row['most_popular_movie']}")

            print(
                "\n--- Query #3: Full-text search in movie title (example: 'באטמן') ---")
            q3_results = query_3_fulltext_search_in_title("באטמן")
            for row in q3_results:
                print(f"Movie ID: {row['id']}, "
                      f"Title: {row['title']}, "
                      f"Popularity: {row['popularity']}")

            print("\n--- Query #4: Group persons by role & gender ---")
            q4_results = query_4_group_roles_by_gender()
            for row in q4_results:
                print(f"{row['category']}: {row['total']}")

    # After the `with redirect_stdout(f)` block, all prints revert to normal console output.
    print("Results successfully written to results.txt")


if __name__ == '__main__':
    # main()
    main_to_file()
