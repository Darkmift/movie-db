import mysql.connector
from mysql.connector import errorcode
from common import DATABASE_NAME

def query_1(company_name):
    """
    get_total_revenue_for_company -
    Returns the total revenue for a given production company by summing
    (box_office - budget) for all its movies. May return negative if
    budgets exceed box_office totals.

    :param company_name: The name of the production company (string).
    :return: A float (total revenue) or None if the company doesn't exist
        or has no movies.
    """
    try:
        mydb = mysql.connector.connect(
            host="127.0.0.2",
            port="3333",
            user="natanel",
            password="nat72836",
            database={DATABASE_NAME}
        )
        cursor = mydb.cursor()

        # Define our query using a parameter placeholder (%s)
        query = """
            SELECT
                SUM(m.box_office - m.budget) AS total_revenue
            FROM production_company pc
            JOIN movie m ON pc.production_company_id = m.production_company_id
            WHERE pc.name = %s
            GROUP BY pc.name
        """

        cursor.execute(query, (company_name,))
        row = cursor.fetchone()

        # If the query returns a row, the first (and only) value is the total revenue
        if row:
            return row[0]  # total_revenue
        else:
            return None

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return None
    finally:
        cursor.close()
        mydb.close()


def query_2(movie_name):
    """
    get_top_profitable_movies_by_name
    Searches for the top 10 most profitable movies containing the given movie name.

    :param movie_name: The movie name keyword to search for (string).
    :return: A list of tuples (title, production_company, revenue) sorted by profitability.
    """
    try:
        # Establish database connection
        mydb = mysql.connector.connect(
            host="127.0.0.2",
            port="3333",
            user="natanel",
            password="nat72836",
            database={DATABASE_NAME}
        )
        cursor = mydb.cursor()

        # Define the full-text search query
        query = """
            SELECT 
                m.title,
                pc.name AS production_company,
                (m.box_office - m.budget) AS revenue
            FROM movie m
            JOIN production_company pc ON m.production_company_id = pc.production_company_id
            WHERE MATCH(m.title) AGAINST (%s IN NATURAL LANGUAGE MODE)
            ORDER BY revenue DESC
            LIMIT 10;
        """

        cursor.execute(query, (movie_name,))
        results = cursor.fetchall()

        return results  # List of (title, production_company, revenue)

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return []
    finally:
        # Close cursor and connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'mydb' in locals() and mydb.is_connected():
            mydb.close()

def query_3(production_company_name):
    """
    get_top_countries_by_movies:
    Returns the top 5 countries where a given production company has produced the most movies.

    :param production_company_name: The name of the production company (string).
    :return: A list of tuples (country_name, movie_count) ordered by the most movies.
    """
    try:
        # Establish database connection
        mydb = mysql.connector.connect(
            host="127.0.0.2",
            port="3333",
            user="natanel",
            password="nat72836",
            database={DATABASE_NAME}
        )
        cursor = mydb.cursor()

        # Define the optimized query
        query = """
            SELECT 
                c.name AS country_name,
                COUNT(m.movie_id) AS movie_count
            FROM movie m
            JOIN production_company pc ON m.production_company_id = pc.production_company_id
            JOIN country c ON m.country_id = c.country_id
            WHERE pc.name = %s
            GROUP BY c.country_id, c.name
            ORDER BY movie_count DESC
            LIMIT 5;
        """

        cursor.execute(query, (production_company_name,))
        results = cursor.fetchall()

        return results  # List of (country_name, movie_count)

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return []
    finally:
        # Close cursor and connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'mydb' in locals() and mydb.is_connected():
            mydb.close()