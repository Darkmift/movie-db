import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Base URL for TMDb API
BASE_URL = 'https://api.themoviedb.org/3'

# Retrieve the TMDb Bearer token from environment variables
TMDB_BEARER_TOKEN = os.environ.get("TMDB_BEARER_TOKEN")
if not TMDB_BEARER_TOKEN:
    raise Exception("TMDB_BEARER_TOKEN is not set in the environment variables.")

# Define common headers for the API requests
HEADERS = {
    "Authorization": f"Bearer {TMDB_BEARER_TOKEN}",
    "Content-Type": "application/json;charset=utf-8"
}


def get_all_genres():
    """
    Fetch all movie genres.
    Endpoint: /genre/movie/list
    """
    url = f"{BASE_URL}/genre/movie/list"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def get_movies_by_genre(genre):
    """
    Fetch movies filtered by a specific genre.
    Endpoint: /discover/movie?with_genres=<genre>
    
    :param genre: Genre id as a string or integer.
    """
    url = f"{BASE_URL}/discover/movie"
    params = {
        "with_genres": genre
    }
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def get_movies_by_page(page):
    """
    Fetch popular movies for a given page.
    Endpoint: /movie/popular?page=<page>
    
    :param page: Page number as an integer.
    """
    url = f"{BASE_URL}/movie/popular"
    params = {
        "page": page
    }
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def get_movie_by_id(movie_id):
    """
    Fetch movie details by its ID.
    Endpoint: /movie/<movie_id>
    
    :param movie_id: The movie's ID.
    """
    url = f"{BASE_URL}/movie/{movie_id}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def get_movie_images(movie_id):
    """
    Fetch all images for a specific movie.
    Endpoint: /movie/<movie_id>/images
    
    :param movie_id: The movie's ID.
    """
    url = f"{BASE_URL}/movie/{movie_id}/images"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


def get_movie_credits(movie_id):
    """
    Fetch cast and crew information for a specific movie.
    Endpoint: /movie/<movie_id>/credits
    
    :param movie_id: The movie's ID.
    """
    url = f"{BASE_URL}/movie/{movie_id}/credits"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    return response.json()


# For quick testing of individual functions
if __name__ == '__main__':
    # Example: Get all genres and print them
    try:
        genres = get_all_genres()
        print("Genres:", genres)
    except Exception as e:
        print("Error fetching genres:", e)
