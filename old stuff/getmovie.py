import requests
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_movies():
    url = "https://api.themoviedb.org/3/genre/movie/list"
    headers = {
        "Authorization": f"Bearer {os.getenv('TOKEN')}"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching movies: {e}")
        return {"success": False, "message": "Failed to fetch movi