import os
from flask import Flask, jsonify
from dotenv import load_dotenv
from services.db import create_tables, get_db_connection, initialize_database
# import all methods from the movie_api.py file
import services.movie_api as movie_api


# Load environment variables from the .env file
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')

try:
    # Initialize the database if it doesn't exist
    initialize_database()
except Exception as e:
    print("Error while connecting to the database:", e)


@app.route('/')
def home():
    return "Hello, Flask with dotenv!"


@app.route('/api/data', methods=['GET'])
def get_data():
    sample_data = {
        "message": "Hello from Flask API using dotenv",
        "status": "success"
    }
    return jsonify(sample_data)

# Testing methods from the movie_api.py file


@app.route('/api/movies/page/<int:page>', methods=['GET'])
def movies_by_page(page):
    try:
        movies = movie_api.get_movie_credits(page)
        # movies = movie_api.get_movies_by_page(page)
        return jsonify(movies)
    except Exception as e:
        # Return a JSON error message in case of failure
        return jsonify({"error": str(e)}), 500

# Use a guard to ensure initialization runs only once.


if not hasattr(app, '_initialized'):
    try:
        # Initialize the database if it hasn't been initialized yet
        print("Initializing the database...")
        initialize_database()
        create_tables()
        app._initialized = True
    except Exception as e:
        print("Error while initializing the database:", e)

if __name__ == '__main__':
    print("Starting Flask app...", flush=True)
    app.run(debug=True)
