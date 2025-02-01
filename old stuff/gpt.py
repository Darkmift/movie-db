from flask import Flask, jsonify, request, abort
from dotenv import load_dotenv
import os
import logging
import requests
from db import mysql
from services.user_service import get_all_users, create_user, get_user_by_id
import seed  # Import seed module to initialize database if needed
from getApiData import get_movies

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# MySQL configuration
app.config['MYSQL_HOST'] = os.getenv('DB_HOST')
app.config['MYSQL_USER'] = os.getenv('DB_USER')
app.config['MYSQL_PASSWORD'] = os.getenv('DB_PASSWORD')
app.config['MYSQL_DATABASE'] = os.getenv('DB_NAME')

mysql.init_app(app)

# Run seed script if database is not initialized
def initialize_db():
    logger.info("Initializing database...")
    try:
        seed.run_seed()  # Ensure seed.py has a function `run_seed()`
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")

initialize_db()

@app.route('/users', methods=['GET'])
def get_users():
    try:
        users = get_all_users()
        return jsonify({"success": True, "data": users})
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        return jsonify({"success": False, "message": "Internal Server Error"}), 500

@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id):
    if user_id <= 0:
        return jsonify({"success": False, "message": "User ID must be a positive integer"}), 400
    try:
        user = get_user_by_id(user_id)
        if not user:
            return jsonify({"success": False, "message": "User not found"}), 404
        return jsonify({"success": True, "data": user})
    except Exception as e:
        logger.error(f"Error fetching user {user_id}: {e}")
        return jsonify({"success": False, "message": "Internal Server Error"}), 500

@app.route('/users', methods=['POST'])
def add_user():
    data = request.json
    if not data or 'name' not in data:
        return jsonify({"success": False, "message": "Invalid input"}), 400
    try:
        result = create_user(data)
        return jsonify({"success": True, "message": "User added successfully", "data": result}), 201
    except Exception as e:
        logger.error(f"Error adding user: {e}")
        return jsonify({"success": False, "message": "Internal Server Error"}), 500

@app.route('/external/movies', methods=['GET'])
def fetch_movies():
    try:
        movies = get_movies()
        return jsonify({"success": True, "data": movies})
    except Exception as e:
        logger.error(f"Error fetching movies: {e}")
        return jsonify({"success": False, "message": "Failed to fetch movies"}), 500

if __name__ == '__main__':
    logger.info("Starting Flask server...")
    app.run(debug=True)
