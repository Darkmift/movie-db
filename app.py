import os
from flask import Flask, jsonify
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')

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

if __name__ == '__main__':
    # Use environment variables for host and port if needed
    app.run(debug=True)
