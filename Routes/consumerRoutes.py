from flask import Blueprint, jsonify, request
import mysql.connector

# Blueprint setup
consumerAPI = Blueprint('consumer', __name__)

# Database configuration
db_config = {
    'user': 'DB-Admin',
    'password': 'DBTest@123',
    'host': '69.62.74.149',
    'database': 'guvnldev'
}


# Route to get consumer details
@consumerAPI.route('/', methods=['GET'])
def get_consumer_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM consumer_details")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# Route to get data from any table (table name as a parameter)
@consumerAPI.route('/<string:table_name>', methods=['GET'])
def get_table_data(table_name):
    # Basic validation to prevent SQL injection
    if not table_name.replace("-", "_").isidentifier():
        return jsonify({"error": "Invalid table name"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Use backticks to handle special characters in table name
        query = f"SELECT * FROM `{table_name}`"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
