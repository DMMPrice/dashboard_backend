# iexRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
iexApi = Blueprint('iex', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl-dev.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnl_dev'
}


@iexApi.route('/all', methods=['GET'])
def get_price_data():
    try:
        # Establish a connection to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # SQL query to count rows in the `plant_details` table
        cursor.execute("SELECT * FROM `Price`")
        price = cursor.fetchall()

        return jsonify(price), 200

    except mysql.connector.Error as err:
        # Handle MySQL connection errors
        return jsonify({"error": str(err)}), 500


@iexApi.route('/dashboard', methods=['GET'])
def get_dashboard():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT AvG(`Actual`) AS `Avg_Price`, AvG(`Pred`) AS `Avg_Pred_Price` FROM `Price`")
        rows = cursor.fetchone()
        cursor.close()

        rows['Avg_Price'] = round(float(rows['Avg_Price']), 2)
        rows['Avg_Pred_Price'] = round(float(rows['Avg_Pred_Price']), 2)

        return jsonify(rows), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@iexApi.route("/quantity", methods=["GET"])
def get_quantity_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM `iex_data`")
        rows = cursor.fetchall()
        cursor.close()
        return jsonify(rows), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
