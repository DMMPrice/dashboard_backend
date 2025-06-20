# iexRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os

# Create a Blueprint
iexApi = Blueprint('iex', __name__)


# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1],
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


@iexApi.route('/range', methods=['GET'])
def get_demand_range():
    start = request.args.get('start')  # e.g. "2021-04-01 00:00:00"
    end = request.args.get('end')  # e.g. "2021-04-02 00:00:00"

    if not start or not end:
        return jsonify({"error": "Both 'start' and 'end' query parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # fetch raw rows (fixed: removed extra comma)
        cursor.execute("""
                       SELECT TimeStamp, Pred_Price AS predicted
                       FROM iex_data
                       WHERE TimeStamp BETWEEN %s
                         AND %s
                       ORDER BY TimeStamp
                       """, (start, end))
        rows = cursor.fetchall()

        # calculate total and average
        total_predicted = sum(r['predicted'] for r in rows)
        average_predicted = total_predicted / len(rows) if rows else None

        cursor.close()
        conn.close()

        return jsonify({
            "data": rows,
            "summary": {
                "total_predicted": total_predicted,
                "average_predicted": round(average_predicted, 2)
            }
        })

    except mysql.connector.Error as err:
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
