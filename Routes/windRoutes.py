# windRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
windApi = Blueprint('wind', __name__)

# MySQL configuration
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'guvnl-db'
}


@windApi.route('/all', methods=['GET'])
def get_wind_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT `TimeStamp`, `Wind(Actual)`,`Wind(Pred)` FROM wind_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@windApi.route('/year', methods=['GET'])
def get_wind_data_by_year():
    year = request.args.get('year')
    if not year:
        return jsonify({"error": "Year parameter is required"}), 400
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT `TimeStamp`, `Wind(Actual)`,`Wind(Pred)` FROM wind_data WHERE YEAR(TimeStamp) = %s"
        cursor.execute(query, (year,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@windApi.route('/consumed', methods=['GET'])
def get_wind_data_consumed():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT `TimeStamp`, `Wind(Actual)` FROM wind_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@windApi.route('/predicted', methods=['GET'])
def get_wind_data_predicted():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT `TimeStamp`, `Wind(Pred)` FROM wind_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@windApi.route('/sum_wind', methods=['GET'])
def get_sum_wind():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT SUM(`Wind(Actual)`) as total_wind FROM wind_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query, (start_date, end_date))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return jsonify(result)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
