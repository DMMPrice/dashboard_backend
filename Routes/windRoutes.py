# windRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
windApi = Blueprint('wind', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'BaBa@123',
    'host': 'guvnl.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnldb'
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

        # First query to get the sum of Solar(Pred)
        query_1 = "SELECT SUM(`Wind(Pred)`) as total_wind FROM wind_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query_1, (start_date, end_date))
        result_1 = cursor.fetchone()

        # Second query to get the total price
        query_2 = "SELECT SUM(ROUND(`Wind(Actual)` * `Price (Rs/ KWh)`,2)) as total_price FROM wind_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query_2, (start_date, end_date))
        result_2 = cursor.fetchone()

        cursor.close()
        conn.close()

        # Combine the results
        result = [
            result_1,result_2
        ]

        return jsonify(result)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
