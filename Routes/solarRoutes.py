# solarRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
solarApi = Blueprint('solar', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnldb'
}


@solarApi.route('/all', methods=['GET'])
def get_solar_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT `TimeStamp`, `Solar(Actual)`,`Solar(Pred)` FROM solar_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})

@solarApi.route('/sum_solar', methods=['GET'])
def get_sum_solar():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # First query to get the sum of Solar(Pred)
        query_1 = "SELECT SUM(`Solar(Pred)`) as total_solar FROM solar_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query_1, (start_date, end_date))
        result_1 = cursor.fetchall()

        # Second query to get the total price
        query_2 = "SELECT SUM(ROUND(`Solar(Pred)` * `Pred Price(Rs/ KWh)`, 2)) as total_price FROM solar_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query_2, (start_date, end_date))
        result_2 = cursor.fetchall()

        cursor.close()
        conn.close()

        # Combine the results
        result = [result_1, result_2]

        return jsonify(result)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
