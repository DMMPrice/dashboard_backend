# demandRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
demandApi = Blueprint('demand', __name__)

# MySQL configuration
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'guvnl-db'
}


@demandApi.route('/all', methods=['GET'])
def get_demand_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM demand_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@demandApi.route('/year', methods=['GET'])
def get_demand_data_by_year():
    year = request.args.get('year')
    if not year:
        return jsonify({"error": "Year parameter is required"}), 400
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM demand_data WHERE YEAR(TimeStamp) = %s"
        cursor.execute(query, (year,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@demandApi.route('/consumed', methods=['GET'])
def get_demand_data_consumed():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT `TimeStamp`, `Demand(Actual)` FROM demand_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@demandApi.route('/predicted', methods=['GET'])
def get_demand_data_predicted():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT `TimeStamp`, `Demand(Actual)` FROM demand_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@demandApi.route('/sum_demand', methods=['GET'])
def get_sum_demand():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = "SELECT SUM(`Demand(Actual)`) as total_demand FROM demand_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query, (start_date, end_date))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return jsonify(result)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
