# Routes/intrastateRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
intraStateApi = Blueprint('intrastate', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnlintra'
}

@intraStateApi.route('/all', methods=['GET'])
def get_inter_state_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Query to get the table names
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'guvnlintra'")
        tables = cursor.fetchall()
        # print(tables)

        cursor.close()
        conn.close()

        # Extract table names into a single array
        table_names = [table['TABLE_NAME'] for table in tables]

        return jsonify(table_names)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})

@intraStateApi.route('/<table_name>', methods=['GET'])
def get_inter_state_data_by_table(table_name):
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # First query to get the sum of Solar(Pred)
        query_1 = f"SELECT SUM(`Pred`) as total_generate FROM {table_name} WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query_1, (start_date, end_date))
        result_1 = cursor.fetchone()

        # Second query to get the total price
        query_2 = f"SELECT SUM(ROUND(`Pred` * `Pred Price(Rs/ KWh)`, 2)) as total_price FROM {table_name} WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(query_2, (start_date, end_date))
        result_2 = cursor.fetchone()

        cursor.close()
        conn.close()

        # Combine the results
        result = [result_1, result_2]

        return jsonify(result)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})