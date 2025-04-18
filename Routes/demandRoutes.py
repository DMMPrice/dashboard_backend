# demandRoutes.py
from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
demandApi = Blueprint('demand', __name__)

# MySQL configuration
db_config = {
    'user': 'DB-Admin',
    'password': 'DBTest@123',
    'host': '69.62.74.149',
    'database': 'guvnldev'
}


# db_config = {
#     "host": "localhost",      # Change if using a remote server
#     "user": "root",           # Change according to your MySQL credentials
#     "password": "",           # Your MySQL password
#     "database": "guvnl_dev"  # Replace with your database name
# }

@demandApi.route('/dashboard', methods=['GET'])
def get_dashboard_data():
    try:
        # Establish a connection to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # SQL query to count rows in the `plant_details` table
        cursor.execute("SELECT COUNT(TYPE) AS count FROM `plant_details`")
        plant_count = cursor.fetchone()

        cursor.execute(
            '''SELECT SUM(`Demand(Actual)`) AS total_demand_actual, SUM(`Demand(Pred)`) AS total_demand_predicted
               FROM `demand_data`;''')
        demand_data = cursor.fetchone()

        cursor.execute('''SELECT AVG(`Cost_Per_Block`) AS average_pred_price
                          FROM `demand_output`;''')
        avg_price = cursor.fetchone()

        # Close cursor and connection
        cursor.close()
        conn.close()

        # Ensure the result is accessed correctly and returned
        if plant_count:
            return jsonify({"plant_count": plant_count["count"],
                            "demand_actual": round(float(demand_data['total_demand_actual']), 3),
                            "demand_predicted": round(float(demand_data['total_demand_predicted']), 3),
                            "avg_price": round(float(avg_price['average_pred_price']), 2)},
                           ), 200
        else:
            return jsonify({"error": "No data found"}), 404

    except mysql.connector.Error as err:
        # Handle MySQL connection errors
        return jsonify({"error": str(err)}), 500


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
        cursor.execute("SELECT `TimeStamp`, `Demand(Pred)` FROM demand_data")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})


@demandApi.route('/data_with_sum', methods=['GET'])
def get_data_with_sum():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch all data within the date range
        data_query = "SELECT * FROM demand_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(data_query, (start_date, end_date))
        data_rows = cursor.fetchall()

        # Fetch the sum of Demand(Pred) within the date range
        sum_query = "SELECT SUM(`Demand(Pred)`) as total_demand FROM demand_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(sum_query, (start_date, end_date))
        sum_result = cursor.fetchone()

        cursor.close()
        conn.close()

        # Combine the data and sum into a single response
        response = {
            "data": data_rows,
            "total_demand": sum_result['total_demand']
        }

        return jsonify(response)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
