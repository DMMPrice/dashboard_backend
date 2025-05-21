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
    'database': 'guvnl_dev'
}


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
