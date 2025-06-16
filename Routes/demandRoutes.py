from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os

# Create a Blueprint
demandApi = Blueprint('demand', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1],
}

@demandApi.route('/dashboard', methods=['GET'])
def get_dashboard_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT COUNT(TYPE) AS count FROM `plant_details`")
        plant_count = cursor.fetchone()

        cursor.execute(
            '''SELECT SUM(`Demand(Actual)`) AS total_demand_actual, 
                      SUM(`Demand(Pred)`) AS total_demand_predicted
               FROM `demand_data`;''')
        demand_data = cursor.fetchone()

        cursor.execute('''SELECT AVG(`Cost_Per_Block`) AS average_pred_price
                          FROM `demand_output`;''')
        avg_price = cursor.fetchone()

        cursor.close()
        conn.close()

        if plant_count:
            return jsonify({
                "plant_count": plant_count["count"],
                "demand_actual": round(float(demand_data['total_demand_actual']), 3),
                "demand_predicted": round(float(demand_data['total_demand_predicted']), 3),
                "avg_price": round(float(avg_price['average_pred_price']), 2)
            }), 200
        else:
            return jsonify({"error": "No data found"}), 404

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

# NEW ENDPOINT FOR DATE RANGE DATA
@demandApi.route('/data', methods=['GET'])
def get_demand_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not start_date or not end_date:
        return jsonify({"error": "Both 'start_date' and 'end_date' parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT TimeStamp, 
                   `Demand(Actual)`,
                   `Demand(Pred)`
            FROM demand_data
            WHERE TimeStamp BETWEEN %s AND %s
            ORDER BY TimeStamp
        """, (start_date, end_date))
        
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify({"demand": rows})

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500