from flask import Blueprint, jsonify, request
import mysql.connector
import json

# Create a Blueprint
procurementAPI = Blueprint('procurement', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl-dev.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnl_dev'
}


@procurementAPI.route('/demand', methods=['GET'])
def get_demand_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the sum of Demand(Pred) within the date range
        sum_query = "SELECT * FROM demand_output WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(sum_query, (start_date, end_date))
        sum_result = cursor.fetchall()

        # Decode JSON fields
        for row in sum_result:
            for key in ['IEX_Data', 'Must_Run', 'Remaining_Plants']:  # Adjust based on your field names
                if key in row and row[key]:
                    try:
                        row[key] = json.loads(row[key])  # Convert JSON string to dictionary
                    except json.JSONDecodeError:
                        row[key] = f"Invalid JSON: {row[key]}"  # Handle JSON decode errors gracefully

        cursor.close()
        conn.close()

        return jsonify(sum_result), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@procurementAPI.route('/all', methods=['GET'])
def get_all_plant_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch all plant data
        query = "SELECT * FROM plant_details"
        cursor.execute(query)
        result = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(result), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@procurementAPI.route('/exchange', methods=['GET'])
def get_exchange_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    cap_price = request.args.get('cap_price')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the sum of Demand(Pred) within the date range
        sum_query = "SELECT `TimeStamp`, `Qty_Pred`, `Pred_Price` FROM iex_data WHERE `TimeStamp` BETWEEN %s AND %s "
        cursor.execute(sum_query, (start_date, end_date))
        sum_result = cursor.fetchall()

        # Make an empty list to store the data which is less than the cap price otherwise keep them as 0
        for i in range(len(sum_result)):
            if sum_result[i]['Pred_Price'] > float(cap_price):
                sum_result[i]['Pred_Price'] = -1
            else:
                sum_result[i]['Qty_Pred'] = round(sum_result[i]['Qty_Pred'] * 1000 * 0.25, 3)
        cursor.close()
        conn.close()

        # Combine the data and sum into a single response
        response = {
            "start_date": start_date,
            "end_date": end_date,
            "cap_price": cap_price,
            "exchange_data": sum_result,
        }
        return jsonify(response), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
    except Exception as e:
        return jsonify({"error": str(e)})


@procurementAPI.route('/plant', methods=['GET'])
def get_plant_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the sum of Demand(Pred) within the date range
        sum_query = (
            "SELECT `name`,`Code`, `Rated_Capacity`,`PAF`, `PLF`, `Type`, `Technical_Minimum`, `Aux_Consumption`, "
            "`Variable_Cost` FROM `plant_details` WHERE `Type` = 'Must run'")
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        sum_query_2 = (
            "SELECT `name`,`Code`, `Rated_Capacity`,`PAF`, `PLF`, `Type`, `Technical_Minimum`, `Aux_Consumption`, "
            "`Variable_Cost` FROM `plant_details` WHERE `Type` = 'Other'")
        cursor.execute(sum_query_2)
        sum_result_2 = cursor.fetchall()

        cursor.close()
        conn.close()

        # Combine the data and sum into a single response
        response = {
            "must_run_count": len(sum_result),
            "other_count": len(sum_result_2),
            "must_run": sum_result,
            "other": sum_result_2
        }
        return jsonify(response), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
    except Exception as e:
        return jsonify({"error": str(e)})


@procurementAPI.route('/<plant_name>', methods=['GET'])
def get_each_plant_data(plant_name):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the data within the date range for the given plant
        sum_query = f"SELECT * FROM `{plant_name}`"
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        cursor.close()
        conn.close()

        # correct_data = {
        #     "TimeStamp": sum_result[0]['TimeStamp'],
        #     "Actual": sum_result[0]['Actual'] * 1000,
        #     "Predicted": sum_result[0]['Pred'] * 1000,
        # }

        return jsonify(sum_result), 200
    except mysql.connector.Error as err:
        return jsonify({"error": "No Data Found"})
    except Exception as e:
        return jsonify({"error": "No Data Found"})
