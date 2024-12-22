from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
procurementAPI = Blueprint('procurement', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
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
        sum_query = ("SELECT SUM(`Demand(Pred)`)*1000 as total_demand , COUNT(`Demand(Pred)`) as total_blocks FROM "
                     "demand_data WHERE `TimeStamp` BETWEEN %s AND %s")
        cursor.execute(sum_query, (start_date, end_date))
        sum_result = cursor.fetchone()

        sum_query_2 = "SELECT * FROM demand_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(sum_query_2, (start_date, end_date))
        sum_result_2 = cursor.fetchall()

        cursor.close()
        conn.close()

        # Combine the data and sum into a single response
        response = {
            "start_date": start_date,
            "end_date": end_date,
            "total_demand": sum_result['total_demand'] * 1000,
            "total_blocks": sum_result['total_blocks'],
            "demand_list": sum_result_2,
        }
        return jsonify(response)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
    except Exception as e:
        return jsonify({"error": str(e)})


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


@procurementAPI.route('/plant-all', methods=['GET'])
def get_all_plant_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the sum of Demand(Pred) within the date range
        sum_query = (
            "SELECT `name`,`Code`, `Rated_Capacity`,`PAF`, `PLF`, `Type`, `Technical_Minimum`, `Aux_Consumption`, "
            "`Variable_Cost` FROM `plant_details`")
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        cursor.close()
        conn.close()

        # Combine the data and sum into a single response
        response = {
            "plant_count": len(sum_result),
            "plant_data": sum_result
        }
        return jsonify(response), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
    except Exception as e:
        return jsonify({"error": str(e)})


@procurementAPI.route('/<plant_name>', methods=['GET'])
def get_each_plant_data(plant_name):
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the data within the date range for the given plant
        sum_query = f"SELECT * FROM `{plant_name}` WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(sum_query, (start_date, end_date))
        sum_result = cursor.fetchall()

        cursor.close()
        conn.close()

        correct_data = {
            "TimeStamp": sum_result[0]['TimeStamp'],
            "Actual": sum_result[0]['Actual'] * 1000,
            "Predicted": sum_result[0]['Pred'] * 1000,
        }

        return jsonify(correct_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": "No Data Found"})
    except Exception as e:
        return jsonify({"error": "No Data Found"})
