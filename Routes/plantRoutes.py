from flask import Blueprint, jsonify, request
import mysql.connector
import json

# Create a Blueprint
procurementAPI = Blueprint('procurement', __name__)

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
            "SELECT `name`,`Code`, `Rated_Capacity`,`PAF`, `PLF`, `Type`, `Technical_Minimum`, `Aux_Consumption`, `Max_Power`, `Min_Power`,"
            "`Variable_Cost` FROM `plant_details` WHERE `Type` = 'Must run'")
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        sum_query_2 = (
            "SELECT `name`,`Code`, `Rated_Capacity`,`PAF`, `PLF`, `Type`, `Technical_Minimum`, `Aux_Consumption`, `Max_Power`, `Min_Power` ,"
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


@procurementAPI.route('/plant', methods=['POST'])
def add_plant():
    """
    Add a new plant record to the database.
    """
    try:
        # Parse incoming JSON data
        data = request.get_json()

        # Validate required fields
        required_fields = [
            "Name", "Code", "Ownership", "Fuel_Type", "Rated_Capacity",
            "PAF", "PLF", "Aux_Consumption", "Variable_Cost",
            "Type", "Technical_Minimum", "Max_Power", "Min_Power"
        ]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Insert query
        insert_query = """
        INSERT INTO `plant_details` 
        (name, Code, Ownership, Fuel_Type, Rated_Capacity, PAF, PLF, Aux_Consumption, Variable_Cost, Type, Technical_Minimum, Max_Power, Min_Power)
        VALUES (%(Name)s, %(Code)s, %(Ownership)s, %(Fuel_Type)s, %(Rated_Capacity)s, %(PAF)s, %(PLF)s, %(Aux_Consumption)s, 
                %(Variable_Cost)s, %(Type)s, %(Technical_Minimum)s, %(Max_Power)s, %(Min_Power)s)
        """

        # Execute the query
        cursor.execute(insert_query, data)
        conn.commit()

        # Close the database connection
        cursor.close()
        conn.close()

        return jsonify({"message": "Plant added successfully"}), 201

    except mysql.connector.Error as err:
        return jsonify({"error": f"MySQL Error: {str(err)}"}), 500
    except Exception as e:
        return jsonify({"error": f"Unexpected Error: {str(e)}"}), 500


@procurementAPI.route('/<plant_code>', methods=['PUT'])
def update_plant_data(plant_code):
    """Update existing data for the specified plant."""
    try:
        # Parse incoming JSON data
        data = request.get_json()

        # Validate required fields in the incoming data
        required_fields = [
            "Name", "Code", "Ownership", "Fuel_Type", "Rated_Capacity",
            "PAF", "PLF", "Aux_Consumption", "Variable_Cost",
            "Type", "Technical_Minimum", "Max_Power", "Min_Power"
        ]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        # Check if the plant code in the URL matches the Code field in the request body
        if plant_code != data["Code"]:
            return jsonify({"error": "Plant code mismatch between URL and request body"}), 400

        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Update query
        update_query = """
        UPDATE `plant_details` 
        SET 
            name = %(Name)s, 
            Ownership = %(Ownership)s, 
            Fuel_Type = %(Fuel_Type)s, 
            Rated_Capacity = %(Rated_Capacity)s, 
            PAF = %(PAF)s, 
            PLF = %(PLF)s, 
            Aux_Consumption = %(Aux_Consumption)s, 
            Variable_Cost = %(Variable_Cost)s, 
            Type = %(Type)s, 
            Technical_Minimum = %(Technical_Minimum)s, 
            Max_Power = %(Max_Power)s, 
            Min_Power = %(Min_Power)s
        WHERE Code = %(Code)s
        """
        # Execute the query
        cursor.execute(update_query, data)
        conn.commit()

        # Close the database connection
        cursor.close()
        conn.close()

        return jsonify({"message": "Plant data updated successfully"}), 200

    except mysql.connector.Error as err:
        return jsonify({"error": f"MySQL Error: {str(err)}"}), 500
    except Exception as e:
        return jsonify({"error": f"Unexpected Error: {str(e)}"}), 500


@procurementAPI.route('/plant', methods=['DELETE'])
def delete_plant_data():
    """Delete a record for the specified plant using the plant code."""
    try:
        # Parse the JSON payload
        data = request.get_json()

        # Ensure the required field is present
        if not data or 'Code' not in data:
            return jsonify({"error": "Missing required field: 'Code'"}), 400

        # Establish database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Prepare and execute the DELETE query
        delete_query = "DELETE FROM `plant_details` WHERE `Code` = %(Code)s"
        cursor.execute(delete_query, {"Code": data["Code"]})
        conn.commit()

        # Check if a record was deleted
        if cursor.rowcount == 0:
            return jsonify({"error": "No record found with the given Code"}), 404

        # Close the connection
        cursor.close()
        conn.close()

        # Respond with success
        return jsonify({"message": "Plant data deleted successfully"}), 200

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

