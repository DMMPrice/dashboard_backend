from flask import Blueprint, jsonify, request
import mysql.connector

# Create a Blueprint
plantAPI = Blueprint('plant', __name__)

# MySQL configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnl_dev'
}


@plantAPI.route('/others', methods=['GET'])
def get_other_run():
    net_demand = request.args.get('net_demand')
    if not net_demand:
        return jsonify({"error": "Net demand parameters are required"}), 400
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the sum of Demand(Pred) within the date range
        sum_query = (
            "SELECT `name`,`Code`, `Rated_Capacity`,`PAF`, `PLF`, `Type`, `Technical_Minimum`, `Aux_Consumption`, "
            "`Variable_Cost` FROM `plant_details` WHERE `Type` = 'Other' ORDER BY `Variable_Cost` ASC")
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        temp_demand = float(net_demand)
        other_plants_charge = []
        plant_data = sum_result
        for i in range(len(plant_data)):
            if temp_demand == 0:
                break
            else:
                single_plant = plant_data[i]
                single_plant_generation = float(single_plant['Rated_Capacity'] * single_plant['PAF'] * single_plant[
                    'PLF'] * 0.25 * 1000 * (1 - single_plant['Aux_Consumption']))
                single_plant_generation_no_plf = single_plant_generation / single_plant['PLF']
                if single_plant_generation > temp_demand:
                    single_plant_generation = temp_demand
                    plf = single_plant_generation / single_plant_generation_no_plf
                    single_plant_generation_cost = single_plant['Variable_Cost'] * single_plant_generation
                    temp_demand = 0
                    plant_details = {
                        "name": single_plant['name'],
                        "generation": single_plant_generation,
                        "code": single_plant['Code'],
                        "cost": single_plant_generation_cost,
                        "PLF": plf,
                        "PAF": single_plant['PAF'],
                        "Type": single_plant['Type'],
                        "Aux_Consumption": single_plant['Aux_Consumption'],
                        "Technical_Minimum": single_plant['Technical_Minimum']
                    }
                    other_plants_charge.append(plant_details)
                else:
                    single_plant_generation_cost = single_plant['Variable_Cost'] * single_plant_generation
                    temp_demand = float(temp_demand) - single_plant_generation
                    plant_details = {
                        "name": single_plant['name'],
                        "code": single_plant['Code'],
                        "generation": single_plant_generation,
                        "cost": single_plant_generation_cost,
                        "PLF": single_plant['PLF'],
                        "PAF": single_plant['PAF'],
                        "Type": single_plant['Type'],
                        "Aux_Consumption": single_plant['Aux_Consumption'],
                        "Technical_Minimum": single_plant['Technical_Minimum']
                    }
                    other_plants_charge.append(plant_details)
                    temp_demand = float(temp_demand) - single_plant_generation

        return jsonify({"other": other_plants_charge, "net-demand": temp_demand}), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
    except Exception as e:
        return jsonify({"error": str(e)})


@plantAPI.route('/must-run', methods=['GET'])
def get_must_run():
    net_demand = request.args.get('net_demand')
    timestamp = request.args.get('timeStamp')
    if not net_demand:
        return jsonify({"error": "Net demand parameters are required"}), 400
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Fetch the must-run plant details
        sum_query = (
            "SELECT `name`, `Code`, `Rated_Capacity`, `PAF`, `PLF`, `Type`, `Technical_Minimum`, `Aux_Consumption`, "
            "`Variable_Cost` FROM `plant_details` WHERE `Type` = 'Must run'")
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        # Iterate over the results and query each plant's data
        plant_data = []
        for plant in sum_result:
            code = plant['Code']
            try:
                plant_query = f"SELECT `TimeStamp`, `Pred` FROM `{code}` WHERE `TimeStamp` = %s"
                cursor.execute(plant_query, (timestamp,))
                plant_result = cursor.fetchall()
                if not plant_result:
                    plant_result = [{"TimeStamp": timestamp, "Pred": 0.00}]
            except mysql.connector.Error:
                plant_result = [{"TimeStamp": timestamp, "Pred": 0.00}]
            print(plant_result)
            generated_energy = float(plant_result[0]['Pred'])
            variable_cost = float(plant['Variable_Cost'])
            plant_data.append({
                "plant_name": plant['name'],
                "plant_code": plant['Code'],
                "Rated_Capacity": plant['Rated_Capacity'],
                "PAF": plant['PAF'],
                "PLF": plant['PLF'],
                "Type": plant['Type'],
                "Technical_Minimum": plant['Technical_Minimum'],
                "Aux_Consumption": plant['Aux_Consumption'],
                "Variable_Cost": variable_cost,
                "generated_energy": generated_energy,
                "net_cost": generated_energy * variable_cost
            })

        cursor.close()
        conn.close()

        return jsonify(plant_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
    except Exception as e:
        return jsonify({"error": str(e)})
