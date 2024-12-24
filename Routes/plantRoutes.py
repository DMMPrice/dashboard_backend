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
        return jsonify({"error": "Net demand parameter is required"}), 400

    try:
        net_demand = float(net_demand)
        if net_demand <= 0:
            return jsonify({"error": "Net demand must be greater than zero"}), 400

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = (
            "SELECT `name`, `Code`, `Rated_Capacity`, `PAF`, `PLF`, `Type`, `Technical_Minimum`, "
            "`Aux_Consumption`, `Variable_Cost` FROM `plant_details` WHERE `Type` = 'Other' "
            "ORDER BY `Variable_Cost` ASC"
        )
        cursor.execute(query)
        plants = cursor.fetchall()

        temp_demand = net_demand
        other_plants_charge = []
        excess_generation_adjusted = False

        for i, plant in enumerate(plants):
            if temp_demand <= 0:
                break

            # Calculate maximum generation
            rated_capacity = float(plant['Rated_Capacity'])
            paf = float(plant['PAF'])
            plf = float(plant['PLF'])
            aux = float(plant['Aux_Consumption'])
            tech_min = float(plant['Technical_Minimum'])

            max_generation = (
                    rated_capacity * paf * plf * 0.25 * 1000 * (1 - aux)
            )
            tech_min_gen = rated_capacity * tech_min * paf * 0.25 * 1000 * (1 - aux)

            # Ensure TM generation is met
            if temp_demand < tech_min_gen:
                temp_demand -= tech_min_gen
                other_plants_charge.append({
                    "name": plant['name'],
                    "code": plant['Code'],
                    "Rated_Capacity": rated_capacity,
                    "generation": round(tech_min_gen, 3),
                    "cost": round(tech_min_gen * float(plant['Variable_Cost']), 2),
                    "PLF": plf,
                    "PAF": paf,
                    "Type": plant['Type'],
                    "Aux_Consumption": aux,
                    "Technical_Minimum": tech_min,
                    "Variable_Cost": plant['Variable_Cost'],
                })
                continue

            actual_generation = min(max_generation, temp_demand)
            temp_demand -= actual_generation

            if temp_demand < 0 and not excess_generation_adjusted:
                # Adjust excess from the second last plant
                excess_generation_adjusted = True
                last_plant = other_plants_charge[-1]
                excess = abs(temp_demand)
                new_generation = last_plant['generation'] - excess
                new_plf = new_generation / (rated_capacity * paf * 0.25 * 1000 * (1 - aux))
                last_plant['generation'] = new_generation
                last_plant['PLF'] = new_plf
                temp_demand = 0

            generation_cost = round(actual_generation * float(plant['Variable_Cost']), 3)

            other_plants_charge.append({
                "name": plant['name'],
                "code": plant['Code'],
                "Rated_Capacity": rated_capacity,
                "generation": round(actual_generation, 3),
                "cost": round(generation_cost, 2),
                "PLF": plf,
                "PAF": paf,
                "Type": plant['Type'],
                "Aux_Consumption": aux,
                "Technical_Minimum": tech_min,
                "Variable_Cost": plant['Variable_Cost'],
            })

        return jsonify({
            "other": other_plants_charge,
            "remaining_demand": temp_demand
        }), 200

    except mysql.connector.Error as err:
        return jsonify({"error": f"MySQL error: {str(err)}"}), 500
    except ValueError as ve:
        return jsonify({"error": f"Value error: {str(ve)}"}), 400
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


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
            # print(plant_result)
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
                "generated_energy": round(generated_energy * 1000 * 0.25, 3),
                "net_cost": round(generated_energy * variable_cost * 1000 * 0.25, 2)
            })

        cursor.close()
        conn.close()

        return jsonify(plant_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)})
    except Exception as e:
        return jsonify({"error": str(e)})
