from flask import Blueprint, jsonify, request
import mysql.connector
import concurrent.futures
from typing import List, Dict, Any, Union
from datetime import datetime
import pprint

# ----------------------------- Blueprint Setup -----------------------------

# Create a Flask Blueprint named 'plant' to modularize the API routes
plantAPI = Blueprint('plant', __name__)

# -------------------------- Database Configuration --------------------------

# MySQL database connection configuration
db_config = {
    'user': 'admin',
    'password': '7%Ky8w@BV!PRYxDw8l',
    'host': 'public-primary-mysql-inmumbaizone2-189017-1638097.db.onutho.com',
    'database': 'guvnldev'
}
# db_config = {
#     "host": "localhost",  # Change if using a remote server
#     "user": "root",  # Change according to your MySQL credentials
#     "password": "",  # Your MySQL password
#     "database": "guvnl_dev"  # Replace with your database name
# }


# ----------------------------- Helper Functions -----------------------------

def map_and_calculate(alloc: Dict[str, Any], plant_dict: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Maps a single allocation to plant data and calculates the Plant Load Factor (plf) and Net Cost.
    """
    plant_code = alloc['plant_code']
    allocated_gen = alloc['allocated_gen']
    min_gen = alloc['min_gen']
    max_gen = alloc['max_gen']

    plant = plant_dict.get(plant_code, {})
    plant_name = plant.get('name', 'Unknown Plant')
    rated_capacity = plant.get('Rated_Capacity', 0.0)
    paf = plant.get('PAF', 0.0)
    aux_consumption = plant.get('Aux_Consumption', 0.0)
    variable_cost = plant.get('Variable_Cost', 0.0)

    denominator = rated_capacity * 1000 * 0.25 * paf * (1 - aux_consumption)
    plf = allocated_gen / denominator if denominator != 0 else 0.0

    net_cost = allocated_gen * variable_cost

    return {
        'plant_name': plant_name,
        'plant_code': plant_code,
        'rated_capacity': rated_capacity,
        'paf': paf,
        'Aux_Consumption': aux_consumption,
        'plf': plf,
        'Variable_Cost': variable_cost,
        'max_power': max_gen,
        'min_power': min_gen,
        'generated_energy': allocated_gen,
        'net_cost': net_cost
    }


def allocate_generation(plants: List[Dict[str, Any]], net_demand: float) -> Dict[str, Union[float, List[Any]]]:
    """
    Allocates energy generation to plants based on their Variable Cost,
    ensuring each plant meets its Minimum Power.
    Utilizes parallel processing to calculate detailed allocation metrics.
    """
    if net_demand <= 0:
        raise ValueError("Net demand must be greater than zero")

    sorted_plants = plants
    allocation = []
    total_allocated = 0.0

    # Modified Allocation Loop: If remaining demand is less than the plant's Max_Power,
    # allocate only the remaining demand.
    for plant in sorted_plants:
        plant_code = plant['Code']
        max_power = plant['Max_Power']
        min_power = plant['Min_Power']

        if max_power == 0:
            continue

        remaining_demand = net_demand - total_allocated
        if remaining_demand <= max_power:
            allocated_gen = remaining_demand
            allocation.append({
                'plant_code': plant_code,
                'allocated_gen': allocated_gen,
                'min_gen': min_power,
                'max_gen': max_power,
                'Type': plant['Type']
            })
            total_allocated += allocated_gen
            break  # Demand met—exit loop
        else:
            allocated_gen = max_power
            allocation.append({
                'plant_code': plant_code,
                'allocated_gen': allocated_gen,
                'min_gen': min_power,
                'max_gen': max_power,
                'Type': plant['Type']
            })
            total_allocated += allocated_gen

    # Backward Adjustment to eliminate any excess generation
    excess_generation = total_allocated - net_demand
    if excess_generation > 0:
        for i in reversed(range(len(allocation))):
            plant_allocation = allocation[i]
            allocated_gen = plant_allocation['allocated_gen']
            min_gen = plant_allocation['min_gen']
            possible_reduction = allocated_gen - min_gen
            reduction = min(possible_reduction, excess_generation)
            if reduction > 0:
                allocation[i]['allocated_gen'] -= reduction
                excess_generation -= reduction
            if excess_generation <= 0:
                break

    plant_dict = {plant['Code']: plant for plant in plants}
    final_allocation = []
    total_cost = 0.0

    try:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(map_and_calculate, alloc, plant_dict)
                for alloc in allocation
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    final_allocation.append(result)
                    total_cost += result['net_cost']
                except Exception as e:
                    print(f"Error processing allocation: {e}")
    except Exception as e:
        print(f"Parallel processing failed: {e}")
        raise RuntimeError("Allocation failed due to parallel processing error") from e

    final_allocation.sort(key=lambda x: x['Variable_Cost'])
    return {"other_plant_data": final_allocation, "total_cost": total_cost}


def get_must_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    """
    Retrieves and processes data for "Must Run" plants,
    calculating total generated energy and total cost.
    """
    if not net_demand:
        return {"error": "Net demand parameters are required"}
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        sum_query = (
            "SELECT `name`, `Code`, `Rated_Capacity`, `PAF`, `PLF`, `Type`, "
            "`Technical_Minimum`, `Aux_Consumption`, `Variable_Cost`, `Max_Power`, `Min_Power` "
            "FROM `plant_details` WHERE `Type` = 'Must run' ORDER BY `Variable_Cost`"
        )
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        generated_energy_all = 0.0
        total_cost = 0.0
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

            generated_energy = round(float(plant_result[0]['Pred']) * 1000 * 0.25, 3)
            generated_energy_all += generated_energy

            variable_cost = float(plant['Variable_Cost'])
            total_cost += round(generated_energy * variable_cost, 2)

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
                "max_power": plant['Max_Power'],
                "min_power": plant['Min_Power'],
                "net_cost": round(generated_energy * variable_cost, 2)
            })

        cursor.close()
        conn.close()

        return {
            "plant_data": plant_data,
            "generated_energy_all": generated_energy_all,
            "total_cost": total_cost
        }
    except Exception as e:
        return {"error": str(e)}


def get_exchange_data(timestamp: str, cap_price: float) -> Union[List[Dict[str, Any]], Dict[str, str]]:
    """
    Fetches and processes IEX (Independent Electricity Exchange) data for a given timestamp.
    Adjusts Pred_Price based on a price cap and converts Qty_Pred to kWh.
    """
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        sum_query = (
            "SELECT `TimeStamp`, `Pred_Price`, `Qty_Pred` "
            "FROM `iex_data` WHERE `TimeStamp` = %s"
        )
        cursor.execute(sum_query, (timestamp,))
        sum_result = cursor.fetchall()

        for i in range(len(sum_result)):
            if sum_result[i]['Pred_Price'] > float(cap_price):
                sum_result[i]['Pred_Price'] = 0.0
            else:
                sum_result[i]['Qty_Pred'] = round(
                    sum_result[i]['Qty_Pred'] * 1000 * 0.25,
                    3
                )

        cursor.close()
        conn.close()

        return sum_result
    except mysql.connector.Error as err:
        return {"error": str(err)}
    except Exception as e:
        return {"error": str(e)}


def get_valid_plants(plants: List[Dict[str, Any]], db_config: Dict[str, str], user_timestamp: datetime, cursor) -> List[
    Dict[str, Any]]:
    """
    Checks each plant's table for data availability at the specified timestamp.
    """
    valid_plants = []

    for plant in plants:
        plant_code = plant["Code"]

        check_table_query = (
            "SELECT COUNT(*) as table_count "
            "FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s"
        )
        cursor.execute(check_table_query, (db_config["database"], plant_code))
        table_exists = cursor.fetchone()["table_count"] > 0

        if table_exists:
            data_query = (
                f"SELECT * FROM {plant_code} "
                "WHERE TimeStamp = %s LIMIT 1"
            )
            cursor.execute(data_query, (user_timestamp,))
            plant_data = cursor.fetchall()
            if plant_data and plant_data[0]["Actual"] > 0:
                plant["PAF"] = 1
                valid_plants.append(plant)
    return valid_plants


def get_other_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    """
    Retrieves and allocates energy generation for "Other" type plants based on net demand.
    Only considers plants whose monthly PAF column (Jan, Feb, Mar, etc.) is 'Y' for the given timestamp.
    """
    from datetime import datetime

    # Convert timestamp string to a datetime object
    dt_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

    if not net_demand:
        return {"error": "Net demand parameter is required"}

    try:
        net_demand = float(net_demand)
        if net_demand <= 0:
            return {"error": "Net demand must be greater than zero"}

        # Open a database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 1) Determine the correct monthly column based on the timestamp's month
        month_val = dt_obj.month

        # Map the month integer to the corresponding column in PAF_Details
        month_map = {
            1: "Jan",
            2: "Feb",
            3: "Mar",
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec"
        }
        paf_column = month_map.get(month_val, "Jan")  # Fallback to "Jan" if needed
        # print(paf_column)

        # 2) Dynamically build your query to select only rows where the monthly PAF column is 'Y'
        #    We are no longer selecting the monthly PAF column in the output.
        query = f"""
            SELECT 
                pd.name,
                pd.Code,
                pd.Rated_Capacity,
                pd.PAF,
                pd.PLF,
                pd.Type,
                pd.Technical_Minimum,
                pd.Aux_Consumption,
                pd.Variable_Cost,
                pd.Max_Power,
                pd.Min_Power
            FROM plant_details pd
            JOIN PAF_Details pfd 
                ON pd.Code = pfd.Code
            WHERE pd.Type = 'Other'
              AND pfd.`{paf_column}` = 'Y'
            ORDER BY pd.Variable_Cost ASC
        """

        # 3) Execute the query to get only those plants whose monthly PAF is 'Y'
        cursor.execute(query)
        plants = cursor.fetchall()
        # print(plants)

        # # 4) Filter out plants that do not have valid data at this timestamp
        # valid_plants = get_valid_plants(plants, db_config, dt_obj, cursor)

        # 5) Allocate generation among these valid plants
        # generation = allocate_generation(valid_plants, net_demand)
        generation = allocate_generation(plants, net_demand)

        # Close the cursor and connection
        cursor.close()
        conn.close()

        return generation

    except Exception as e:
        return {"error": str(e)}
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


# ------------------------------ API Routes ----------------------------------

@plantAPI.route('/', methods=['GET'])
def get_demand():
    """
    Flask route to handle GET requests for demand data and allocation.
    """
    start_date = request.args.get('start_date')
    start_date = start_date[:19]
    price_cap = request.args.get('price_cap')

    if not start_date:
        return jsonify({"error": "Start date parameter is required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        sum_query = "SELECT * FROM demand_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(sum_query, (start_date, start_date))
        sum_result = cursor.fetchall()

        sum_query_1 = "SELECT * FROM Banking_Data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(sum_query_1, (start_date, start_date))
        sum_result_1 = cursor.fetchall()
        banking_unit = sum_result_1[0]["Banking_Unit"]
        # print(banking_unit)

        if not sum_result:
            return jsonify({"error": "No demand data found for the given date"}), 404

        demand_results = []
        for demand in sum_result:
            actual_converted = round(float(demand['Demand(Actual)']) * 1000 * 0.25, 3)
            pred_converted = round(float(demand['Demand(Pred)']) * 1000 * 0.25, 3)
            pred_converted_banked = round(float(pred_converted+banking_unit), 3)
            # print(pred_converted_banked)
            # print(demand['TimeStamp'])

            must_run_data = get_must_run(pred_converted, demand['TimeStamp'])
            if "error" in must_run_data:
                return jsonify({"error": must_run_data["error"]}), 500

            # print(must_run_data)
            must_run_total_gen = must_run_data['generated_energy_all']
            must_run_plants = must_run_data['plant_data']
            # print("Must Run Energy: ", must_run_total_gen)

            iex_data_list = get_exchange_data(demand['TimeStamp'], price_cap)
            if isinstance(iex_data_list, dict) and "error" in iex_data_list:
                return jsonify({"error": iex_data_list["error"]}), 500

            if not iex_data_list:
                iex_data_list = [{"TimeStamp": demand['TimeStamp'], "Pred_Price": 0.0, "Qty_Pred": 0.0}]
            iex_data = iex_data_list[0]

            if iex_data["Pred_Price"] != 0:
                iex_cost = iex_data["Pred_Price"] * iex_data["Qty_Pred"]
                iex_gen = iex_data["Qty_Pred"]
            else:
                iex_cost = 0.0
                iex_gen = 0.0

            net_demand = pred_converted - float(must_run_total_gen)
            # print("Net demand after must run", net_demand)
            net_demand_2 = net_demand - iex_gen
            # print("Net demand after iex", net_demand_2)

            remaining_plants_data = get_other_run(net_demand_2, start_date)

            if remaining_plants_data.get('other_plant_data'):
                last_price = round(float(remaining_plants_data['other_plant_data'][-1]['Variable_Cost']), 2)
            else:
                last_price = round(must_run_plants[-1]['Variable_Cost'], 2)

            single_demand = {
                "TimeStamp": demand['TimeStamp'],
                "Demand(Actual)": actual_converted,
                "Demand(Pred)": pred_converted,
                "Must_Run": must_run_plants,
                "Must_Run_Total_Gen": must_run_total_gen,
                "Must_Run_Total_Cost": must_run_data['total_cost'],
                "IEX_Data": iex_data,
                "IEX_Cost": iex_cost,
                "Remaining_Plants": remaining_plants_data.get('other_plant_data', []),
                "Remaining_Plants_Total_Cost": remaining_plants_data.get('total_cost', 0.0),
                "Last_Price": last_price,
                "Cost_Per_Block": round(
                    (must_run_data['total_cost'] + iex_cost + remaining_plants_data.get('total_cost',
                                                                                        0.0)) / pred_converted,
                    2
                ) if pred_converted != 0 else 0.0
            }

            demand_results.append(single_demand)

        cursor.close()
        conn.close()

        return jsonify(demand_results[0]), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
