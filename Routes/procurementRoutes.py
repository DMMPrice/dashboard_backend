from flask import Blueprint, jsonify, request
import mysql.connector
import concurrent.futures
from typing import List, Dict, Any, Tuple, Union
from datetime import datetime
import pprint

# ----------------------------- Blueprint Setup -----------------------------

# Create a Flask Blueprint named 'plant' to modularize the API routes
plantAPI = Blueprint('plant', __name__)

# -------------------------- Database Configuration --------------------------

# MySQL database connection configuration
db_config = {
    'user': 'admin',
    'password': 'Babai123',
    'host': 'guvnl-dev.cv4e0cyc8rtk.ap-south-1.rds.amazonaws.com',
    'database': 'guvnl_dev'
}


# ----------------------------- Helper Functions -----------------------------

def map_and_calculate(alloc: Dict[str, Any], plant_dict: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Maps a single allocation to plant data and calculates the Plant Load Factor (plf) and Net Cost.

    Parameters:
    - alloc (dict): Allocation details for a single plant, containing:
        - 'plant_code' (str): Unique identifier for the plant.
        - 'allocated_gen' (float): Amount of energy allocated to the plant.
        - 'min_gen' (float): Minimum generation capacity of the plant.
        - 'max_gen' (float): Maximum generation capacity of the plant.
    - plant_dict (dict): Dictionary mapping plant codes to their respective data.

    Returns:
    - dict: Detailed allocation with calculated metrics, including:
        - 'plant_name' (str)
        - 'plant_code' (str)
        - 'rated_capacity' (float)
        - 'paf' (float): Plant Availability Factor.
        - 'Aux_Consumption' (float): Auxiliary consumption rate.
        - 'plf' (float): Plant Load Factor.
        - 'Variable_Cost' (float): Cost per unit of energy.
        - 'max_power' (float)
        - 'min_power' (float)
        - 'generated_energy' (float): Energy generated.
        - 'net_cost' (float): Total cost for the generated energy.
    """
    # Extract allocation details
    plant_code = alloc['plant_code']
    allocated_gen = alloc['allocated_gen']
    min_gen = alloc['min_gen']
    max_gen = alloc['max_gen']

    # Retrieve corresponding plant data from the dictionary
    plant = plant_dict.get(plant_code, {})
    plant_name = plant.get('name', 'Unknown Plant')
    rated_capacity = plant.get('Rated_Capacity', 0.0)
    paf = plant.get('PAF', 0.0)
    aux_consumption = plant.get('Aux_Consumption', 0.0)
    variable_cost = plant.get('Variable_Cost', 0.0)

    # Calculate Plant Load Factor (plf)
    denominator = rated_capacity * 1000 * 0.25 * paf * (1 - aux_consumption)
    plf = allocated_gen / denominator if denominator != 0 else 0.0

    # Calculate Net Cost
    net_cost = allocated_gen * variable_cost

    # Return detailed allocation data
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
    Allocates energy generation to plants based on their Variable Cost, ensuring each plant meets its Minimum Power.
    Utilizes parallel processing to calculate detailed allocation metrics.

    Parameters:
    - plants (list of dict): Pre-sorted list containing plant data in ascending order of 'Variable_Cost'.
      Each plant dictionary should include:
        - 'Code' (str)
        - 'Name' (str)
        - 'Rated_Capacity' (float)
        - 'PAF' (float)
        - 'Aux_Consumption' (float)
        - 'Variable_Cost' (float)
        - 'Max_Power' (float)
        - 'Min_Power' (float)
    - net_demand (float): The total net demand to be satisfied (in kWh).

    Returns:
    - dict:
        - 'other_plant_data' (list of dict): Detailed allocation for each plant.
        - 'total_cost' (float): Total cost of the allocation.
    """
    # Validate net demand input
    if net_demand <= 0:
        raise ValueError("Net demand must be greater than zero")

    # Step 1: Use pre-sorted plants (Assumes ascending order by 'Variable_Cost') - O(1)
    sorted_plants = plants

    allocation = []  # List to hold initial allocation details
    total_allocated = 0.0  # Tracks total energy allocated

    # Step 2: Initial Allocation - Assign Max_Power to each plant until net_demand is met or exceeded - O(n)
    for plant in sorted_plants:
        plant_code = plant['Code']
        max_power = plant['Max_Power']
        min_power = plant['Min_Power']

        # Skip plants that cannot contribute (Max_Power == 0)
        if max_power == 0:
            continue

        allocated_gen = max_power
        allocation.append({
            'plant_code': plant_code,
            'allocated_gen': allocated_gen,
            'min_gen': min_power,
            'max_gen': max_power,
            'Type': plant['Type']
        })
        total_allocated += allocated_gen

        # Stop allocating once demand is met or exceeded
        if total_allocated >= net_demand:
            break

    # Step 3: Backward Adjustment - Reduce allocations to eliminate excess - O(n)
    excess_generation = total_allocated - net_demand

    if excess_generation > 0:
        # Iterate backward through the allocation list to reduce excess
        for i in reversed(range(len(allocation))):
            plant_allocation = allocation[i]
            allocated_gen = plant_allocation['allocated_gen']
            min_gen = plant_allocation['min_gen']

            # Calculate possible reduction without going below Min_Power
            possible_reduction = allocated_gen - min_gen
            reduction = min(possible_reduction, excess_generation)

            if reduction > 0:
                allocation[i]['allocated_gen'] -= reduction
                excess_generation -= reduction

            # If excess generation is eliminated, exit the loop
            if excess_generation <= 0:
                break

    # Step 4: Map allocations to plant data and compute metrics using Parallel Processing - O(n)
    # Create a dictionary for quick lookup of plant data by Code - O(n)
    plant_dict = {plant['Code']: plant for plant in plants}

    final_allocation = []  # List to hold detailed allocation results
    total_cost = 0.0  # Accumulates the total cost of allocations

    try:
        # Initialize a ProcessPoolExecutor for parallel processing
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # Submit map_and_calculate tasks for each allocation entry
            futures = [
                executor.submit(map_and_calculate, alloc, plant_dict)
                for alloc in allocation
            ]

            # As each future completes, append the result to final_allocation and update total_cost
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    final_allocation.append(result)
                    total_cost += result['net_cost']
                except Exception as e:
                    # Log any errors encountered during parallel processing
                    print(f"Error processing allocation: {e}")
    except Exception as e:
        # Handle exceptions related to parallel processing
        print(f"Parallel processing failed: {e}")
        raise RuntimeError("Allocation failed due to parallel processing error") from e

    # Optional: Sort final_allocation by Variable_Cost ascending for readability - O(k log k)
    final_allocation.sort(key=lambda x: x['Variable_Cost'])

    # Step 5: Return Allocation and Total Cost - O(1)
    return {"other_plant_data": final_allocation, "total_cost": total_cost}


def get_must_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    """
    Retrieves and processes data for "Must Run" plants, calculating total generated energy and total cost.

    Parameters:
    - net_demand (float): The net demand that needs to be met (in kWh).
    - timestamp (str): The specific timestamp for which data is fetched.

    Returns:
    - dict:
        - 'plant_data' (list of dict): Detailed data for each "Must Run" plant.
        - 'generated_energy_all' (float): Total energy generated by all "Must Run" plants.
        - 'total_cost' (float): Total cost of generating the energy.
        - 'error' (str, optional): Error message in case of failure.
    """
    # Validate net_demand parameter
    if not net_demand:
        return {"error": "Net demand parameters are required"}
    try:
        # Establish database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Query to fetch details of "Must Run" plants
        sum_query = (
            "SELECT `name`, `Code`, `Rated_Capacity`, `PAF`, `PLF`, `Type`, "
            "`Technical_Minimum`, `Aux_Consumption`, `Variable_Cost`, `Max_Power`, `Min_Power` "
            "FROM `plant_details` WHERE `Type` = 'Must run'"
        )
        cursor.execute(sum_query)
        sum_result = cursor.fetchall()

        generated_energy_all = 0.0  # Total energy generated by "Must Run" plants
        total_cost = 0.0  # Total cost for "Must Run" plants
        plant_data = []  # List to store detailed plant data

        # Iterate through each "Must Run" plant and calculate generated energy and cost
        for plant in sum_result:
            code = plant['Code']
            try:
                # Query to fetch predicted energy generation for the plant at the given timestamp
                plant_query = f"SELECT `TimeStamp`, `Pred` FROM `{code}` WHERE `TimeStamp` = %s"
                cursor.execute(plant_query, (timestamp,))
                plant_result = cursor.fetchall()

                # If no data is found for the timestamp, assume zero generation
                if not plant_result:
                    plant_result = [{"TimeStamp": timestamp, "Pred": 0.00}]
            except mysql.connector.Error:
                # In case of a database error, assume zero generation
                plant_result = [{"TimeStamp": timestamp, "Pred": 0.00}]

            # Convert Pred (MW) to kWh by multiplying by 1000 and then by 0.25 (assuming 15-minute intervals)
            generated_energy = round(float(plant_result[0]['Pred']) * 1000 * 0.25, 3)
            generated_energy_all += generated_energy

            # Calculate cost for the generated energy
            variable_cost = float(plant['Variable_Cost'])
            total_cost += round(generated_energy * variable_cost, 2)

            # Append detailed plant data to the list
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

        # Close database cursor and connection
        cursor.close()
        conn.close()

        # Return the aggregated data
        return {
            "plant_data": plant_data,
            "generated_energy_all": generated_energy_all,
            "total_cost": total_cost
        }
    except Exception as e:
        # Handle any unexpected exceptions
        return {"error": str(e)}


def get_exchange_data(timestamp: str, cap_price: float) -> Union[List[Dict[str, Any]], Dict[str, str]]:
    """
    Fetches and processes IEX (Independent Electricity Exchange) data for a given timestamp.
    Adjusts Pred_Price based on a price cap and converts Qty_Pred to kWh.

    Parameters:
    - timestamp (str): The specific timestamp for which IEX data is fetched.
    - cap_price (float): The maximum allowed Pred_Price. Prices above this cap are set to 0.

    Returns:
    - list of dicts: Processed IEX data entries with adjusted prices and quantities.
    - dict: Error message in case of failure.
    """
    try:
        # Establish database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Query to fetch IEX data for the given timestamp
        sum_query = (
            "SELECT `TimeStamp`, `Pred_Price`, `Qty_Pred` "
            "FROM `iex_data` WHERE `TimeStamp` = %s"
        )
        cursor.execute(sum_query, (timestamp,))
        sum_result = cursor.fetchall()

        # Process each IEX data row
        for i in range(len(sum_result)):
            if sum_result[i]['Pred_Price'] > float(cap_price):
                # If Pred_Price exceeds the cap, set it to 0
                sum_result[i]['Pred_Price'] = 0.0
            else:
                # Otherwise, convert Qty_Pred to kWh by multiplying by 1000 and then by 0.25
                sum_result[i]['Qty_Pred'] = round(
                    sum_result[i]['Qty_Pred'] * 1000 * 0.25,
                    3
                )

        # Close database cursor and connection
        cursor.close()
        conn.close()

        # Return the processed IEX data
        return sum_result
    except mysql.connector.Error as err:
        # Handle MySQL-specific errors
        return {"error": str(err)}
    except Exception as e:
        # Handle any unexpected exceptions
        return {"error": str(e)}


def get_valid_plants(plants: List[Dict[str, Any]], db_config: Dict[str, str], user_timestamp: datetime, cursor) -> List[
    Dict[str, Any]]:
    """
    Checks each plant's table for data availability at the specified timestamp.

    Parameters:
    - plants (List[Dict[str, Any]]): List of plant details, where each plant has a "Code" key for the table name.
    - db_config (Dict[str, str]): Database configuration dictionary.
    - user_timestamp (datetime): Timestamp for which data availability is checked.
    - cursor: Database cursor object for executing queries.

    Returns:
    - List[Dict[str, Any]]: List of plants with valid data for the given timestamp.
    """
    valid_plants = []  # List to hold plants with valid data for the specified timestamp

    for plant in plants:
        plant_code = plant["Code"]  # Table name is the same as the plant code

        # Check if the table exists
        check_table_query = (
            "SELECT COUNT(*) as table_count "
            "FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s"
        )
        cursor.execute(check_table_query, (db_config["database"], plant_code))
        table_exists = cursor.fetchone()["table_count"] > 0

        if table_exists:
            # Query for data availability for the given timestamp
            data_query = (
                f"SELECT * FROM {plant_code} "
                "WHERE TimeStamp = %s LIMIT 1"
            )
            cursor.execute(data_query, (user_timestamp,))
            plant_data = cursor.fetchall()

            # To check if there is data or not

            if plant_data:
                if plant_data[0]["Actual"] > 0:
                    plant["PAF"] = 1
                    valid_plants.append(plant)  # Add plant to valid list if data exists
    return valid_plants


def get_other_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    """
    Retrieves and allocates energy generation for "Other" type plants based on net demand.

    Parameters:
    ----------
    net_demand : float
        The remaining net demand after accounting for "Must Run" and IEX allocations (in kWh).
    timestamp : str
        The specific timestamp for which the generation data is required.
        Expected format: 'YYYY-MM-DD HH:MM:SS'.

    Returns:
    -------
    dict
        A dictionary containing:
        - 'other_plant_data' (list of dict): Detailed allocation for each "Other" type plant. Each dictionary in the
          list includes:
            - 'plant_name' (str): Name of the plant.
            - 'plant_code' (str): Unique code for the plant.
            - 'rated_capacity' (float): Rated capacity of the plant in MW.
            - 'paf' (float): Plant Availability Factor.
            - 'Aux_Consumption' (float): Auxiliary consumption rate.
            - 'plf' (float): Plant Load Factor.
            - 'Variable_Cost' (float): Cost per unit of energy.
            - 'max_power' (float): Maximum generation capacity of the plant.
            - 'min_power' (float): Minimum generation capacity of the plant.
            - 'generated_energy' (float): Energy generated in kWh.
            - 'net_cost' (float): Total cost for the generated energy.
        - 'total_cost' (float): Total cost of the allocated energy.
        - 'error' (str, optional): An error message in case of failure.

    Raises:
    ------
    ValueError
        If the net demand is less than or equal to zero.

    Notes:
    -----
    - The function ensures all database connections are properly closed, even in the event of errors.
    - If no valid plants are found or an error occurs during database operations, an error message is returned.
    - The function handles exceptions gracefully to provide meaningful error messages.

    Examples:
    --------
    # >>> get_other_run(5000.0, "2024-05-01 12:00:00")
    {
        'other_plant_data': [
            {
                'plant_name': 'Plant A',
                'plant_code': 'PLT001',
                'rated_capacity': 200.0,
                'paf': 0.85,
                'Aux_Consumption': 0.05,
                'plf': 0.76,
                'Variable_Cost': 2.5,
                'max_power': 180.0,
                'min_power': 50.0,
                'generated_energy': 4000.0,
                'net_cost': 10000.0
            }
        ],
        'total_cost': 10000.0
    }
    """
    # Validate net_demand parameter
    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    # if timestamp.year >= 2021:
    #     timestamp = timestamp.replace(year=2021)
    timestamp = timestamp.replace(year=2021)
    # print(timestamp)

    if not net_demand:
        return {"error": "Net demand parameter is required"}

    try:
        net_demand = float(net_demand)
        if net_demand <= 0:
            return {"error": "Net demand must be greater than zero"}

        # Establish database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Query to fetch "Other" type plants, ordered by Variable_Cost ascending
        query = (
            "SELECT name, Code, Rated_Capacity, PAF, PLF, Type, "
            "Technical_Minimum, Aux_Consumption, Variable_Cost, Max_Power, Min_Power "
            "FROM plant_details WHERE Type = 'Other' "
            "ORDER BY Variable_Cost ASC"
        )
        cursor.execute(query)
        plants = cursor.fetchall()

        # Fetch valid plants with data for the specified timestamp
        valid_plants = get_valid_plants(plants, db_config, timestamp, cursor)

        # Allocate generation using the allocate_generation function
        generation = allocate_generation(valid_plants, net_demand)

        # Close database cursor and connection
        cursor.close()
        conn.close()

        # Return the allocation results
        return generation
    except Exception as e:
        # Handle any unexpected exceptions
        return {"error": str(e)}
    finally:
        # Ensure that database connections are closed even if an error occurs
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


# ------------------------------ API Routes ----------------------------------

@plantAPI.route('/', methods=['GET'])
def get_demand():
    """
    Flask route to handle GET requests for demand data and allocation.

    Query Parameters:
    - start_date (str): The specific timestamp for which demand data is fetched.
    - price_cap (float): The maximum allowed price for IEX data.

    Returns:
    - JSON response containing:
        - 'TimeStamp' (str)
        - 'Demand(Actual)' (float): Actual demand in kWh.
        - 'Demand(Pred)' (float): Predicted demand in kWh.
        - 'Must_Run' (list of dict): Allocation details for "Must Run" plants.
        - 'Must_Run_Total_Gen' (float): Total energy generated by "Must Run" plants.
        - 'Must_Run_Total_Cost' (float): Total cost for "Must Run" plants.
        - 'IEX_Data' (dict): Processed IEX data.
        - 'IEX_Cost' (float): Cost associated with IEX allocations.
        - 'Remaining_Plants' (list of dict): Allocation details for "Other" plants.
        - 'Remaining_Plants_Total_Cost' (float): Total cost for "Other" plants.
        - 'Last_Price' (float): Last Variable_Cost of allocated "Other" plants.
        - 'Cost_Per_Block' (float): Cost per unit of predicted demand.
    """
    # Extract query parameters from the request
    start_date = request.args.get('start_date')  # Expected format: 'YYYY-MM-DD HH:MM:SS'
    # formatted_date = start_date[:19]
    start_date = start_date[:19]
    price_cap = request.args.get('price_cap')  # Expected to be a float

    # Validate presence of start_date parameter
    if not start_date:
        return jsonify({"error": "Start date parameter is required"}), 400

    try:
        # Establish database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Query to fetch demand data for the given start_date
        sum_query = "SELECT * FROM demand_data WHERE `TimeStamp` BETWEEN %s AND %s"
        cursor.execute(sum_query, (start_date, start_date))
        sum_result = cursor.fetchall()

        # If no demand data is found for the given date, return a 404 error
        if not sum_result:
            return jsonify({"error": "No demand data found for the given date"}), 404

        demand_results = []  # List to store results for each demand entry

        # Iterate through each demand entry (typically one per timestamp)
        for demand in sum_result:
            # Convert Demand(Actual) & Demand(Pred) from MW to kWh by multiplying by 1000 and then by 0.25
            actual_converted = round(float(demand['Demand(Actual)']) * 1000 * 0.25, 3)
            pred_converted = round(float(demand['Demand(Pred)']) * 1000 * 0.25, 3)

            # 1) Must-run data: Fetch and process "Must Run" plant allocations
            must_run_data = get_must_run(pred_converted, demand['TimeStamp'])
            if "error" in must_run_data:
                return jsonify({"error": must_run_data["error"]}), 500

            must_run_total_gen = must_run_data['generated_energy_all']
            must_run_plants = must_run_data['plant_data']

            # 2) IEX data: Fetch and process IEX allocations based on price_cap
            iex_data_list = get_exchange_data(demand['TimeStamp'], price_cap)
            if isinstance(iex_data_list, dict) and "error" in iex_data_list:
                return jsonify({"error": iex_data_list["error"]}), 500

            # If no IEX data is found, default to zero values
            if not iex_data_list:
                iex_data_list = [{"TimeStamp": demand['TimeStamp'], "Pred_Price": 0.0, "Qty_Pred": 0.0}]
            iex_data = iex_data_list[0]

            # Calculate IEX Cost and generated energy based on Pred_Price
            if iex_data["Pred_Price"] != 0:
                iex_cost = iex_data["Pred_Price"] * iex_data["Qty_Pred"]
                iex_gen = iex_data["Qty_Pred"]
            else:
                iex_cost = 0.0
                iex_gen = 0.0

            # 3) Net demand after accounting for "Must Run" allocations
            net_demand = pred_converted - float(must_run_total_gen)

            # 4) Net demand after accounting for IEX allocations
            net_demand_2 = net_demand - iex_gen

            # 5) Other plant data: Fetch and allocate generation to "Other" plants based on remaining demand
            remaining_plants_data = get_other_run(net_demand_2, start_date)

            # Extract the last Variable_Cost for reference
            if remaining_plants_data.get('other_plant_data'):
                last_price = round(float(remaining_plants_data['other_plant_data'][-1]['Variable_Cost']), 2)
            else:
                last_price = 0.0  # Default if no other plant data is available

            # Build the final response structure for this demand entry
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
                "Last_Price": last_price,  # Rounded to 2 decimals

                # Calculate Cost Per Block: (Must Run Cost + IEX Cost + Other Plants Cost) / Predicted Demand
                "Cost_Per_Block": round(
                    (must_run_data['total_cost'] + iex_cost + remaining_plants_data.get('total_cost',
                                                                                        0.0)) / pred_converted,
                    2
                ) if pred_converted != 0 else 0.0
            }

            # Append the single demand result to the list
            demand_results.append(single_demand)

        # Close database cursor and connection
        cursor.close()
        conn.close()

        # Return the first (and typically only) demand result as JSON
        return jsonify(demand_results[0]), 200
    except mysql.connector.Error as err:
        # Handle MySQL-specific errors
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        # Handle any unexpected exceptions
        return jsonify({"error": str(e)}), 500
    finally:
        # Ensure that database connections are closed even if an error occurs
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
