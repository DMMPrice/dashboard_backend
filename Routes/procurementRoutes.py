from flask import Blueprint, jsonify, request
import mysql.connector
from typing import List, Dict, Any, Union
from datetime import datetime
from collections import OrderedDict

# ----------------------------- Blueprint Setup -----------------------------
procurementAPI = Blueprint('procurement', __name__)

# -------------------------- Database Configuration --------------------------
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'guvnl_dev'
}


# ----------------------------- Helper Functions -----------------------------

def map_and_calculate(alloc: Dict[str, Any], plant_dict: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Maps a single allocation to plant data and calculates PLF and Net Cost.
    """
    plant_code = alloc['plant_code']
    allocated = alloc['allocated_gen']
    plant = plant_dict.get(plant_code, {})

    rated = plant.get('Rated_Capacity', 0.0)
    paf = plant.get('PAF', 0.0)
    aux = plant.get('Aux_Consumption', 0.0)
    var_cost = plant.get('Variable_Cost', 0.0)

    denom = rated * 1000 * 0.25 * paf * (1 - aux) or 1.0
    plf = allocated / denom
    net_cost = allocated * var_cost

    return {
        'plant_name': plant.get('name', 'Unknown'),
        'plant_code': plant_code,
        'rated_capacity': rated,
        'paf': round(paf, 2),
        'Aux_Consumption': aux,
        'plf': round(plf, 4),
        'Variable_Cost': var_cost,
        'max_power': round(alloc['max_gen'], 3),
        'min_power': round(alloc['min_gen'], 3),
        'generated_energy': round(allocated, 3),
        'net_cost': round(net_cost, 2)
    }


def allocate_generation(
        plants: List[Dict[str, Any]],
        net_demand: float,
        backdown_table: List[Dict[str, float]]
) -> Dict[str, Union[float, List[Any]]]:
    if net_demand <= 0:
        raise ValueError("Net demand must be greater than zero")
    sorted_plants = sorted(plants, key=lambda p: p['Variable_Cost'])
    allocation = []
    total_alloc = 0.0
    for plant in sorted_plants:
        code = plant['Code']
        max_p = plant['Max_Power']
        min_p = plant['Min_Power']
        if max_p <= 0:
            continue
        rem = net_demand - total_alloc
        if rem <= 0:
            break
        if rem <= max_p:
            alloc_val = max(min_p, rem)
            allocation.append(
                {'plant_code': code, 'allocated_gen': alloc_val,
                 'min_gen': min_p, 'max_gen': max_p, 'Type': plant['Type']}
            )
            total_alloc += alloc_val
            break
        allocation.append(
            {'plant_code': code, 'allocated_gen': max_p,
             'min_gen': min_p, 'max_gen': max_p, 'Type': plant['Type']}
        )
        total_alloc += max_p
    excess = total_alloc - net_demand
    if excess > 0:
        for alloc in reversed(allocation):
            reducible = alloc['allocated_gen'] - alloc['min_gen']
            if reducible <= 0:
                continue
            red = min(reducible, excess)
            alloc['allocated_gen'] -= red
            excess -= red
            if excess <= 0:
                break
    plant_dict = {p['Code']: p for p in plants}
    final_list = []
    total_cost = 0.0
    for alloc in allocation:
        base = map_and_calculate(alloc, plant_dict)
        plf_pct = base['plf'] * 100
        SHR = Aux = 0.0
        for row in backdown_table:
            if row['lower'] <= plf_pct <= row['upper']:
                SHR = row['SHR']
                Aux = row['Aux_Consumption']
                break
        var_cost = base['Variable_Cost']
        max_gen = base['max_power']
        gen = base['generated_energy']
        backdown_rate = round(var_cost * ((1 + SHR / 100) / (1 - Aux / 100)), 2)
        backdown_cost = round(backdown_rate * (max_gen - gen), 2)
        base['backdown_rate'] = backdown_rate
        base['backdown_cost'] = backdown_cost
        final_list.append(base)
        total_cost += base['net_cost']
    final_list.sort(key=lambda x: x['Variable_Cost'])
    return {"other_plant_data": final_list, "total_cost": total_cost}


def get_must_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    if not net_demand:
        return {"error": "Net demand parameters are required"}
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        # Fetch must-run plant details
        cursor.execute(
            """
            SELECT name,
                   Code,
                   Rated_Capacity,
                   PAF,
                   PLF,
                   Type,
                   Technical_Minimum,
                   Aux_Consumption,
                   Variable_Cost,
                   Max_Power,
                   Min_Power
            FROM plant_details
            WHERE Type = 'Must run'
            ORDER BY Variable_Cost
            """
        )
        plants = cursor.fetchall()
        gen_all = 0.0
        cost_all = 0.0
        data = []
        for plant in plants:
            code = plant['Code']
            # Use LIMIT 1 to avoid unread-result errors
            try:
                cursor.execute(
                    f"SELECT Pred FROM `{code}` WHERE TimeStamp = %s LIMIT 1",
                    (timestamp,)
                )
                pred_row = cursor.fetchone() or {'Pred': 0.0}
            except mysql.connector.Error:
                pred_row = {'Pred': 0.0}
            gen_kwh = round(float(pred_row['Pred']) * 1000 * 0.25, 3)
            gen_all += gen_kwh
            var_cost = float(plant['Variable_Cost'])
            cost_all += round(gen_kwh * var_cost, 2)
            data.append({
                'plant_name': plant['name'],
                'plant_code': code,
                'Rated_Capacity': plant['Rated_Capacity'],
                'PAF': plant['PAF'],
                'PLF': plant['PLF'],
                'Type': plant['Type'],
                'Aux_Consumption': plant['Aux_Consumption'],
                'Variable_Cost': var_cost,
                'generated_energy': gen_kwh,
                'max_power': plant['Max_Power'],
                'min_power': plant['Min_Power'],
                'net_cost': round(gen_kwh * var_cost, 2)
            })
        cursor.close()
        conn.close()
        return {'plant_data': data, 'generated_energy_all': gen_all, 'total_cost': cost_all}
    except Exception as e:
        return {'error': str(e)}


def get_exchange_data(timestamp: str, cap_price: float) -> Union[List[Dict[str, Any]], Dict[str, str]]:
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT `TimeStamp`, `Pred_Price`, `Qty_Pred` FROM iex_data WHERE TimeStamp = %s",
            (timestamp,)
        )
        rows = cursor.fetchall()
        for r in rows:
            if r['Pred_Price'] > float(cap_price):
                r['Pred_Price'] = 0.0
            else:
                r['Qty_Pred'] = round(r['Qty_Pred'] * 1000 * 0.25, 3)
        cursor.close()
        conn.close()
        return rows
    except mysql.connector.Error as err:
        return {'error': str(err)}
    except Exception as e:
        return {'error': str(e)}


def get_other_run(net_demand: float, timestamp: str) -> Dict[str, Any]:
    if net_demand is None or float(net_demand) <= 0:
        return {'error': 'Net demand must be greater than zero'}
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    col = month_map[dt.month]
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"""
        SELECT pd.name, pd.Code, pd.Rated_Capacity, pd.PAF, pd.PLF, pd.Type,
               pd.Technical_Minimum, pd.Aux_Consumption, pd.Variable_Cost,
               pd.Max_Power, pd.Min_Power
        FROM plant_details pd
        JOIN PAF_Details pfd ON pd.Code=pfd.Code
        WHERE pd.Type='Other' AND pfd.`{col}`='Y'
        ORDER BY pd.Variable_Cost ASC
    """)
    plants = cursor.fetchall()
    cursor.execute(
        "SELECT Start_Load, End_Load, SHR, Aux_Consumption FROM Back_Down_Table"
    )
    bd_rows = cursor.fetchall()
    backdown_table = [
        {'lower': r['Start_Load'], 'upper': r['End_Load'], 'SHR': r['SHR'], 'Aux_Consumption': r['Aux_Consumption']} for
        r in bd_rows]
    cursor.close()
    conn.close()
    return allocate_generation(plants, float(net_demand), backdown_table)


@procurementAPI.route('/', methods=['GET'])
def get_demand():
    start_date = request.args.get('start_date')
    price_cap = request.args.get('price_cap', 0)

    if not start_date:
        return jsonify({'error': 'Start date parameter is required'}), 400
    start_date = start_date[:19]

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # fetch demand
        cursor.execute(
            "SELECT `Demand(Actual)`, `Demand(Pred)`, `TimeStamp` "
            "FROM demand_data WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, start_date)
        )
        demand_row = cursor.fetchone()

        # fetch banking
        cursor.execute(
            "SELECT Banking_Unit FROM Banking_Data WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, start_date)
        )
        bank_row = cursor.fetchone() or {'Banking_Unit': 0}
        banking_unit = bank_row.get('Banking_Unit', 0) or 0

        if not demand_row:
            return jsonify({'error': 'No demand data found for the given date'}), 404

        # convert to kWh
        actual_kwh = round(float(demand_row['Demand(Actual)']) * 1000 * 0.25, 3)
        pred_kwh = round(float(demand_row['Demand(Pred)']) * 1000 * 0.25, 3)
        pred_banked = pred_kwh + banking_unit

        # must-run
        must = get_must_run(pred_banked, demand_row['TimeStamp'])
        if 'error' in must:
            return jsonify({'error': must['error']}), 500

        # IEX
        iex_list = get_exchange_data(demand_row['TimeStamp'], price_cap)
        if isinstance(iex_list, dict) and 'error' in iex_list:
            return jsonify({'error': iex_list['error']}), 500
        iex = iex_list[0] if iex_list else {'Pred_Price': 0.0, 'Qty_Pred': 0.0}
        iex_cost = iex['Pred_Price'] * iex['Qty_Pred'] if iex['Pred_Price'] else 0.0
        iex_gen = iex['Qty_Pred'] if iex['Pred_Price'] else 0.0

        # remaining
        net1 = pred_banked - must['generated_energy_all']
        net2 = net1 - iex_gen
        other = get_other_run(net2, start_date)
        if 'error' in other:
            return jsonify({'error': other['error']}), 500

        rem_plants = other['other_plant_data']
        rem_cost = other['total_cost']
        rem_gen = sum(p['generated_energy'] for p in rem_plants)

        # ─────────────── BANKING‐CHECK FOR BACKDOWN ───────────────
        if banking_unit == 0:
            # zero out each plant’s backdown_cost
            for p in rem_plants:
                p['backdown_cost'] = 0.0
            total_backdown = 0.0
        else:
            # sum up precomputed backdown_costs
            total_backdown = sum(p['backdown_cost'] for p in rem_plants)
        # ────────────────────────────────────────────────────────────
        iex_price = iex['Pred_Price'] if iex['Qty_Pred'] > 0 else 0.0
        last_price = max(round(rem_plants[-1]['Variable_Cost'], 2), iex_price)
        cost_per_block = round((must['total_cost'] + iex_cost + rem_cost) / pred_banked, 2) if pred_banked else 0.0

        result = OrderedDict({
            'TimeStamp': demand_row['TimeStamp'],
            'Demand(Actual)': actual_kwh,
            'Demand(Pred)': pred_kwh,
            'Banking_Unit': banking_unit,
            'Demand_Banked': pred_banked,
            'Must_Run': must['plant_data'],
            'Must_Run_Total_Gen': must['generated_energy_all'],
            'Must_Run_Total_Cost': must['total_cost'],
            'IEX_Data': iex,
            'IEX_Gen': round(iex_gen, 3),
            'IEX_Cost': round(iex_cost, 2),
            'Remaining_Plants': rem_plants,
            'Remaining_Plants_Total_Gen': round(rem_gen, 3),
            'Remaining_Plants_Total_Cost': round(rem_cost, 2),
            'Last_Price': round(last_price, 2),
            'Cost_Per_Block': round(cost_per_block, 2),
            'Backdown_Cost': round(total_backdown, 2),
        })

        cursor.close()
        conn.close()
        return jsonify(result), 200

    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()


@procurementAPI.route('/range', methods=['GET'])
def get_demand_range():
    start = request.args.get('start')  # e.g. "2021-04-01 00:00:00"
    end = request.args.get('end')  # e.g. "2021-04-02 00:00:00"

    if not start or not end:
        return jsonify({"error": "Both 'start' and 'end' query parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # fetch raw rows (fixed: removed extra comma)
        cursor.execute("""
                       SELECT timestamp, cost_per_block, last_price
                       FROM demand_output
                       WHERE timestamp BETWEEN %s
                         AND %s
                       ORDER BY TimeStamp
                       """, (start, end))
        rows = cursor.fetchall()

        # calculate total and average
        total_cost_per_block = sum(r['cost_per_block'] for r in rows)
        average_cost_per_block = total_cost_per_block / len(rows) if rows else None

        total_mod = sum(r['last_price'] for r in rows)
        average_mod = total_mod / len(rows) if rows else None

        cursor.close()
        conn.close()

        return jsonify({
            "data": rows,
            "summary": {
                "total_cost_per_block": total_cost_per_block,
                "average_cost_per_block": round(average_cost_per_block, 2),
                "total_mod": total_mod,
                "average_mod": round(average_mod, 2)

            }
        })

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
