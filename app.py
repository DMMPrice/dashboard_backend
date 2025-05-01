from flask import Flask, jsonify, request
from flask_cors import CORS
from Routes.demandRoutes import demandApi
from Routes.iexRoutes import iexApi
from Routes.procurementRoutes import procurementAPI
from Routes.plantRoutes import plantAPI
from Routes.BankingRoutes import bankingAPI
from Routes.availibilityfactorRoutes import availabilityAPI
import mysql.connector
import json

# MySQL configuration
db_config = {
    'user': 'DB-Admin',
    'password': 'DBTest@123',
    'host': '69.62.74.149',
    'database': 'guvnldev'
}
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(procurementAPI, url_prefix='/procurement')
app.register_blueprint(plantAPI, url_prefix='/plant')
app.register_blueprint(demandApi, url_prefix='/demand')
app.register_blueprint(bankingAPI, url_prefix='/banking')
app.register_blueprint(iexApi, url_prefix='/iex')
app.register_blueprint(availabilityAPI, url_prefix='/availability')


@app.route('/dashboard', methods=['GET'])
def get_data_with_sum():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # ── 1️⃣ Raw demand data ──────────────────────────────
        cursor.execute(
            "SELECT * "
            "FROM demand_data "
            "WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        demand_rows = cursor.fetchall()

        # compute sums for demand_data (adjust field names to your schema)
        total_actual = sum(r.get('Actual_Demand', 0) for r in demand_rows)
        total_predicted = sum(r.get('Predicted_Demand', 0) for r in demand_rows)

        # ── 2️⃣ IEX data ────────────────────────────────────
        cursor.execute(
            "SELECT * "
            "FROM iex_data "
            "WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        iex_rows = cursor.fetchall()

        # example: sum some numeric field in iex_data
        total_iex_value = sum(r.get('SomeIexMetric', 0) for r in iex_rows)

        # ── 3️⃣ Procurement data ────────────────────────────
        cursor.execute(
            "SELECT * FROM demand_output WHERE `TimeStamp` BETWEEN %s AND %s",
            (start_date, end_date)
        )
        procurement_rows = cursor.fetchall()

        # for each row, parse the JSON-string columns
        for row in procurement_rows:
            # iex_data is a JSON-string: e.g. "{\"Qty_Pred\": 0, …}"
            if row.get("iex_data"):
                try:
                    row["iex_data"] = json.loads(row["iex_data"])
                except json.JSONDecodeError:
                    # leave it as string if it really isn't JSON
                    pass

            # must_run is a JSON array in string form
            if row.get("must_run"):
                try:
                    row["must_run"] = json.loads(row["must_run"])
                except json.JSONDecodeError:
                    pass

            # remaining_plants likewise
            if row.get("remaining_plants"):
                try:
                    row["remaining_plants"] = json.loads(row["remaining_plants"])
                except json.JSONDecodeError:
                    pass

        cursor.close()
        conn.close()

        return jsonify({
            "demand": demand_rows,
            "iex": iex_rows,
            "procurement": procurement_rows,
        })

    except mysql.connector.Error as err:
        # you might want to log err.errno, err.msg, etc.
        return jsonify({"error": str(err)}), 500


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, threaded=True, debug=True)  # Run the app on all available IP addresses
