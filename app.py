from flask import Flask, jsonify, request
from flask_cors import CORS
from Routes.demandRoutes import demandApi
from Routes.iexRoutes import iexApi
from Routes.procurementRoutes import procurementAPI
from Routes.plantRoutes import plantAPI
from Routes.BankingRoutes import bankingAPI
from Routes.availibilityfactorRoutes import availabilityAPI
from Routes.dtrRoutes import dtrApi
from Routes.feederRoutes import feederApi
from Routes.substationRoutes import substationApi
from Routes.lowTensionRoutes import lowTensionApi
import mysql.connector
import json
from dotenv import load_dotenv
import os

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
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
app.register_blueprint(dtrApi, url_prefix='/dtr')
app.register_blueprint(feederApi, url_prefix='/feeder')
app.register_blueprint(substationApi, url_prefix='/substation')
app.register_blueprint(lowTensionApi, url_prefix='/low-tension')


@app.route('/dashboard', methods=['GET'])
def get_dashboard_data():
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({"error": "start_date and end_date are required"}), 400

        # Create database connection
        conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAMES').split(',')[0]  # Using first database
        )
        cursor = conn.cursor(dictionary=True)

        # Your dashboard query here
        cursor.execute("""
            SELECT * FROM dtr 
            WHERE DATE(installed_date) BETWEEN %s AND %s
        """, (start_date, end_date))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(results), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Add error handler for 500 errors
@app.errorhandler(500)
def handle_500_error(e):
    return jsonify({
        "error": "Internal server error",
        "message": str(e)
    }), 500


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True, debug=True)  # Run the app on all available IP addresses
