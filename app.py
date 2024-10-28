# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from Routes.demandRoutes import demandApi
from Routes.solarRoutes import solarApi
from Routes.windRoutes import windApi
from Routes.intrastateRoutes import intraStateApi
from Routes.interstateRoutes import interStateApi
import requests

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(demandApi, url_prefix='/demand')
app.register_blueprint(windApi, url_prefix='/wind')
app.register_blueprint(solarApi, url_prefix='/solar')
app.register_blueprint(intraStateApi, url_prefix='/intra-state')
app.register_blueprint(interStateApi, url_prefix='/inter-state')


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


@app.route('/renewable')
def renewable():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        # Fetch data from /wind/sum_wind
        wind_response = requests.get('http://3.109.157.36:4000/wind/sum_wind',
                                     params={'start_date': start_date, 'end_date': end_date})
        wind_data = wind_response.json()

        # Fetch data from /solar/sum_solar
        solar_response = requests.get('http://3.109.157.36:4000/solar/sum_solar',
                                      params={'start_date': start_date, 'end_date': end_date})
        solar_data = solar_response.json()

        # Combine the results
        result = {
            "wind": wind_data,
            "solar": solar_data
        }

        return jsonify(result)
    except requests.RequestException as e:
        return jsonify({"error": str(e)})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)  # Run the app on all available IP addresses
