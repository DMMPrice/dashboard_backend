from flask import Flask, jsonify, request
from flask_cors import CORS
from Routes.demandRoutes import demandApi
from Routes.plantRoutes import plantAPI
from Routes.procurementRoutes import procurementAPI
import requests

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(procurementAPI, url_prefix='/procurement')
app.register_blueprint(plantAPI, url_prefix='/plant')


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)  # Run the app on all available IP addresses
