from flask import Flask, jsonify, request
from flask_cors import CORS
from GetRoutes.demandRoutes import demandApi
from GetRoutes.procurementRoutes import plantAPI
from GetRoutes.plantRoutes import procurementAPI
from GetRoutes.consumerRoutes import consumerAPI
import requests

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(procurementAPI, url_prefix='/procurement')
app.register_blueprint(plantAPI, url_prefix='/plant')
app.register_blueprint(demandApi, url_prefix='/demand')
app.register_blueprint(consumerAPI, url_prefix='/consumer')


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, threaded=True, debug=True)  # Run the app on all available IP addresses
