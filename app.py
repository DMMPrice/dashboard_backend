from flask import Flask, jsonify, request
from flask_cors import CORS
from Routes.demandRoutes import demandApi
from Routes.procurementRoutes import plantAPI
from Routes.plantRoutes import procurementAPI
from Routes.consumerRoutes import consumerAPI
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
