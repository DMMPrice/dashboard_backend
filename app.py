from flask import Flask, jsonify, request
from flask_cors import CORS
from Routes.demandRoutes import demandApi
from Routes.solarRoutes import solarApi
from Routes.windRoutes import windApi
import requests

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(demandApi, url_prefix='/demand')
app.register_blueprint(windApi, url_prefix='/wind')
app.register_blueprint(solarApi, url_prefix='/solar')


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)  # Run the app on all available IP addresses
