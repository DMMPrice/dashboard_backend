# app.py
from flask import Flask
from flask_cors import CORS
from Routes.demandRoutes import demandApi
from Routes.windRoutes import windApi

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(demandApi, url_prefix='/demand')
app.register_blueprint(windApi, url_prefix='/wind')


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000)  # Run the app on all available IP addresses
