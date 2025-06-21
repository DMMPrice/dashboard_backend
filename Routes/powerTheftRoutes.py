from flask import Blueprint, jsonify

powerTheftApi = Blueprint('powerTheftApi', __name__)

@powerTheftApi.route('/power-theft/analysis', methods=['GET'])
def power_theft_analysis():
    return jsonify([]), 200