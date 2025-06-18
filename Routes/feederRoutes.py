from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os
import json

# Create a Blueprint
feederApi = Blueprint('feeder', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': 'guvnl_consumers',
}

@feederApi.route('/all', methods=['GET'])
def get_all_feeder_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM feeder")
        feeder_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify(feeder_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@feederApi.route('/<int:id>', methods=['GET'])
def get_feeder_by_id(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM feeder_power_theft WHERE id = %s", (id,))
        feeder_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if feeder_data:
            return jsonify(feeder_data), 200
        return jsonify({"error": "Feeder record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@feederApi.route('/', methods=['POST'])
def create_feeder_record():
    data = request.json
    required_fields = ['feeder_id', 'substation_id', 'feeder_name']
    
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400
        
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""            INSERT INTO feeder 
            (feeder_id, substation_id, feeder_name)
            VALUES (%s, %s, %s)
        """, (data['feeder_id'], data['substation_id'], data['feeder_name']))
        
        conn.commit()
        new_id = cursor.lastrowid
        
        cursor.close()
        conn.close()
        
        return jsonify({"message": "Record created successfully", "id": new_id}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@feederApi.route('/<int:id>', methods=['PUT'])
def update_feeder_record(id):
    data = request.json
    updateable_fields = ['date', 'feeder_name', 'units_assessed', 'amount_assessed', 'amount_realized']
    
    if not any(field in data for field in updateable_fields):
        return jsonify({"error": "No valid fields to update"}), 400
        
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Build update query dynamically based on provided fields
        update_fields = []
        update_values = []
        for field in updateable_fields:
            if field in data:
                update_fields.append(f"{field} = %s")
                update_values.append(data[field])
        
        update_values.append(id)  # Add id for WHERE clause
        update_query = f"""
            UPDATE feeder_power_theft 
            SET {', '.join(update_fields)}
            WHERE id = %s
        """
        
        cursor.execute(update_query, tuple(update_values))
        conn.commit()
        
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        
        if affected_rows > 0:
            return jsonify({"message": "Record updated successfully"}), 200
        return jsonify({"error": "Record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@feederApi.route('/<int:id>', methods=['DELETE'])
def delete_feeder_record(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("DELETE FROM feeder_power_theft WHERE id = %s", (id,))
        conn.commit()
        
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        
        if affected_rows > 0:
            return jsonify({"message": "Record deleted successfully"}), 200
        return jsonify({"error": "Record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@feederApi.route('/by-substation/<string:substation_id>', methods=['GET'])
def get_feeders_by_substation(substation_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Get all feeders for the given substation with their details
        query = """
            SELECT f.feeder_id, f.feeder_name, f.substation_id, s.substation_name
            FROM feeder f
            LEFT JOIN substation s ON f.substation_id = s.substation_id
            WHERE f.substation_id = %s
            ORDER BY f.feeder_name
        """
        cursor.execute(query, (substation_id,))
        feeders = cursor.fetchall()
        
        cursor.close()
        conn.close()

        if not feeders:
            return jsonify({"message": "No feeders found for this substation"}), 404
            
        return jsonify({
            "status": "success",
            "data": feeders
        }), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
