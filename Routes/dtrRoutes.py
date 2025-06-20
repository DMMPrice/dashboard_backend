from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os
import json

# Create a Blueprint
dtrApi = Blueprint('dtr', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0]  # Using guvnl_consumers for DTR routes
}

@dtrApi.route('/all', methods=['GET'])
def get_all_dtr_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT d.*, f.feeder_name 
            FROM dtr d
            LEFT JOIN feeder f ON d.feeder_id = f.feeder_id
        """)
        dtr_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify(dtr_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@dtrApi.route('/<string:dtr_id>', methods=['GET'])
def get_dtr_by_id(dtr_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT d.*, f.feeder_name 
            FROM dtr d
            LEFT JOIN feeder f ON d.feeder_id = f.feeder_id
            WHERE d.dtr_id = %s
        """, (dtr_id,))
        dtr_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if dtr_data:
            return jsonify(dtr_data), 200
        return jsonify({"error": "DTR record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@dtrApi.route('/', methods=['POST'])
def create_dtr_record():
    data = request.json
    required_fields = ['dtr_id', 'feeder_id', 'location_description', 'capacity_kva', 'residential_connections', 'installed_date']
    
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400
        
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # First check if feeder_id exists
        cursor.execute("SELECT feeder_id FROM feeder WHERE feeder_id = %s", (data['feeder_id'],))
        if not cursor.fetchone():
            return jsonify({"error": "Invalid feeder_id provided"}), 400
        
        cursor.execute("""
            INSERT INTO dtr 
            (dtr_id, feeder_id, location_description, capacity_kva, residential_connections, installed_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (data['dtr_id'], data['feeder_id'], data['location_description'],
              data['capacity_kva'], data['residential_connections'], data['installed_date']))
        
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return jsonify({"message": "Record created successfully", "dtr_id": data['dtr_id']}), 201
    except mysql.connector.Error as err:
        if err.errno == 1062:  # Duplicate entry error
            return jsonify({"error": "DTR ID already exists"}), 400
        return jsonify({"error": str(err)}), 500

@dtrApi.route('/<string:dtr_id>', methods=['PUT'])
def update_dtr_record(dtr_id):
    data = request.json
    updateable_fields = ['feeder_id', 'location_description', 'capacity_kva', 'residential_connections', 'installed_date']
    
    if not any(field in data for field in updateable_fields):
        return jsonify({"error": "No valid fields to update"}), 400
        
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Check if feeder_id exists if it's being updated
        if 'feeder_id' in data:
            cursor.execute("SELECT feeder_id FROM feeder WHERE feeder_id = %s", (data['feeder_id'],))
            if not cursor.fetchone():
                return jsonify({"error": "Invalid feeder_id provided"}), 400
        
        # Build update query dynamically based on provided fields
        update_fields = []
        update_values = []
        for field in updateable_fields:
            if field in data:
                update_fields.append(f"{field} = %s")
                update_values.append(data[field])
        
        update_values.append(dtr_id)  # Add dtr_id for WHERE clause
        update_query = f"""
            UPDATE dtr 
            SET {', '.join(update_fields)}
            WHERE dtr_id = %s
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

@dtrApi.route('/<string:dtr_id>', methods=['DELETE'])
def delete_dtr_record(dtr_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("DELETE FROM dtr WHERE dtr_id = %s", (dtr_id,))
        conn.commit()
        
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        
        if affected_rows > 0:
            return jsonify({"message": "Record deleted successfully"}), 200
        return jsonify({"error": "Record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@dtrApi.route('/stats', methods=['GET'])
def get_dtr_stats():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_dtrs,
                COUNT(DISTINCT feeder_id) as total_feeders,
                SUM(capacity_kva) as total_capacity,
                AVG(capacity_kva) as avg_capacity,
                SUM(residential_connections) as total_connections
            FROM dtr
        """)
        stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify(stats), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@dtrApi.route('/by-feeder/<string:feeder_id>', methods=['GET'])
def get_dtr_by_feeder(feeder_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT d.*, f.feeder_name 
            FROM dtr d
            LEFT JOIN feeder f ON d.feeder_id = f.feeder_id
            WHERE d.feeder_id = %s
        """, (feeder_id,))
        
        dtr_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify(dtr_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
