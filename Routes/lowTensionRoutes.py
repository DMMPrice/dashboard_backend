from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os
import json

# Create a Blueprint
lowTensionApi = Blueprint('low_tension', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0],
}

@lowTensionApi.route('/all', methods=['GET'])
def get_all_lt_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Query from power_theft.Low_Tension table
        cursor.execute("""
            SELECT 
                LT_ID as id,
                AREA as area,
                UNITS_ASSESSED as units_assessed,
                AMOUNT_ASSESSED as amount_assessed,
                AMOUNT_REALIZED as amount_realized,
                DATE as date,
                CREATED_AT as created_at,
                UPDATED_AT as updated_at
            FROM Low_Tension
        """)
        lt_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify(lt_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@lowTensionApi.route('/<int:id>', methods=['GET'])
def get_lt_by_id(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM low_tension_power_theft WHERE id = %s", (id,))
        lt_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if lt_data:
            return jsonify(lt_data), 200
        return jsonify({"error": "Low Tension record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@lowTensionApi.route('/', methods=['POST'])
def create_lt_record():
    data = request.json
    required_fields = ['date', 'area', 'units_assessed', 'amount_assessed', 'amount_realized']
    
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400
        
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            INSERT INTO low_tension_power_theft 
            (date, area, units_assessed, amount_assessed, amount_realized)
            VALUES (%s, %s, %s, %s, %s)
        """, (data['date'], data['area'], data['units_assessed'], 
              data['amount_assessed'], data['amount_realized']))
        
        conn.commit()
        new_id = cursor.lastrowid
        
        cursor.close()
        conn.close()
        
        return jsonify({"message": "Record created successfully", "id": new_id}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@lowTensionApi.route('/<int:id>', methods=['PUT'])
def update_lt_record(id):
    data = request.json
    updateable_fields = ['date', 'area', 'units_assessed', 'amount_assessed', 'amount_realized']
    
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
            UPDATE low_tension_power_theft 
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

@lowTensionApi.route('/<int:id>', methods=['DELETE'])
def delete_lt_record(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("DELETE FROM low_tension_power_theft WHERE id = %s", (id,))
        conn.commit()
        
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        
        if affected_rows > 0:
            return jsonify({"message": "Record deleted successfully"}), 200
        return jsonify({"error": "Record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@lowTensionApi.route('/stats', methods=['GET'])
def get_lt_stats():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Get summary statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_cases,
                SUM(units_assessed) as total_units_assessed,
                AVG(amount_assessed) as avg_amount_assessed,
                SUM(amount_realized) as total_amount_realized,
                COUNT(DISTINCT area) as total_areas
            FROM low_tension_power_theft
        """)
        stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify(stats), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@lowTensionApi.route('/by-area', methods=['GET'])
def get_by_area():
    area = request.args.get('area')
    
    if not area:
        return jsonify({"error": "area parameter is required"}), 400
        
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT * FROM low_tension_power_theft 
            WHERE area = %s
        """, (area,))
        
        area_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify(area_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@lowTensionApi.route('/monthly-trend', methods=['GET'])
def get_monthly_trend():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT 
                DATE_FORMAT(date, '%Y-%m') as month,
                COUNT(*) as cases,
                SUM(units_assessed) as units_assessed,
                SUM(amount_assessed) as amount_assessed,
                SUM(amount_realized) as amount_realized
            FROM low_tension_power_theft
            GROUP BY DATE_FORMAT(date, '%Y-%m')
            ORDER BY month
        """)
        
        trend_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify(trend_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
