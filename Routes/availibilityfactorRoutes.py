from flask import Blueprint, jsonify, request
import mysql.connector
import traceback
from flask import current_app as app

# Blueprint setup
availabilityAPI = Blueprint('availability', __name__)

# Database configuration
db_config = {
    'user': 'DB-Admin',
    'password': 'DBTest@123',
    'host': '69.62.74.149',
    'database': 'guvnldev'
}


def get_db_connection():
    return mysql.connector.connect(**db_config)


@availabilityAPI.route('/', methods=['GET'])
def get_consumer_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM PAF_Details")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@availabilityAPI.route('/', methods=['POST'])
def add_consumer_data():
    try:
        # 1) Parse JSON (and force an error if it isn't JSON)
        data = request.get_json(force=True)
        app.logger.debug("POST /availability/ payload: %s", data)

        # 2) Validate required fields
        required = [
            "Code", "name",
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]
        missing = [col for col in required if col not in data]
        if missing:
            return jsonify({
                "error": "Missing required fields: " + ", ".join(missing)
            }), 400

        # 3) Build a properly quoted INSERT
        cols = ", ".join(f"`{col}`" for col in required)
        placeholders = ", ".join(["%s"] * len(required))
        sql = f"INSERT INTO `PAF_Details` ({cols}) VALUES ({placeholders})"

        values = [data[col] for col in required]

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Record created", "Code": data["Code"]}), 201

    except Exception as e:
        # 4) Print full stack trace to your Flask log
        traceback.print_exc()
        app.logger.error("Error in add_consumer_data: %s", e)
        return jsonify({"error": str(e)}), 500


@availabilityAPI.route('/<string:code>', methods=['PUT'])
def update_consumer_data(code):
    try:
        # force=True will 400 us if the body isn’t valid JSON
        data = request.get_json(force=True)
        app.logger.debug("PUT /availability/%s payload: %s", code, data)

        allowed = ["name",
                   "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        updates = []
        params = []

        for col in allowed:
            if col in data:
                updates.append(f"`{col}` = %s")  # backtick‑quote the column
                params.append(data[col])

        if not updates:
            return jsonify({"error": "No valid fields to update"}), 400

        params.append(code)
        set_clause = ", ".join(updates)
        sql = f"UPDATE `PAF_Details` SET {set_clause} WHERE `Code` = %s"

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params)

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({"error": "Record not found"}), 404

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": "Record updated", "Code": code}), 200

    except Exception as e:
        traceback.print_exc()
        app.logger.error("Error in update_consumer_data: %s", e)
        return jsonify({"error": str(e)}), 500


@availabilityAPI.route('/<string:code>', methods=['DELETE'])
def delete_consumer_data(code):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM PAF_Details WHERE Code = %s", (code,))
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            return jsonify({"error": "Record not found"}), 404
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Record deleted", "Code": code}), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
