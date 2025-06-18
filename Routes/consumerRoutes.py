from flask import Blueprint, jsonify
import mysql.connector
import os
from dotenv import load_dotenv

consumerApi = Blueprint('consumer', __name__)
load_dotenv()

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': 'guvnl_consumers',
}

@consumerApi.route('/by-dtr/<string:dtr_id>', methods=['GET'])
def get_consumers_by_dtr(dtr_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT ConsumerID as consumer_id, Name as name, Consumer_type as type, Address as address, District as district, DTR_id as dtr_id, PinCode as pincode
            FROM consumers_details
            WHERE DTR_id = %s
        """, (dtr_id,))
        consumers = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(consumers), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
