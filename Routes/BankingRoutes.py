from flask import Blueprint, jsonify, request
import mysql.connector
import datetime

bankingAPI = Blueprint('banking', __name__)

db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'guvnl_dev'
}


@bankingAPI.route('/', methods=['GET'])
def get_consumer_data():
    # 1) grab from args OR JSON body
    args = request.args or {}
    body = request.get_json(silent=True) or {}
    raw_start = args.get('start_date') or body.get('start_date')
    raw_end = args.get('end_date') or body.get('end_date')

    # helper: turn "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" into "YYYY-MM-DD"
    def normalize_date_str(s):
        if not s:
            return None
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                dt = datetime.datetime.strptime(s, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    start_date = normalize_date_str(raw_start)
    end_date = normalize_date_str(raw_end)

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 2) apply filter only if both parsed dates present
        if start_date and end_date:
            cursor.execute(
                "SELECT * FROM Banking_Data WHERE Date BETWEEN %s AND %s",
                (start_date, end_date)
            )
        else:
            cursor.execute("SELECT * FROM Banking_Data")

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # 3) for each row, parse strings to date/time then combine
        def parse_date(d):
            if isinstance(d, str):
                for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
                    try:
                        return datetime.datetime.strptime(d, fmt).date()
                    except ValueError:
                        continue
                return None
            return d  # assume it's already a date

        def parse_time(t):
            if isinstance(t, str):
                try:
                    return datetime.datetime.strptime(t, '%H:%M:%S').time()
                except ValueError:
                    return None
            return t  # assume it's already a time

        for row in rows:
            d_obj = parse_date(row.get('Date'))
            st_obj = parse_time(row.get('Start_Time'))
            et_obj = parse_time(row.get('End_Time'))

            if d_obj and st_obj:
                dt_start = datetime.datetime.combine(d_obj, st_obj)
                row['Start_DateTime'] = dt_start.strftime('%Y-%m-%d %H:%M:%S')
            else:
                row['Start_DateTime'] = None

            if d_obj and et_obj:
                dt_end = datetime.datetime.combine(d_obj, et_obj)
                row['End_DateTime'] = dt_end.strftime('%Y-%m-%d %H:%M:%S')
            else:
                row['End_DateTime'] = None

        return jsonify(rows), 200

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
