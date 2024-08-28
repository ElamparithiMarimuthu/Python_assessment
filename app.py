from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

def connect():
    con = sqlite3.connect("assessment.db")
    tool = con.cursor()
    return con, tool

@app.route("/tables", methods=['GET'])
def list_tables():
    con, tool = connect()
    tool.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
    tables = tool.fetchall()
    con.close()

    table_names = [i[0] for i in tables]

    return jsonify({"tables": table_names})

@app.route("/tables/<string:tablename>", methods=['GET'])
def list_table(tablename):
    con, tool = connect()
    
    tool.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tablename,))
    table_exists = tool.fetchone()
    
    if table_exists:
        tool.execute(f"SELECT * FROM {tablename}")
        rows = tool.fetchall()
        tool.execute(f"PRAGMA table_info({tablename})")
        columns = [col[1] for col in tool.fetchall()]
        con.close()
        result = [dict(zip(columns, row)) for row in rows]  

        return jsonify({tablename: result})
    else:
        con.close()
        return jsonify({"error": f"Table '{tablename}' not found"})

@app.route("/tables/<string:tablename>", methods=["POST"]) 
def create_content(tablename):
    con, tool = connect()
    tool.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tablename,))
    table_exists = tool.fetchone()

    if not table_exists:
        con.close()
        return jsonify({"error": f"Table '{tablename}' not found"})
    
    data = request.get_json()

    if not data:
        con.close()
        return jsonify({"error": "No data provided"})
    
    tool.execute(f"PRAGMA table_info({tablename})")
    columns = [col[1] for col in tool.fetchall()] 

    invalid_columns = [key for key in data.keys() if key not in columns]
    if invalid_columns:
        con.close()
        return jsonify({"error": f"Invalid columns: {', '.join(invalid_columns)}"})
    
    keys = ', '.join(data.keys())
    question_marks = ', '.join(['?'] * len(data))
    values = tuple(data.values())

    sql = f"INSERT INTO {tablename} ({keys}) VALUES ({question_marks})"
    
    try:
        tool.execute(sql, values)
        con.commit()
    except sqlite3.Error as e:
        con.close()
        return jsonify({"error": str(e)})

    con.close()
    
    return jsonify({"message": "Data inserted successfully"})

@app.route("/tables/<string:tablename>/<int:record_id>", methods=["PUT"])
def update_record(tablename, record_id):
    con, tool = connect()
    tool.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tablename,))
    table_exists = tool.fetchone()

    if not table_exists:
        con.close()
        return jsonify({"error": f"Table '{tablename}' not found"})

    data = request.get_json()
    
    if not data:
        con.close()
        return jsonify({"error": "No data provided"})
    
    tool.execute(f"PRAGMA table_info({tablename})")
    columns = [col[1] for col in tool.fetchall()]

    invalid_columns = [key for key in data.keys() if key not in columns]
    if invalid_columns:
        con.close()
        return jsonify({"error": f"Invalid columns: {', '.join(invalid_columns)}"})
    
    set_clause = ', '.join([f"{key} = ?" for key in data.keys()])
    values = tuple(data.values()) + (record_id,)
    
    sql = f"UPDATE {tablename} SET {set_clause} WHERE id = ?"
    
    try:
        tool.execute(sql, values)
        con.commit()
    except sqlite3.Error as e:
        con.close()
        return jsonify({"error": str(e)})

    con.close()
    
    return jsonify({"message": "Record updated successfully"})

@app.route("/tables/<string:tablename>/<int:record_id>", methods=["DELETE"])
def delete_record(tablename, record_id):
    con, tool = connect()
    tool.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tablename,))
    table_exists = tool.fetchone()

    if not table_exists:
        con.close()
        return jsonify({"error": f"Table '{tablename}' not found"})

    sql = f"DELETE FROM {tablename} WHERE id = ?"
    
    try:
        tool.execute(sql, (record_id,))
        con.commit()
    except sqlite3.Error as e:
        con.close()
        return jsonify({"error": str(e)})

    con.close()
    
    return jsonify({"message": "Record deleted successfully"})

if __name__ == "__main__":
    app.run(debug=True)
