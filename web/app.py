# web/app.py

import os
from flask import Flask, render_template, request, jsonify
from db.engine import DatabaseEngine

# Ensure Flask knows the templates folder is inside the current directory
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
app = Flask(__name__, template_folder=template_dir)

engine = DatabaseEngine()


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/query", methods=["POST"])
def run_query():
    sql = request.form.get("sql", "").strip()

    if not sql:
        return jsonify({"error": "Empty query"}), 400

    try:
        result = engine.execute(sql)
        return jsonify({"result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    # Run the Flask server
    app.run(debug=True)
