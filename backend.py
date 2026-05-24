import os
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return jsonify({"status": "online", "message": "Test successful"})

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "env_vars_set": bool(os.getenv("ANGEL_API_KEY"))})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)