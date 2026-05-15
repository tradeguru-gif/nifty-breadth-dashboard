import os
import time
import threading
import logging
import requests
import pandas as pd
from collections import deque
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS

# UPDATED IMPORT: Direct import to avoid naming conflicts
import dhanhq 

# ------------------------------------------------------------
# Logging & Flask Setup
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
application = app 

# ------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("CRITICAL: Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN")

# UPDATED INITIALIZATION: Explicitly calling the class from the module
dhan = dhanhq.dhanhq(CLIENT_ID, ACCESS_TOKEN)
logger.info("✅ Dhan client initialized successfully")