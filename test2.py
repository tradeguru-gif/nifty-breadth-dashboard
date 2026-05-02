from flask import Flaskapp = Flask(_name_)@app.route('/')def home():    return "OK"if _name_ == '_main_':    app.run(port=5000)



python test2.py
