from flask import (Flask, request, render_template)
import mysql.connector
from decimal import Decimal
import json
import boto3
from boto3.dynamodb.conditions import Key
app = Flask(__name__)


@app.route("/")
def welcome():
    return render_template("index.html")


if __name__ == "__main__":
    app.run(port=3000, host="0.0.0.0")
