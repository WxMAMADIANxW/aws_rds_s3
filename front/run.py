from flask import (Flask, request, render_template)
import mysql.connector
from decimal import Decimal
import json
import boto3
from boto3.dynamodb.conditions import Key
from requests_html import HTMLSession

app = Flask(__name__)


@app.route("/")
def welcome():
    return render_template("index.html")


@app.route("/s3_to_rds", methods=['GET','POST'])
def s3_to_rds():
    session = HTMLSession()
    response = session.get("http://13.38.117.219:3000/s3_to_rds")
    return render_template(response.html)


@app.route("/rds_to_s3", methods=['GET','POST'])
def rds_to_s3():
    session = HTMLSession()
    response = session.get("http://13.38.117.219:3000/rds_to_s3")
    return render_template(response.html)


if __name__ == "__main__":
    app.run(port=3000, host="0.0.0.0")
