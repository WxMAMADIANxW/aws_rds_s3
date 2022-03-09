from crypt import methods
from hashlib import new
from flask import (Flask, request, render_template)
import boto3 
import psycopg2 as ps
import pandas as pd
import psycopg2 as ps
import pandas as pd 
import mysql.connector
from mysql.connector import errorcode



app = Flask(__name__)



@app.route("/s3_to_rds", methods=['GET','POST'])
def s3_to_rds():
    return render_template("tords.html")


@app.route("/rds_to_s3", methods=['GET','POST'])
def rds_to_s3():
    return render_template("tos3.html")



if __name__ == "__main__":
    app.run(port=3000, host="0.0.0.0")