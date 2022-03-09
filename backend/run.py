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

class s3:
    def __init__(self):
        self.s3 = boto3.client('s3',
                                   aws_access_key_id='XXXX',
                                   aws_secret_access_key='XXXX')


    def read_data_from_s3(self):
        bucket_name ='myawsbucketmamad'                                            #event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name ='data.csv'                                           #event["Records"][0]["s3"]["object"]["key"]
        resp = self.s3.get_object(Bucket=bucket_name, Key=s3_file_name) 
        object_content = resp['Body'].read() 
        return(object_content)


 

class databse_mysql: 
    
    def connect_to_db(self,host_name, dbname, port, username, password):
        try:
            conn = mysql.connector.connect(host=host_name, db=dbname, user=username, passwd=password, port=port)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            return conn
            
    def create_table(self,curr):
        create_table_command = ("""CREATE TABLE IF NOT EXISTS matieres (
                        Nom VARCHAR(255) PRIMARY KEY,
                        Description TEXT NOT NULL,
                        Nombre_dheure INTEGER,
                )""")

        curr.execute(create_table_command) 

    def insert_into_table(self,curr, Nom, Description, Nombre_dheure):
        insert_into_matiere = ("""INSERT INTO matiere (Nom, Description, Nombre_dheure)
        VALUES(%s,%s,%s);""")
        row_to_insert = (Nom, Description, Nombre_dheure)
        curr.execute(insert_into_matiere, row_to_insert) 
        

    def append_from_df_to_db(self,curr,df):
        for i, row in df.iterrows():
            self.insert_into_table(curr, row['Nom'], row['Description'], row['Nombre_dheure']) 



@app.route("/s3_to_rds", methods=['GET','POST'])
def s3_to_rds():
    s = s3()
    database = databse_mysql()
    host_name = 'trainingxx.cm8hel5isvcq.eu-west-3.rds.amazonaws.com'
    dbname = 'trainingxx'
    port = '3306'
    username = 'rjbatista'
    password = '123azenbvd!'

    data=s.read_data_from_s3()
    conn = database.connect_to_db(host_name, dbname, port, username, password)
    curr = conn.cursor() 
    database.create_table(curr) 
    database.append_from_df_to_db(curr, data)
    conn.commit()
    return render_template("tords.html")

@app.route("/rds_to_s3", methods=['GET','POST'])
def rds_to_s3():
    
    #Connect to de database 
    database = databse_mysql()
    host_name = 'trainingxx.cm8hel5isvcq.eu-west-3.rds.amazonaws.com'
    dbname = 'trainingxx'
    port = '3306'
    username = 'rjbatista'
    password = '123azenbvd!'
    
    #Fectch all the data
    conn = database.connect_to_db(host_name, dbname, port, username, password)
    curr = conn.cursor()  
    
    curr.execute("SELECT * from matiere") 
    rows=curr.fetchall() 
    data=rows.to_csv(index=False)  
    
    #Store de data into S3 bucket
    s = s3()
    bucket_name ='myawsbucketmamad'                                            #event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name ='data.csv'                                           #event["Records"][0]["s3"]["object"]["key"]
    object = s.Object(bucket_name, s3_file_name) # Pas oublier ici de remplacer la clé parce que je ne la connais pas :)
    object.put(Body=data)
    return render_template("tos3.html")

if __name__ == "__main__":
    app.run(port=3000, host="0.0.0.0")
