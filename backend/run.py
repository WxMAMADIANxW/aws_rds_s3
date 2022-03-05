from flask import (Flask, request, render_template)
import boto3 
import psycopg2 as ps
import pandas as pd
import psycopg2 as ps
import pandas as pd 
import mysql.connector



app = Flask(__name__)

class s3:
    def __init__(self):
        self.s3 = boto3.client('s3',
                                   aws_access_key_id='AKIAT22H3R5CNPAJKQGA',
                                   aws_secret_access_key='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')


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

        except mysql.connector.OperationalError as e:
            raise e
        else:
            print('Connected!')
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
            insert_into_table(curr, row['Nom'], row['Description'], row['Nombre_dheure']) 

def connect_to_db(self,host_name, dbname, port, username, password):
        try:
            conn = mysql.connector.connect(host=host_name, db=dbname, user=username, passwd=password, port=port)

        except ps.OperationalError as e:
            raise e
        else:
            print('Connected!')
            return conn

@app.route("/s3_to_rds")
def s3_to_rds():
    host_name = 'trainingxx.cm8hel5isvcq.eu-west-3.rds.amazonaws.com'
    dbname = 'trainingxx'
    port = '3306'
    username = 'rjbatista'
    password = '123azenbvd!'

    data=s3.read_data_from_s3()
    conn = databse_mysql.connect_to_db(host_name, dbname, port, username, password)
    curr = conn.cursor() 
    create_table(curr) 
    databse_mysql.append_from_df_to_db(curr, new_vid_df)
    conn.commit()

@app.route("/rds_to_s3")
def rds_to_s3():
    #Connect to de database 
    host_name = 'trainingxx.cm8hel5isvcq.eu-west-3.rds.amazonaws.com'
    dbname = 'trainingxx'
    port = '3306'
    username = 'rjbatista'
    password = '123azenbvd!'
    
    #Fect all the data
    conn = connect_to_db(host_name, dbname, port, username, password)
    curr = conn.cursor()  
    
    curr.execute("SELECT * from matiere") 
    rows=curr.fetchall() 
    data=rows.to_csv(index=False)  
    
    #Store de data into S3 bucket
    #s3 = boto3.client('s3',aws_access_key_id='AKIAT22H3R5CNPAJKQGA',aws_secret_access_key='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')
    bucket_name ='myawsbucketmamad'                                            #event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name ='data.csv'                                           #event["Records"][0]["s3"]["object"]["key"]
    object = s3.Object(bucket_name, s3_file_name) # Pas oublier ici de remplacer la clé parce que je ne la connais pas :)
    object.put(Body=data)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)