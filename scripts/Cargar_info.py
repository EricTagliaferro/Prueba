import requests
from os import environ as env
from commons import ETL_Spark

#Defino instacia para traer la conexion de SPARK
inst = ETL_Spark()

def carga():
    #Traigo la informacion generada en la tarea "Transformar_info" 
    Top20 = inst.spark.read.format("csv").option("header",True).option("inferSchema",True).option("delimiter", ",").load("/opt/airflow/Data/Data_filtered.csv")
    Top20.show()

    #Cargo la informacion en Redshift en la DB, usuario y pass que se encuentra en el archivo .env. Todo dentro de la Tabla nbastat3    
    Top20.write \
        .format("jdbc") \
        .option("url", env['REDSHIFT_URL']) \
        .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.nbastat3") \
        .option("user", env['REDSHIFT_USER']) \
        .option("password", env['REDSHIFT_PASSWORD']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
        
carga()