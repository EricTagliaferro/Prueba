import requests
from os import environ as env
from email import message
import smtplib
from airflow.models import Variable
from commons import ETL_Spark
import time
from pyspark.sql import SparkSession

#Defino instacia para traer la conexion de SPARK
inst = ETL_Spark()

def extract():
    # Arranco un conteo para ver la duracion del proceso
    start = time.time()

    #Extracion de info de API y creacion de DF
    test_url = "https://stats.nba.com/stats/leagueLeaders?LeagueID=00&PerMode=PerGame&Scope=S&Season=2022-23&SeasonType=Playoffs&StatCategory=PTS"
    r =requests.get(url=test_url).json()
    Columnas = r['resultSet']["headers"]
    Tabla = inst.spark.createDataFrame(data = r['resultSet']["rowSet"],schema=Columnas) 
    Tabla.printSchema()
    Tabla.show()

    # Finalizo el conteo para ver la duracion del proceso
    end =time.time()

    #Envio e-mail en caso de que la duracion del proceso sea mayor a la variable esperada
    Threshold_tiempo = Variable.get('SMTP_THRESHOLD')
    if (((end-start) * 10**3) > float(Threshold_tiempo) ):
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
     # Envio email usando variables
        x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))
        subject='Alerta tiempo extraccion API'
        body_text= f"El tiempo de ejecucion del programa supero el threshold de {Threshold_tiempo}. El tiempo de ejecucion fue de : {end-start} ms."
        message='Subject: {}\n\n{}'.format(subject,body_text)

        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'),message)
        print('Mensaje enviado')
    else:
        print("El proceso se ejecuto en tiempo correcto")

    #Guarda la tabla generada para luego poder transformarla
    Tabla.write.mode('overwrite').option("header",True).csv("/opt/airflow/Data/Data_raw.csv")

extract()
