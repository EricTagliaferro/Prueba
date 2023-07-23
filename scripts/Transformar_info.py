from os import environ as env
from email import message
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import regexp_replace,col
import smtplib
from airflow.models import Variable
from commons import ETL_Spark

inst = ETL_Spark()

def transformar():
    #Importo el csv generado en la tarea extraer_info
    Tabla = inst.spark.read.format("csv").option("header",True).option("inferSchema",True).option("delimiter", ",").load("/opt/airflow/Data/Data_raw.csv")
        
    #Hago un cheack de duplicados
    Tabla.count()

    TablacheckC = Tabla
    TablacheckC.dropDuplicates(['PLAYER'])
    TablacheckC.count()
         
    #Se envia un email notificando de los duplicados o no generados
    if Tabla.count() ==  TablacheckC.count():
        Respuesta = 'No hay duplicados'

    else:
        Duplicados = Tabla.count() - TablacheckC.count()
        Respuesta = f"Se removieron {Duplicados} duplicados"

    x=smtplib.SMTP('smtp.gmail.com',587)
    x.starttls()#
    # se envia email usando las variables cargadas en Airflow en funcion de lo aclarado ene l reade de Github
    x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))
    subject='Check de duplicados'
    body_text = Respuesta
    message='Subject: {}\n\n{}'.format(subject,body_text)
    x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'),message)
    print('Enviado')

    #Genero 2 variables nuevas a la tabla
    w = Window.partitionBy('TEAM')
    Tabla = Tabla.withColumn('TEAM_PLAYERS', F.count('TEAM').over(w))
    Tabla = Tabla.withColumn('SUM_GP_TEAM', F.sum('GP').over(w))

    #Saco apostrofes de los nombres para que no genere conflicto
    Tabla = Tabla.withColumn('PLAYER', regexp_replace('PLAYER',"'",""))

    #Selecciono columnas y los primeros 20 registros
    Tablaresumen = Tabla.select('PLAYER_ID', 'RANK','PLAYER', 'TEAM_ID', 'TEAM', 'GP', 'REB', 'AST', 'STL', 'BLK','TEAM_PLAYERS','SUM_GP_TEAM')
    Tablaresumen = Tablaresumen.sort("RANK")
    Top20 = Tablaresumen[Tablaresumen["RANK"]<20]
    Top20.show()

    # se guarda la tabla generada en un csv para su posterior carga en Redshift
    Top20.write.mode('overwrite').option("header",True).csv("/opt/airflow/Data/Data_filtered.csv")

transformar()
