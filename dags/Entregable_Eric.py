from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta


defaul_args = {
    "owner": "Eric",
    "start_date": datetime(2023, 7, 22),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    'catchup': False,
}

#Parametros del DAG

with DAG(
    dag_id="Entregable_Eric",
    default_args=defaul_args,
    description="Entregable3_Eric",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tareas
    Extraer_info = SparkSubmitOperator(
        task_id="Extraer_info",
        application=f'{Variable.get("spark_scripts_dir")}/Extraer_info.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    Transformar_data = SparkSubmitOperator(
        task_id="Transformar_data",
        application=f'{Variable.get("spark_scripts_dir")}/Transformar_info.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    crear_tabla = SparkSubmitOperator(
        task_id="Crea_tabla",
        application=f'{Variable.get("spark_scripts_dir")}/Crear_tabla.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    Enviar_Redshift = SparkSubmitOperator(
        task_id="Enviar_Redshift",
        application=f'{Variable.get("spark_scripts_dir")}/Cargar_info.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    Extraer_info  >> Transformar_data >> crear_tabla >> Enviar_Redshift
