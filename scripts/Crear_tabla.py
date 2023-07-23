from os import environ as env
import psycopg2


def crear_tablaNBA():
    # Genero la conexion con pycopg2
    conn = psycopg2.connect(
        host=env["REDSHIFT_HOST"],
        port=env["REDSHIFT_PORT"],
        dbname=env["REDSHIFT_DB"],
        user=env["REDSHIFT_USER"],
        password=env["REDSHIFT_PASSWORD"]
    )

    # Corro la Query de SQL que genera la tabla
    cursor = conn.cursor()
    cursor.execute(f"""
        create table if not exists {env["REDSHIFT_USER"]}.NBAstat3 (
        PLAYER_ID float distkey, 
        RANK float, PLAYER varchar,
        TEAM_ID float ,
        TEAM varchar,
        GP float,
        REB float,
        AST float,
        STL float,
        BLK float,
        TEAM_PLAYERS float,
        SUM_GP_TEAM float
        ) sortkey(TEAM);
        """)
    conn.commit()
    cursor.close()

crear_tablaNBA()
