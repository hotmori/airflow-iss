from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
import requests

def finalize():
    print("All is done")

def get_iss_data(iss_data):
    url = 'http://api.open-notify.org/iss-now.json'
    x = requests.get(url)
    status_code = x.status_code
    print("status_code: ",status_code)
    if status_code != 200:
        raise ValueError('ISS API connection error status code:', status_code)
    
    
    json_data = x.json()
    message = json_data["message"]
  

    #example: {'message': 'success', 'iss_position': {'latitude': '10.4216', 'longitude': '57.4463'}, 'timestamp': 1679483851}

    print("json_data: ",json_data)
    if message != "success":
        raise ValueError('ISS API response error message:', message)
    
    iss_position = json_data["iss_position"]
    latitude = json_data["iss_position"]["latitude"]
    longitude = json_data["iss_position"]["longitude"]
    timestamp = json_data["timestamp"]
    result = {"timestamp":timestamp, "latitude": latitude, "longitude": longitude, "message": message}
    iss_data.xcom_push(key='iss_data', value=result) 

    return 
    print("iss_position: ", iss_position)
    print("timestamp: ", timestamp)
    print("latitude: ", latitude)
    print("longitude: ", longitude)
    


with DAG(dag_id="iss_data_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="30 * * * *",
         catchup=False) as dag:
    
    task_get_iss_data = PythonOperator(
        task_id="get_iss_data",
        python_callable=get_iss_data)
    
    task_connect_postgres_db = PostgresOperator(task_id = "connect_postgres_db",
                                                postgres_conn_id="postgres_default",
                                                #postgress_conn_id = "postgres_default",
                                                sql = "SELECT 1 x;")
    
    task_save_data = PostgresOperator(task_id = "save_data_postgres_db",
                                      postgres_conn_id="postgres_default",
                                      sql = """insert into iss_positions(ts, longitude, latitude,message) \
                                               values(to_timestamp('{{ params.ux_timestamp }}'), 57.4463, 10.4216, 'success' );
                                            """,
                                      params = {'ux_timestamp':1679483851},
                                      autocommit = True)

    task_finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize)

task_get_iss_data >> task_connect_postgres_db >>  task_save_data >> task_finalize
