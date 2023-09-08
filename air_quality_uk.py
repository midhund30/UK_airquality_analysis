import pandas as pd
from uuid import uuid4
from datetime import *
import psycopg2
from sqlalchemy import create_engine
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

#STEP_1: API call

def api_call():

    waqi_url = "https://api.waqi.info"
    api_key = '9977e43376bcfc3b91b25e3fd3b93e8806a90e7e'
    uk_latlon ="49.628854,-11.267577,59.376053,2.948731"
    data_url=f"/map/bounds/?latlng={uk_latlon}&token={api_key}"
    aq_data = pd.read_json(waqi_url + data_url)
    print('columns->', aq_data.columns)
    data = []
    for i in aq_data['data']:
        data.append([i['station']['name'],i['station']['time'],i['lat'],i['lon'],i['aqi']])
        air_df = pd.DataFrame(data, columns=['station_name','time', 'lat', 'lon', 'aqi'])
    print(air_df.info())
    return air_df

#STEP_2:Data cleaning

def data_cleaning(air_df):

    air_df["country"] = air_df.station_name.str.split(',',expand = True)[2]
    air_df["country"] = air_df["country"].fillna(air_df.station_name.str.split(',',expand = True)[1])
    air_df['station_name'].replace(",.*", "", regex=True, inplace=True)
    air_df[["date",'time']] = air_df.time.str.split('T',expand = True)
    air_df["time"] = air_df.time.str.split('+',expand = True)[0]
    air_df = air_df.dropna(subset = ['aqi'])

    only_uk = (air_df['country'].str.strip() == 'United Kingdom')
    uk_df = air_df[only_uk]
    uk_df['aqi'] = uk_df.aqi.str.replace('-','0')
    uk_df = uk_df.drop_duplicates()

    uk_df['air_id'] = uk_df.index.map(lambda _:'AQ'+ uuid4().hex[:10])
    uk_df = uk_df[['air_id','station_name', 'date', 'time','country', 'lat','lon','aqi']]
    print(uk_df.info())
    return uk_df

#STEP_3: Daily update checks

def daily_check(uk_df):

    uk_df["date"] = pd.to_datetime(uk_df["date"])
    daily_df = uk_df[uk_df['date'].dt.strftime('%Y-%m-%d')==str(date.today())]
    print(daily_df.info())
    return daily_df

#STEP_4: Creating a table for data

def table_creation():

    conn_details = psycopg2.connect(
        host="data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com",
        database="pagila",
        user="de8_mira43",
        password="LMplx56)",
        port= '5432')
    
    cursor = conn_details.cursor()
    table_creation = '''
        CREATE TABLE student.mrrd_air_uk (
        air_id varchar(12) PRIMARY KEY,
        station_name varchar(80),
        date date,
        time time,
        country varchar(40),
        lat float,
        lon float,
        aqi int)
    '''
    cursor.execute(table_creation)
    conn_details.commit()
    cursor.close()
    conn_details.close()
    

#STEP_5: Data loading to database

def data_loading(daily_df):

    conn_string = 'postgresql://de8_mira43:LMplx56)@data-sandbox.c1tykfvfhpit.eu-west-2.rds.amazonaws.com:5432/pagila'
    db = create_engine(conn_string)
    conn = db.connect()
    daily_df.to_sql('mrrd_air_uk', con=conn, schema ='student',if_exists='append', index=False)


#Pipeline for the ETL Process

def pipeline_air():
    try:
        table_creation()
    except:
        print('Table pre-defined')
    air_uk = api_call()
    air_uk = data_cleaning(air_uk)
    air_uk = daily_check(air_uk)
    data_loading(air_uk)


#Automating and scheduling using Apache Airflow

args = {
 
    'owner': 'midhun',
    'start_date': days_ago(1)
}
 
 
args = {
 
    'owner': 'midhun',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'catchup_by_default': False,
}
 
 
dag = DAG(dag_id = 'air_quality_pipeline', default_args=args, schedule_interval='@daily', catchup =False)

air_datapipeline = PythonOperator(
    task_id="air_datapipeline",
    provide_context=True,
    python_callable=pipeline_air,
    dag=dag,
)

air_datapipeline
