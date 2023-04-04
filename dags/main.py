from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
import datetime
import pandas as pd, glob, os, airflow.utils.dates, sys
from pathlib import Path
dag = DAG(
    dag_id="student_project_1",
    start_date=airflow.utils.dates.days_ago(7),
    schedule_interval= None,
    description="Student Project IT Product Catalogue",
)
path    = "/home/airflow/data"
dateTime = datetime.datetime.now()     
def buat_file():
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    data1 = {
            "sku": ["TM1", "TM2", "TM3", "TM4", "TM5", "TM6"],
            "name": ["Digimax", "MaxStream", "IndieHome", "Halo", "UltraData", "MaxData"],
            "stock": [15, 13, -13, 25, 12, 17],
    }
    data2 = {
            "sku": ["TM1", "TM2", "TM3", "TM4", "TM5", "TM6"],
            "name": ["Digimax", "MaxStream", "IndieHome", "Halo", "UltraData", "MaxData"],
            "stock": [13, 15, 14, 11, 8, 15],
    }
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    df1.to_csv(str(path+'/product_'+dateTime.strftime("%Y%m%d%H%M")+'_1.csv'), index=False)
    df2.to_csv(str(path+'/product_'+dateTime.strftime("%Y%m%d%H%M")+'_2.csv'), index=False)

    print("Memulai membuat file csv")

def proses_data():
    allFiles = glob.glob(str(path+"/*.csv"), recursive=True)

    for files in allFiles:
        # Open Postgres Connection
        conn = PostgresHook(postgres_conn_id='postgre_airflow').get_conn()
        cursor = conn.cursor()
        with open(files, 'r') as f:
            df = pd.read_csv(f)
            print("Proses input data dari csv")
            for index, row in df.iterrows():                
                cursor.execute('select count(*) from product where sku = %s', (row['sku'],))
                result = cursor.fetchone()

                for res in result:
                    print(res)
                print("Total number of rows on sku "+row['sku']+" : ", res)
                
                # sys.exit()
                if res < 1:
                    params = (row['sku'], row['name'], row['stock'])

                    cursor.execute(
                        "INSERT INTO product (sku, name, stock) VALUES (%s, %s, %s)", params
                    )
                else:
                    params = (row['stock'], row['sku'])

                    cursor.execute(
                        "UPDATE product set stock = %s where sku = %s", params
                    )            
            conn.commit()
            # cursor.copy_from(f, 'product', sep=',')
            # conn.commit()
            # cursor.close()
def hapus_file():
    allFiles = glob.glob(str(path+"/*.csv"), recursive=True)
    for files in allFiles:
        os.remove(files)
        print("Deleting File. . .")

buatTable = PostgresOperator(
    task_id = 'buat_table',
    postgres_conn_id = 'postgre_airflow',
    sql = '''
         create table if not exists product(
            id SERIAL PRIMARY KEY,
            sku VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            stock INTEGER NOT NULL not null default 0
        );
    ''',
    dag = dag
)

buatFile = PythonOperator(
    task_id='buat_file',
    python_callable = buat_file,
    dag=dag
)

process = PythonOperator(
    task_id='process',
    python_callable = proses_data,
    dag=dag
)

hapusFile = PythonOperator(
    task_id='hapus_file',
    python_callable = hapus_file,
    dag=dag
)

done = BashOperator(
    task_id = 'done',
    bash_command = 'echo "Selesai"',
    dag = dag
)

buatTable >> buatFile >> process >> hapusFile >> done