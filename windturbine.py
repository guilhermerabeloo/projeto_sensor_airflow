from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os


def getDataFile(**kwargs):
    try:
        with open(Variable.get('path_file')) as file:
            data = json.load(file)
            kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
            kwargs['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
            kwargs['ti'].xcom_push(key='hydraulicpressure', value=data['hydraulicpressure'])
            kwargs['ti'].xcom_push(key='temperature', value=data['temperature'])
            kwargs['ti'].xcom_push(key='timeregister', value=data['timeregister'])
        os.remove(Variable.get('path_file'))
    except Exception as err:
        raise Exception(err)

def defineTipoAlerta(**kwargs):
    try:
        temperatura = float(kwargs['ti'].xcom_pull(task_ids='tsk_get_data_file', key='temperature'))

        if temperatura >= 24:
            return 'group_check_temperatura.tsk_send_email_alert'
        else:
            return 'group_check_temperatura.tsk_send_email_normal'
    except Exception as err:
        raise Exception(err)

default_args = {
    'depends_on_past': False,
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'delay': timedelta(seconds=10)
}

with DAG('wind_turbine',
         description='Dados da turbina',
         schedule_interval=None,
         start_date=datetime(2024,10,31),
         default_args=default_args,
         default_view='graph',
         doc_md="## Dag para registrar dados de turbina eolica",
         catchup=False) as dag:

    group_check_temperatura = TaskGroup('group_check_temperatura')
    group_database = TaskGroup('group_database')

    task_file_sensor = FileSensor(
        task_id='tsk_file_sensor',
        filepath=Variable.get('path_file'),
        fs_conn_id='fs_default',
        poke_interval=10    
    )
    
    task_get_data_file = PythonOperator(
        task_id='tsk_get_data_file',
        python_callable=getDataFile,
        provide_context=True
    )

    task_create_table = PostgresOperator(
        task_id='tsk_create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS sensors(
                idtemp varchar,
                powerfactor varchar,
                hydraulicpressure varchar,
                temperature varchar,
                timeregister timestamp
            );
        ''',
        task_group=group_database
    )

    task_insert_data = PostgresOperator(
        task_id='tsk_insert_data',
        postgres_conn_id='postgres',
        sql='''
            INSERT INTO sensors (idtemp, powerfactor, hydraulicpressure, temperature, timeregister)
            VALUES (
                '{{ task_instance.xcom_pull(task_ids="tsk_get_data_file", key="idtemp") }}',
                '{{ task_instance.xcom_pull(task_ids="tsk_get_data_file", key="powerfactor") }}',
                '{{ task_instance.xcom_pull(task_ids="tsk_get_data_file", key="hydraulicpressure") }}',
                '{{ task_instance.xcom_pull(task_ids="tsk_get_data_file", key="temperature") }}',
                '{{ task_instance.xcom_pull(task_ids="tsk_get_data_file", key="timeregister") }}'
            )
        ''',
        task_group=group_database
    )


    task_send_email_alert = EmailOperator(
        task_id="tsk_send_email_alert",
        to='guilhermerabelo699@gmail.com',
        subject='WindTurbine: Temperature alert!',
        html_content='''
            <h2> Email alert </h2>
            <p> This is an alert email for high temperature detection. </p>
        ''',
        task_group=group_check_temperatura
    )

    task_send_email_normal = EmailOperator(
        task_id="tsk_send_email_normal",
        to='guilhermerabelo699@gmail.com',
        subject='WindTurbine: Temperature normal',
        html_content='''
            <h2> Email info </h2>
            <p> This is an informational email about normal temperature. </p>
        ''',
        task_group=group_check_temperatura
    )

    task_branch_email = BranchPythonOperator(
        task_id='tsk_branch_email',
        python_callable=defineTipoAlerta,
        provide_context=True,
        task_group=group_check_temperatura
    )

with group_check_temperatura:
    task_branch_email >> [task_send_email_alert, task_send_email_normal]

with group_database:
    task_create_table >> task_insert_data

task_file_sensor >> task_get_data_file 
task_get_data_file >> group_check_temperatura 
task_get_data_file >> group_database


