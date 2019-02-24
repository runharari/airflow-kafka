import datetime
from airflow.macros.my_topic_plugin import send_my_topic
from airflow.models import DAG

main_args = {
    'owner': 'me',
    'start_date': datetime.datetime(2019, 2, 24, 8, 00),
}

######################################################################################################################
my_dag = DAG(dag_id='MyDagExample',
               default_args=main_args,
               schedule_interval='@hourly')

send_my_topic(dag=my_dag, task_id='doing_some_stuff')
