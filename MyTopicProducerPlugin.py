# Implementation example
from airflow.plugins_manager import AirflowPlugin
from ProduceKafkaMessagePlugin import ProduceKafkaMessageOperator

### Plugin consts
PLUGIN_NAME = 'my_topic_plugin'

### Consts
MY_TOPIC_NAME = 'topictest'


def send_my_topic(dag, **kwargs):
    key = '{{ dag.dag_id }}'

    value = 'hello'

    return ProduceKafkaMessageOperator(dag=dag,
                                  topic=MY_TOPIC_NAME,
                                  key=key,
                                  value=value,
                                  **kwargs)

class MyTopicPlugin(AirflowPlugin):
    name = PLUGIN_NAME
    macros = [send_my_topic]
