from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from confluent_kafka import Producer
from datetime import timedelta

### Plugin consts
IS_DEBUG = conf.getboolean('core', 'is_debug')
PLUGIN_NAME = u'kafka_plugin'

### Variables
PRODUCER_PROPS_VAR = 'producer_props'
PRODUCER_FLUSH_TIMEOUT_SEC_VAR = 'producer_flush_timeout_sec'
PRODUCER_BOOTSTRAP_SERVERS_VAR = 'bootstrap_servers'

### Default producer consts
DEFAULT_PRODUCER_FLUSH_TIMEOUT_SEC = 10.0
DEFAULT_PRODUCER_PROPS = {
    "acks": "all",
    "compression.type": "none",
    "request.timeout.ms": "10000",
    "bootstrap.servers": ""
}

class ProduceKafkaMessageOperator(BaseOperator):
    template_fields = ('props', 'topic', 'bootstrap_servers', 'key', 'value',)

    @apply_defaults
    def __init__(self,
                 topic,
                 key,
                 value,
                 bootstrap_servers=None,
                 props=None,
                 retries=5,
                 retry_exponential_backoff=True,
                 retry_delay=timedelta(minutes=1),
                 max_retry_delay=timedelta(minutes=10),
                 *args,
                 **kwargs):

        self.props = props
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.key = key
        self.value = value

        super(ProduceKafkaMessageOperator, self).__init__(
            retries=retries,
            retry_exponential_backoff=retry_exponential_backoff,
            retry_delay=retry_delay,
            max_retry_delay=max_retry_delay,
            *args, **kwargs)

    def execute(self, context):
        # Debug defaults
        default_bootstrap_servers = conf.get('core', 'LOCALHOST_IP') + ":" + str(9092) if IS_DEBUG else self.bootstrap_servers
        default_props = DEFAULT_PRODUCER_PROPS if IS_DEBUG else self.props
        default_flush_timeout_sec = DEFAULT_PRODUCER_FLUSH_TIMEOUT_SEC if IS_DEBUG else None

        # Deal with producer props
        if self.props is None:
            self.props = Variable.get(PRODUCER_PROPS_VAR, deserialize_json=True, default_var=default_props)

        # Deal with bootstrap servers
        if self.bootstrap_servers is None:
            self.bootstrap_servers = Variable.get(PRODUCER_BOOTSTRAP_SERVERS_VAR, default_var=default_bootstrap_servers)
        self.props['bootstrap.servers'] = self.bootstrap_servers

        # Verify all params exist
        if (self.bootstrap_servers is None
                or self.props is None
                or self.topic is None
                or self.key is None
                or self.value is None):
            raise AirflowException(
                "ProduceKafkaMessageOperator missing params: bootstrap_servers={}, topic={}, key={}, value={}, props={}".format(
                    self.bootstrap_servers, self.topic, self.key, self.value, self.props))

        self.log.info("ProduceKafkaMessageOperator creating producer for: props={}".format(self.props))
        producer = Producer(self.props)

        # Did not find any sync api in confluent kafka, and I'd like to fail the DAG
        state = ProducerStatefulCallback()

        # Producing message to topic
        producer.produce(topic=self.topic, key=self.key, value=self.value, on_delivery=state.from_callback)

        # Waiting for producer to finish sending messages
        flush_timeout = float(Variable.get(PRODUCER_FLUSH_TIMEOUT_SEC_VAR, default_var=default_flush_timeout_sec))
        queue = producer.flush(flush_timeout)

        # Maybe a chance for race condition for callback to be called after flush timeout (nothing in docs)
        if state.error or queue > 0:
            raise AirflowException(
                "ProduceKafkaMessageOperator failed to produce message on time: topic={}, key={}, value={}, queue={}, error={}, props={}".format(
                    self.topic, self.key, self.value, queue, state.get_err(), self.props))

        self.log.info(
            "ProduceKafkaMessageOperator produced message: topic={}, key={}, value={}, queue={}, error={}, props={}".format(
                self.topic, self.key, self.value, queue, state.get_err(), self.props))

class ProducerStatefulCallback:
    def __init__(self):
        self.error = None
        self.message = None

    def from_callback(self, err, msg):
        self.error = err
        self.message = msg

    def get_err(self):
        return self.error

class KafkaProducerPlugin(AirflowPlugin):
    name = PLUGIN_NAME
    operators = [ProduceKafkaMessageOperator]
