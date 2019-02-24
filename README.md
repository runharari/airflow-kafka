# Apache Airflow Kafka Plugin

* Simple plugin for [Apache Airflow](https://airflow.apache.org/) that produces a kafka message.
* The operator was not designed for high performance (creates producer on each run)
* Can use Airflow variables to configure producer props, flush timeout, and bootstrap servers
