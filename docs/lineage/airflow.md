# Airflow Integration

DataHub supports integration of 
- Airflow Pipeline (DAG) metadata
- DAG and Task run information as well as 
- Lineage information when present

There are a few ways to enable these integrations from Airflow into DataHub.


## Using Datahub's Airflow lineage backend (recommended)

:::caution

The Airflow lineage backend is only supported in Airflow 1.10.15+ and 2.0.2+.

:::

:::note

If you are looking to run Airflow and DataHub using docker locally, follow the guide [here](../../docker/airflow/local_airflow.md). Otherwise proceed to follow the instructions below.
:::

## Setting up Airflow to use DataHub as Lineage Backend

1. You need to install the required dependency in your airflow. See https://registry.astronomer.io/providers/datahub/modules/datahublineagebackend

```shell
  pip install acryl-datahub[airflow]
```

2. You must configure an Airflow hook for Datahub. We support both a Datahub REST hook and a Kafka-based hook, but you only need one.

   ```shell
   # For REST-based:
   airflow connections add  --conn-type 'datahub_rest' 'datahub_rest_default' --conn-host 'http://localhost:8080'
   # For Kafka-based (standard Kafka sink config can be passed via extras):
   airflow connections add  --conn-type 'datahub_kafka' 'datahub_kafka_default' --conn-host 'broker:9092' --conn-extra '{}'
   ```

3. Add the following lines to your `airflow.cfg` file.
   ```ini
   [lineage]
   backend = datahub_provider.lineage.datahub.DatahubLineageBackend
   datahub_kwargs = {
       "datahub_conn_id": "datahub_rest_default",
       "cluster": "prod",
       "capture_ownership_info": true,
       "capture_tags_info": true,
       "graceful_exceptions": true }
   # The above indentation is important!
   ```
   **Configuration options:**
   - `datahub_conn_id` (required): Usually `datahub_rest_default` or `datahub_kafka_default`, depending on what you named the connection in step 1.
   - `cluster` (defaults to "prod"): The "cluster" to associate Airflow DAGs and tasks with.
   - `capture_ownership_info` (defaults to true): If true, the owners field of the DAG will be capture as a DataHub corpuser.
   - `capture_tags_info` (defaults to true): If true, the tags field of the DAG will be captured as DataHub tags.
   - `capture_executions` (defaults to false): If true, it captures task runs as DataHub DataProcessInstances. **This feature only works with Datahub GMS version v0.8.33 or greater.**
   - `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall task to fail. Note that configuration issues will still throw exceptions.
4. Configure `inlets` and `outlets` for your Airflow operators. For reference, look at the sample DAG in [`lineage_backend_demo.py`](../../metadata-ingestion/src/datahub_provider/example_dags/lineage_backend_demo.py), or reference [`lineage_backend_taskflow_demo.py`](../../metadata-ingestion/src/datahub_provider/example_dags/lineage_backend_taskflow_demo.py) if you're using the [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html).
5. [optional] Learn more about [Airflow lineage](https://airflow.apache.org/docs/apache-airflow/stable/lineage.html), including shorthand notation and some automation.

## Emitting lineage via a separate operator

Take a look at this sample DAG:

- [`lineage_emission_dag.py`](../../metadata-ingestion/src/datahub_provider/example_dags/lineage_emission_dag.py) - emits lineage using the DatahubEmitterOperator.

In order to use this example, you must first configure the Datahub hook. Like in ingestion, we support a Datahub REST hook and a Kafka-based hook. See step 1 above for details.
