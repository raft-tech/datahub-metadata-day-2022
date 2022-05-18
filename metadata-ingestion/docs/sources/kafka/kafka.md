:::note
Stateful Ingestion is available only when a Platform Instance is assigned to this source.
:::

### Connecting to Confluent Cloud

If using Confluent Cloud you can use a recipe like this. In this `consumer_config.sasl.username` and `consumer_config.sasl.password` are the API credentials that you get (in the Confluent UI) from your cluster -> Data Integration -> API Keys. `schema_registry_config.basic.auth.user.info`  has API credentials for Confluent schema registry which you get (in Confluent UI) from Schema Registry -> API credentials.

When creating API Key for the cluster ensure that the ACLs associated with the key are set like below. This is required for DataHub to read topic metadata from topics in Confluent Cloud.
```
Topic Name = *
Permission = ALLOW
Operation = DESCRIBE
Pattern Type = LITERAL
```

```yml
source:
  type: "kafka"
  config:
    platform_instance: "YOUR_CLUSTER_ID"
    connection:
      bootstrap: "abc-defg.eu-west-1.aws.confluent.cloud:9092"
      consumer_config:
        security.protocol: "SASL_SSL"
        sasl.mechanism: "PLAIN"
        sasl.username: "${CLUSTER_API_KEY_ID}"
        sasl.password: "${CLUSTER_API_KEY_SECRET}"
      schema_registry_url: "https://abc-defgh.us-east-2.aws.confluent.cloud"
      schema_registry_config:
        basic.auth.user.info: "${REGISTRY_API_KEY_ID}:${REGISTRY_API_KEY_SECRET}"

sink:
  # sink configs
```

If you are trying to add domains to your topics you can use a configuration like below.

```yml
source:
  type: "kafka"
  config:
    # ...connection block
    domain:
      "urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810":
        allow:
          - ".*"
      "urn:li:domain:d6ec9868-6736-4b1f-8aa6-fee4c5948f17":
        deny:
          - ".*"
```

Note that the `domain` in config above can be either an _urn_ or a domain _id_ (i.e. `urn:li:domain:13ae4d85-d955-49fc-8474-9004c663a810` or simply `13ae4d85-d955-49fc-8474-9004c663a810`). The Domain should exist in your DataHub instance before ingesting data into the Domain. To create a Domain on DataHub, check out the [Domains User Guide](https://datahubproject.io/docs/domains/).

If you are using a non-default subject naming strategy in the schema registry, such as [RecordNameStrategy](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work), the mapping for the topic's key and value schemas to the schema registry subject names should be provided via `topic_subject_map` as shown in the configuration below.

```yml
source:
  type: "kafka"
  config:
    # ...connection block
    # Defines the mapping for the key & value schemas associated with a topic & the subject name registered with the
    # kafka schema registry.
    topic_subject_map:
      # Defines both key & value schema for topic 'my_topic_1'
      "my_topic_1-key": "io.acryl.Schema1"
      "my_topic_1-value": "io.acryl.Schema2"
      # Defines only the value schema for topic 'my_topic_2' (the topic doesn't have a key schema).
      "my_topic_2-value": "io.acryl.Schema3"
```

### Custom Schema Registry

The Kafka Source uses the schema registry to figure out the schema associated with both `key` and `value` for the topic.
By default it uses the [Confluent's Kafka Schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
and supports the `AVRO` and `PROTOBUF` schema types.


If you're using a custom schema registry, or you are using schema type other than `AVRO` or `PROTOBUF`, then you can provide your own
custom implementation of the `KafkaSchemaRegistryBase` class, and implement the `get_schema_metadata(topic, platform_urn)` method that
given a topic name would return object of `SchemaMetadata` containing schema for that topic. Please refer
`datahub.ingestion.source.confluent_schema_registry::ConfluentSchemaRegistry` for sample implementation of this class.
```python
class KafkaSchemaRegistryBase(ABC):
    @abstractmethod
    def get_schema_metadata(
        self, topic: str, platform_urn: str
    ) -> Optional[SchemaMetadata]:
        pass
```

The custom schema registry class can be configured using the `schema_registry_class` config param of the `kafka` source as shown below.
```YAML
source:
  type: "kafka"
  config:
    # Set the custom schema registry implementation class
    schema_registry_class: "datahub.ingestion.source.confluent_schema_registry.ConfluentSchemaRegistry"
    # Coordinates
    connection:
      bootstrap: "broker:9092"
      schema_registry_url: http://localhost:8081

# sink configs
```

### Limitations of `PROTOBUF` schema types implementation

The current implementation of the support for `PROTOBUF` schema type has the following limitations:

+ Requires Python 3.7 & above.
+ Recursive types are not supported.
+ If the schemas of different topics define a type in the same package, the source would raise an exception.

In addition to this, maps are represented as arrays of messages. The following message,

```
message MessageWithMap {
  map<int, string> map_1 = 1;
}
```

becomes:

```
message Map1Entry {
  int key = 1;
  string value = 2/
}
message MessageWithMap {
  repeated Map1Entry map_1 = 1;
}
```
