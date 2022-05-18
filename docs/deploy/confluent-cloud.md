# Integrating with Confluent Cloud 

DataHub provides the ability to easily leverage Confluent Cloud as your Kafka provider. To do so, you'll need to configure DataHub to talk to a broker and schema registry hosted by Confluent.

Doing this is a matter of configuring the Kafka Producer and Consumers used by DataHub correctly. There are 2 places where Kafka configuration should be provided: the metadata server (GMS) and the frontend server (datahub-frontend). Follow the steps below to configure these components for your deployment.

## **Step 1: Create topics in Confluent Control Center**

First, you'll need to create following new topics in the [Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html). By default they have the following names:

1. **MetadataChangeEvent_v4**: Metadata change proposal messages
2. **MetadataAuditEvent_v4**: Metadata change log messages 
3. **FailedMetadataChangeEvent_v4**: Failed to process #1 event
4. **DataHubUsageEvent_v1**: User behavior tracking event for UI
5. **MetadataChangeProposal_v1** 
6. **FailedMetadataChangeProposal_v1**
7. **MetadataChangeLog_Versioned_v1**
8. **MetadataChangeLog_Timeseries_v1**

The last 4 are exaplined in [MCP/MCL](../advanced/mcp-mcl.md)

To do so, navigate to your **Cluster** and click "Create Topic". Feel free to tweak the default topic configurations to
match your preferences.

![CreateTopic](../imgs/confluent-create-topic.png)

## Step 2: Configure DataHub Container to use Confluent Cloud Topics

### Docker Compose

If you are deploying DataHub via docker compose, enabling connection to Confluent is a matter of a) creating topics in the Confluent Control Center and b) changing the default container environment variables.

First, configure GMS to connect to Confluent Cloud by changing `docker/gms/env/docker.env`:

```
KAFKA_BOOTSTRAP_SERVER=pkc-g4ml2.eu-west-2.aws.confluent.cloud:9092
KAFKA_SCHEMAREGISTRY_URL=https://plrm-qwlpp.us-east-2.aws.confluent.cloud

# Confluent Cloud Configs
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule   required username='XFA45EL1QFUQP4PA' password='ltyf96EvR1YYutsjLB3ZYfrk+yfCXD8sQHCE3EMp57A2jNs4RR7J1bU9k6lM6rU';
SPRING_KAFKA_PROPERTIES_SASL_MECHANISM=PLAIN
SPRING_KAFKA_PROPERTIES_CLIENT_DNS_LOOKUP=use_all_dns_ips
SPRING_KAFKA_PROPERTIES_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO
SPRING_KAFKA_PROPERTIES_BASIC_AUTH_USER_INFO=P2ETAN5QR2LCWL14:RTjqw7AfETDl0RZo/7R0123LhPYs2TGjFKmvMWUFnlJ3uKubFbB1Sfs7aOjjNi1m23
```

Next, configure datahub-frontend to connect to Confluent Cloud by changing `docker/datahub-frontend/env/docker.env`:

```
KAFKA_BOOTSTRAP_SERVER=pkc-g4ml2.eu-west-2.aws.confluent.cloud:9092

# Confluent Cloud Configs
KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL
KAFKA_PROPERTIES_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule   required username='XFA45EL1QFUQP4PA' password='ltyf96EvR1YYutsjLB3ZYfrk+yfCXD8sQHCE3EMp57A2jNs4RR7J1bU9k6lM6rU';
KAFKA_PROPERTIES_SASL_MECHANISM=PLAIN
KAFKA_PROPERTIES_CLIENT_DNS_LOOKUP=use_all_dns_ips
KAFKA_PROPERTIES_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO
KAFKA_PROPERTIES_BASIC_AUTH_USER_INFO=P2ETAN5QR2LCWL14:RTjqw7AfETDl0RZo/7R0123LhPYs2TGjFKmvMWUFnlJ3uKubFbB1Sfs7aOjjNi1m23
```

Note that this step is only required if DATAHUB_ANALYTICS_ENABLED is not set to false.

If you're deploying with Docker Compose, you do not need to deploy the Zookeeper, Kafka Broker, or Schema Registry containers that ship by default.

### Helm

If you're deploying to K8s using Helm, you can simply change the `datahub-helm` values.yml to point to Confluent Cloud and disable some default containers:

First, disable the `cp-schema-registry` service:

```
cp-schema-registry:
  enabled: false 
```

Next, disable the `kafkaSetupJob` service:

```
kafkaSetupJob:
    enabled: false
```

Then, update the `kafka` configurations to point to your Confluent Cloud broker and schema registry instance, along with the topics you've created in Step 1:

```
kafka:
      bootstrap:
        server: pkc-g4ml2.eu-west-2.aws.confluent.cloud:9092
      schemaregistry:
        url: https://plrm-qwlpp.us-east-2.aws.confluent.cloud
```

Next, you'll want to create 2 new Kubernetes secrets, one for the JaaS configuration which contains the username and password for Confluent,
and another for the user info used for connecting to the schema registry. You'll find the values for each within the Confluent Control Center. Specifically,
select "Clients" -> "Configure new Java Client". You should see a page like the following:


![Config](../imgs/confluent-cloud-config.png)

You'll want to generate both a Kafka Cluster API Key & a Schema Registry key. Once you do so,you should see the config
automatically populate with your new secrets:

![Config](../imgs/confluent-cloud-config-2.png)

You'll need to copy the values of `sasl.jaas.config` and `basic.auth.user.info`
for the next step.

The next step is to create K8s secrets containing the config values you've just generated. Specifically, you'll run the following commands:

```shell
kubectl create secret generic confluent-secrets --from-literal=sasl_jaas_config="<your-sasl.jaas.config>"
kubectl create secret generic confluent-secrets --from-literal=basic_auth_user_info="<your-basic.auth.user.info>"
```

With your config values substituted as appropriate. For example, in our case we'd run:

```shell
kubectl create secret generic confluent-secrets --from-literal=sasl_jaas_config="org.apache.kafka.common.security.plain.PlainLoginModule   required username='XFA45EL1QFUQP4PA' password='ltyf96EvR1YYutsjLB3ZYfrk+yfCXD8sQHCE3EMp57A2jNs4RR7J1bU9k6lM6rU';"
kubectl create secret generic confluent-secrets --from-literal=basic_auth_user_info="P2ETAN5QR2LCWL14:RTjqw7AfETDl0RZo/7R0123LhPYs2TGjFKmvMWUFnlJ3uKubFbB1Sfs7aOjjNi1m23"
```

Finally, we'll configure our containers to pick up the Confluent Kafka Configs by changing two config blocks in our values.yaml file. You
should see these blocks commented at the bottom of the template. You'll want to uncomment them and set them to the following values: 

```
credentialsAndCertsSecrets:
  name: confluent-secrets
  secureEnv:
    sasl.jaas.config: sasl_jaas_config
    basic.auth.user.info: basic_auth_user_info


springKafkaConfigurationOverrides:
      security.protocol: SASL
      sasl.mechanism: PLAIN
      client.dns.lookup: use_all_dns_ips
      basic.auth.credentials.source: USER_INFO
```

Then simply apply the updated `values.yaml` to your K8s cluster via `kubectl apply`. 

## Contribution
Accepting contributions for a setup script compatible with Confluent Cloud!

Currently the kafka-setup-job container we ship with is only compatible with a distribution of Kafka wherein ZooKeeper
is exposed and available. A version of the job using the [Confluent CLI](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_create.html) 
would be very useful for the broader community. 