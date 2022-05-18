import json
import logging
import re
from dataclasses import dataclass
from hashlib import md5
from typing import Iterable, List, Optional, Tuple, cast

import requests

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_domain_to_entity_wu
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.kafka_state import KafkaCheckpointState
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_config.pulsar import PulsarSourceConfig
from datahub.ingestion.source_report.pulsar import PulsarSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    JobStatusClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)


class PulsarTopic(object):
    __slots__ = ["topic_parts", "fullname", "type", "tenant", "namespace", "topic"]

    def __init__(self, topic):
        topic_parts = re.split("[: /]", topic)
        self.fullname = topic
        self.type = topic_parts[0]
        self.tenant = topic_parts[3]
        self.namespace = topic_parts[4]
        self.topic = topic_parts[5]


class PulsarSchema(object):
    __slots__ = [
        "schema_version",
        "schema_name",
        "schema_description",
        "schema_type",
        "schema_str",
        "properties",
    ]

    def __init__(self, schema):
        self.schema_version = schema.get("version")

        avro_schema = json.loads(schema.get("data"))
        self.schema_name = avro_schema.get("namespace") + "." + avro_schema.get("name")
        self.schema_description = avro_schema.get("doc")
        self.schema_type = schema.get("type")
        self.schema_str = schema.get("data")
        self.properties = schema.get("properties")


@platform_name("Pulsar")
@support_status(SupportStatus.INCUBATING)
@config_class(PulsarSourceConfig)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@dataclass
class PulsarSource(StatefulIngestionSourceBase):
    def __init__(self, config: PulsarSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.platform: str = "pulsar"
        self.config: PulsarSourceConfig = config
        self.report: PulsarSourceReport = PulsarSourceReport()
        self.base_url: str = self.config.web_service_url + "/admin/v2"
        self.tenants: List[str] = config.tenants

        if (
            self.is_stateful_ingestion_configured()
            and not self.config.platform_instance
        ):
            raise ConfigurationError(
                "Enabling Pulsar stateful ingestion requires to specify a platform instance."
            )

        self.session = requests.Session()
        self.session.verify = self.config.verify_ssl
        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )

        if self._is_oauth_authentication_configured():
            # Get OpenId configuration from issuer, e.g. token_endpoint
            oid_config_url = (
                "%s/.well-known/openid-configuration" % self.config.issuer_url
            )
            oid_config_response = requests.get(
                oid_config_url, verify=False, allow_redirects=False
            )

            if oid_config_response:
                self.config.oid_config.update(oid_config_response.json())
            else:
                logger.error(
                    "Unexpected response while getting discovery document using %s : %s"
                    % (oid_config_url, oid_config_response)
                )

            if "token_endpoint" not in self.config.oid_config:
                raise Exception(
                    "The token_endpoint is not set, please verify the configured issuer_url or"
                    " set oid_config.token_endpoint manually in the configuration file."
                )

        # Authentication configured
        if (
            self._is_token_authentication_configured()
            or self._is_oauth_authentication_configured()
        ):
            # Update session header with Bearer token
            self.session.headers.update(
                {"Authorization": f"Bearer {self.get_access_token()}"}
            )

    def get_access_token(self) -> str:
        """
        Returns an access token used for authentication, token comes from config or third party provider
        when issuer_url is provided
        """
        # JWT, get access token (jwt) from config
        if self._is_token_authentication_configured():
            return str(self.config.token)

        # OAuth, connect to issuer and return access token
        if self._is_oauth_authentication_configured():
            assert self.config.client_id
            assert self.config.client_secret
            data = {"grant_type": "client_credentials"}
            try:
                # Get a token from the issuer
                token_endpoint = self.config.oid_config["token_endpoint"]
                logger.info(f"Request access token from {token_endpoint}")
                token_response = requests.post(
                    url=token_endpoint,
                    data=data,
                    verify=False,
                    allow_redirects=False,
                    auth=(
                        self.config.client_id,
                        self.config.client_secret,
                    ),
                )
                token_response.raise_for_status()

                return token_response.json()["access_token"]

            except requests.exceptions.RequestException as e:
                logger.error(f"An error occurred while handling your request: {e}")
        # Failed to get an access token,
        raise ConfigurationError(
            f"Failed to get the Pulsar access token from token_endpoint {self.config.oid_config.get('token_endpoint')}."
            f" Please check your input configuration."
        )

    def _get_pulsar_metadata(self, url):
        """
        Interacts with the Pulsar Admin Api and returns Pulsar metadata. Invocations with insufficient privileges
        are logged.
        """
        try:
            # Request the Pulsar metadata
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            # Return the response for status_code 200
            return response.json()

        except requests.exceptions.HTTPError as http_error:
            # Topics can exist without a schema, log the warning and move on
            if http_error.response.status_code == 404 and "/schemas/" in url:
                message = (
                    f"Failed to get schema from schema registry. The topic is either schema-less or"
                    f" no messages have been written to the topic yet."
                    f" {http_error}"
                )
                self.report.report_warning("NoSchemaFound", message)
            else:
                # Authorization error
                message = f"An HTTP error occurred: {http_error}"
                self.report.report_warning("HTTPError", message)
        except requests.exceptions.RequestException as e:
            raise Exception(
                f"An ambiguous exception occurred while handling the request: {e}"
            )

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        return job_id == (
            self.get_default_ingestion_job_id()
            and self.is_stateful_ingestion_configured()
            and self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
        )

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name that kafka provides.
        """
        return JobId("ingest_from_pulsar_source")

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Create a custom checkpoint with empty state for the job.
        """
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.config,
                # TODO Create a PulsarCheckpointState ?
                state=KafkaCheckpointState(),
            )
        return None

    def get_platform_instance_id(self) -> str:
        assert self.config.platform_instance is not None
        return self.config.platform_instance

    @classmethod
    def create(cls, config_dict, ctx):
        config = PulsarSourceConfig.parse_obj(config_dict)

        # Do not include each individual partition for partitioned topics,
        if config.exclude_individual_partitions:
            config.topic_patterns.deny.append(r".*-partition-[0-9]+")

        return cls(config, ctx)

    def soft_delete_dataset(self, urn: str, type: str) -> Iterable[MetadataWorkUnit]:
        logger.debug(f"Soft-deleting stale entity of type {type} - {urn}.")
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=urn,
            changeType=ChangeTypeClass.UPSERT,
            aspectName="status",
            aspect=StatusClass(removed=True),
        )
        wu = MetadataWorkUnit(id=f"soft-delete-{type}-{urn}", mcp=mcp)
        self.report.report_workunit(wu)
        self.report.report_stale_entity_soft_deleted(urn)
        yield wu

    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), KafkaCheckpointState
        )
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        if (
            self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
            and last_checkpoint is not None
            and last_checkpoint.state is not None
            and cur_checkpoint is not None
            and cur_checkpoint.state is not None
        ):
            logger.debug("Checking for stale entity removal.")

            last_checkpoint_state = cast(KafkaCheckpointState, last_checkpoint.state)
            cur_checkpoint_state = cast(KafkaCheckpointState, cur_checkpoint.state)

            for topic_urn in last_checkpoint_state.get_topic_urns_not_in(
                cur_checkpoint_state
            ):
                yield from self.soft_delete_dataset(topic_urn, "topic")

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Interacts with the Pulsar Admin Api and loops over tenants, namespaces and topics. For every topic
        the schema information is retrieved if available.

        Pulsar web service admin rest api urls for retrieving topic information
            - [web_service_url]/admin/v2/persistent/{tenant}/{namespace}
            - [web_service_url]/admin/v2/persistent/{tenant}/{namespace}/partitioned
            - [web_service_url]/admin/v2/non-persistent/{tenant}/{namespace}
            - [web_service_url]/admin/v2/non-persistent/{tenant}/{namespace}/partitioned
        """
        topic_urls = [
            self.base_url + "/persistent/{}",
            self.base_url + "/persistent/{}/partitioned",
            self.base_url + "/non-persistent/{}",
            self.base_url + "/non-persistent/{}/partitioned",
        ]

        # Report the Pulsar broker version we are communicating with
        self.report.report_pulsar_version(
            self.session.get(
                "%s/brokers/version" % self.base_url,
                timeout=self.config.timeout,
            ).text
        )

        # If no tenants are provided, request all tenants from cluster using /admin/v2/tenants endpoint.
        # Requesting cluster tenant information requires superuser privileges
        if not self.tenants:
            self.tenants = self._get_pulsar_metadata(self.base_url + "/tenants") or []

        # Initialize counters
        self.report.tenants_scanned = 0
        self.report.namespaces_scanned = 0
        self.report.topics_scanned = 0

        for tenant in self.tenants:
            self.report.tenants_scanned += 1
            if self.config.tenant_patterns.allowed(tenant):
                # Get namespaces belonging to a tenant, /admin/v2/%s/namespaces
                # A tenant admin role has sufficient privileges to perform this action
                namespaces = (
                    self._get_pulsar_metadata(self.base_url + "/namespaces/%s" % tenant)
                    or []
                )
                for namespace in namespaces:
                    self.report.namespaces_scanned += 1
                    if self.config.namespace_patterns.allowed(namespace):
                        # Get all topics (persistent, non-persistent and partitioned) belonging to a tenant/namespace
                        # Four endpoint invocations are needs to get all topic metadata for a namespace
                        topics = {}
                        for url in topic_urls:
                            # Topics are partitioned when admin url ends with /partitioned
                            partitioned = url.endswith("/partitioned")
                            # Get the topics for each type
                            pulsar_topics = (
                                self._get_pulsar_metadata(url.format(namespace)) or []
                            )
                            # Create a mesh of topics with partitioned values, the
                            # partitioned info is added as a custom properties later
                            topics.update(
                                {topic: partitioned for topic in pulsar_topics}
                            )

                        # For all allowed topics get the metadata
                        for topic, is_partitioned in topics.items():
                            self.report.topics_scanned += 1
                            if self.config.topic_patterns.allowed(topic):

                                yield from self._extract_record(topic, is_partitioned)
                                # Add topic to checkpoint if stateful ingestion is enabled
                                if self.is_stateful_ingestion_configured():
                                    self._add_topic_to_checkpoint(topic)
                            else:
                                self.report.report_topics_dropped(topic)

                        if self.is_stateful_ingestion_configured():
                            # Clean up stale entities.
                            yield from self.gen_removed_entity_workunits()

                    else:
                        self.report.report_namespaces_dropped(namespace)
            else:
                self.report.report_tenants_dropped(tenant)

    def _add_topic_to_checkpoint(self, topic: str) -> None:
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )

        if cur_checkpoint is not None:
            checkpoint_state = cast(KafkaCheckpointState, cur_checkpoint.state)
            checkpoint_state.add_topic_urn(
                make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=topic,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
            )

    def _is_token_authentication_configured(self) -> bool:
        if self.config.token is not None:
            return True
        return False

    def _is_oauth_authentication_configured(self) -> bool:
        if self.config.issuer_url is not None:
            return True
        return False

    def _get_schema_and_fields(
        self, pulsar_topic: PulsarTopic, is_key_schema: bool
    ) -> Tuple[Optional[PulsarSchema], List[SchemaField]]:

        pulsar_schema: Optional[PulsarSchema] = None

        schema_url = self.base_url + "/schemas/%s/%s/%s/schema" % (
            pulsar_topic.tenant,
            pulsar_topic.namespace,
            pulsar_topic.topic,
        )

        schema_payload = self._get_pulsar_metadata(schema_url)

        # Get the type and schema from the Pulsar Schema
        if schema_payload is not None:
            # pulsar_schema: Optional[PulsarSchema] = None
            pulsar_schema = PulsarSchema(schema_payload)

        # Obtain the schema fields from schema for the topic.
        fields: List[SchemaField] = []
        if pulsar_schema is not None:
            fields = self._get_schema_fields(
                pulsar_topic=pulsar_topic,
                schema=pulsar_schema,
                is_key_schema=is_key_schema,
            )
        return pulsar_schema, fields

    def _get_schema_fields(
        self, pulsar_topic: PulsarTopic, schema: PulsarSchema, is_key_schema: bool
    ) -> List[SchemaField]:
        # Parse the schema and convert it to SchemaFields.
        fields: List[SchemaField] = []
        if schema.schema_type == "AVRO" or schema.schema_type == "JSON":
            # Extract fields from schema and get the FQN for the schema
            fields = schema_util.avro_schema_to_mce_fields(
                schema.schema_str, is_key_schema=is_key_schema
            )
        else:
            self.report.report_warning(
                pulsar_topic.fullname,
                f"Parsing Pulsar schema type {schema.schema_type} is currently not implemented",
            )
        return fields

    def _get_schema_metadata(
        self, pulsar_topic: PulsarTopic, platform_urn: str
    ) -> Tuple[Optional[PulsarSchema], Optional[SchemaMetadata]]:

        schema, fields = self._get_schema_and_fields(
            pulsar_topic=pulsar_topic, is_key_schema=False
        )  # type: Tuple[Optional[PulsarSchema], List[SchemaField]]

        # Create the schemaMetadata aspect.
        if schema is not None:
            md5_hash = md5(schema.schema_str.encode()).hexdigest()

            return schema, SchemaMetadata(
                schemaName=schema.schema_name,
                version=schema.schema_version,
                hash=md5_hash,
                platform=platform_urn,
                platformSchema=KafkaSchema(
                    documentSchema=schema.schema_str if schema is not None else "",
                    keySchema=None,
                ),
                fields=fields,
            )
        return None, None

    def _extract_record(
        self, topic: str, partitioned: bool
    ) -> Iterable[MetadataWorkUnit]:
        logger.info(f"topic = {topic}")

        # 1. Create and emit the default dataset for the topic. Extract type, tenant, namespace
        # and topic name from full Pulsar topic name i.e. persistent://tenant/namespace/topic
        pulsar_topic = PulsarTopic(topic)

        platform_urn = make_data_platform_urn(self.platform)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=pulsar_topic.fullname,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        status_wu = MetadataWorkUnit(
            id=f"{dataset_urn}-status",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="status",
                aspect=StatusClass(removed=False),
            ),
        )
        self.report.report_workunit(status_wu)
        yield status_wu

        # 2. Emit schemaMetadata aspect
        schema, schema_metadata = self._get_schema_metadata(pulsar_topic, platform_urn)
        if schema_metadata is not None:
            schema_metadata_wu = MetadataWorkUnit(
                id=f"{dataset_urn}-schemaMetadata",
                mcp=MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="schemaMetadata",
                    aspect=schema_metadata,
                ),
            )
            self.report.report_workunit(schema_metadata_wu)
            yield schema_metadata_wu

        # TODO Add topic properties (Pulsar 2.10.0 feature)
        # 3. Construct and emit dataset properties aspect
        if schema is not None:
            schema_properties = {
                "schema_version": str(schema.schema_version),
                "schema_type": schema.schema_type,
                "partitioned": str(partitioned).lower(),
            }
            # Add some static properties to the schema properties
            schema.properties.update(schema_properties)

            dataset_properties_wu = MetadataWorkUnit(
                id=f"{dataset_urn}-datasetProperties",
                mcp=MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="datasetProperties",
                    aspect=DatasetPropertiesClass(
                        description=schema.schema_description,
                        customProperties=schema.properties,
                    ),
                ),
            )
            self.report.report_workunit(dataset_properties_wu)
            yield dataset_properties_wu

        # 4. Emit browsePaths aspect
        pulsar_path = (
            f"{pulsar_topic.tenant}/{pulsar_topic.namespace}/{pulsar_topic.topic}"
        )
        browse_path_suffix = (
            f"{self.config.platform_instance}/{pulsar_path}"
            if self.config.platform_instance
            else pulsar_path
        )

        browse_path_wu = MetadataWorkUnit(
            id=f"{dataset_urn}-browsePaths",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="browsePaths",
                aspect=BrowsePathsClass(
                    [f"/{self.config.env.lower()}/{self.platform}/{browse_path_suffix}"]
                ),
            ),
        )
        self.report.report_workunit(browse_path_wu)
        yield browse_path_wu

        # 5. Emit dataPlatformInstance aspect.
        if self.config.platform_instance:
            platform_instance_wu = MetadataWorkUnit(
                id=f"{dataset_urn}-dataPlatformInstance",
                mcp=MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=dataset_urn,
                    aspectName="dataPlatformInstance",
                    aspect=DataPlatformInstanceClass(
                        platform=platform_urn,
                        instance=make_dataplatform_instance_urn(
                            self.platform, self.config.platform_instance
                        ),
                    ),
                ),
            )
            self.report.report_workunit(platform_instance_wu)
            yield platform_instance_wu

        # 6. Emit subtype aspect marking this as a "topic"
        subtype_wu = MetadataWorkUnit(
            id=f"{dataset_urn}-subTypes",
            mcp=MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=dataset_urn,
                aspectName="subTypes",
                aspect=SubTypesClass(typeNames=["topic"]),
            ),
        )
        self.report.report_workunit(subtype_wu)
        yield subtype_wu

        # 7. Emit domains aspect
        domain_urn: Optional[str] = None
        for domain, pattern in self.config.domain.items():
            if pattern.allowed(pulsar_topic.fullname):
                domain_urn = make_domain_urn(domain)

        if domain_urn:
            wus = add_domain_to_entity_wu(
                entity_type="dataset",
                entity_urn=dataset_urn,
                domain_urn=domain_urn,
            )
            for wu in wus:
                self.report.report_workunit(wu)
                yield wu

    def get_report(self):
        return self.report

    def update_default_job_run_summary(self) -> None:
        summary = self.get_job_run_summary(self.get_default_ingestion_job_id())
        if summary is not None:
            # For now just add the config and the report.
            summary.config = self.config.json()
            summary.custom_summary = self.report.as_string()
            summary.runStatus = (
                JobStatusClass.FAILED
                if self.get_report().failures
                else JobStatusClass.COMPLETED
            )

    def close(self):
        self.update_default_job_run_summary()
        self.prepare_for_commit()
        self.session.close()
