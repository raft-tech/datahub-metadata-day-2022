import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List, Optional, Union, cast

from datahub.api.entities.datajob import DataFlow, DataJob
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey
from datahub.metadata.com.linkedin.pegasus2avro.dataprocess import (
    DataProcessInstanceInput,
    DataProcessInstanceOutput,
    DataProcessInstanceProperties,
    DataProcessInstanceRelationships,
    RunResultType,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DataProcessInstanceRunEventClass,
    DataProcessInstanceRunResultClass,
    DataProcessRunStatusClass,
    DataProcessTypeClass,
    StatusClass,
)
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn
from datahub.utilities.urns.data_process_instance_urn import DataProcessInstanceUrn
from datahub.utilities.urns.dataset_urn import DatasetUrn

if TYPE_CHECKING:
    from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
    from datahub.emitter.rest_emitter import DatahubRestEmitter


class DataProcessInstanceKey(DatahubKey):
    cluster: str
    orchestrator: str
    id: str


class InstanceRunResult(str, Enum):
    SUCCESS = RunResultType.SUCCESS
    SKIPPED = RunResultType.SKIPPED
    FAILURE = RunResultType.FAILURE


@dataclass
class DataProcessInstance:
    """This is a DataProcessInstance class which represent an instance of a DataFlow or DataJob.

    Args:
        id (str): The id of the dataprocess instance execution.
        orchestrator (str): The orchestrator which does the execution. For example airflow.
        type (str): The execution type like Batch, Streaming, Ad-hoc, etc..  See valid values at DataProcessTypeClass
        template_urn (Optional[Union[DataJobUrn, DataFlowUrn]]): The parent DataJob or DataFlow which was instantiated if applicable
        parent_instance (Optional[DataProcessInstanceUrn]): The parent execution's urn if applicable
        properties Dict[str, str]: Custom properties to set for the DataProcessInstance
        url (Optional[str]): Url which points to the exection at the orchestrator
        inlets (List[str]): List of entities the DataProcessInstance consumes
        outlets (List[str]): List of entities the DataProcessInstance produces
    """

    id: str
    urn: DataProcessInstanceUrn = field(init=False)
    orchestrator: str
    cluster: str
    type: str = DataProcessTypeClass.BATCH_SCHEDULED
    template_urn: Optional[Union[DataJobUrn, DataFlowUrn]] = None
    parent_instance: Optional[DataProcessInstanceUrn] = None
    properties: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    inlets: List[DatasetUrn] = field(default_factory=list)
    outlets: List[DatasetUrn] = field(default_factory=list)
    upstream_urns: List[DataProcessInstanceUrn] = field(default_factory=list)
    _template_object: Optional[Union[DataJob, DataFlow]] = field(
        init=False, default=None, repr=False
    )

    def __post_init__(self):
        self.urn = DataProcessInstanceUrn.create_from_id(
            dataprocessinstance_id=DataProcessInstanceKey(
                cluster=self.cluster,
                orchestrator=self.orchestrator,
                id=self.id,
            ).guid()
        )

    def start_event_mcp(
        self, start_timestamp_millis: int, attempt: Optional[int] = None
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """

        :rtype: (Iterable[MetadataChangeProposalWrapper])
        :param start_timestamp_millis:  (int) the execution start time in milliseconds
        :param attempt: (int) the number of attempt of the execution with the same execution id
        """
        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=str(self.urn),
            aspectName="dataProcessInstanceRunEvent",
            aspect=DataProcessInstanceRunEventClass(
                status=DataProcessRunStatusClass.STARTED,
                timestampMillis=start_timestamp_millis,
                attempt=attempt,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

    def emit_process_start(
        self,
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        start_timestamp_millis: int,
        attempt: Optional[int] = None,
        emit_template: bool = True,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """

        :rtype: None
        :param emitter: Datahub Emitter to emit the proccess event
        :param start_timestamp_millis: (int) the execution start time in milliseconds
        :param attempt: the number of attempt of the execution with the same execution id
        :param emit_template: (bool) If it is set the template of the execution (datajob, datflow) will be emitted as well.
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        if emit_template and self.template_urn is not None:
            template_object: Union[DataJob, DataFlow]
            if self._template_object is None:
                input_datajob_urns: List[DataJobUrn] = []
                if isinstance(self.template_urn, DataFlowUrn):
                    job_flow_urn = self.template_urn
                    template_object = DataFlow(
                        cluster=self.template_urn.get_env(),
                        orchestrator=self.template_urn.get_orchestrator_name(),
                        id=self.template_urn.get_flow_id(),
                    )
                    for mcp in template_object.generate_mcp():
                        self._emit_mcp(mcp, emitter, callback)
                elif isinstance(self.template_urn, DataJobUrn):
                    job_flow_urn = self.template_urn.get_data_flow_urn()
                    template_object = DataJob(
                        id=self.template_urn.get_job_id(),
                        upstream_urns=input_datajob_urns,
                        flow_urn=self.template_urn.get_data_flow_urn(),
                    )
                    for mcp in template_object.generate_mcp():
                        self._emit_mcp(mcp, emitter, callback)
                else:
                    raise Exception(
                        f"Invalid urn type {self.template_urn.__class__.__name__}"
                    )
                for upstream in self.upstream_urns:
                    input_datajob_urns.append(
                        DataJobUrn.create_from_ids(
                            job_id=upstream.get_dataprocessinstance_id(),
                            data_flow_urn=str(job_flow_urn),
                        )
                    )
            else:
                template_object = self._template_object

            for mcp in template_object.generate_mcp():
                self._emit_mcp(mcp, emitter, callback)

        for mcp in self.generate_mcp():
            self._emit_mcp(mcp, emitter, callback)
        for mcp in self.start_event_mcp(start_timestamp_millis, attempt):
            self._emit_mcp(mcp, emitter, callback)

    def end_event_mcp(
        self,
        end_timestamp_millis: int,
        result: InstanceRunResult,
        result_type: Optional[str] = None,
        attempt: Optional[int] = None,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """

        :param end_timestamp_millis: the end time of the execution in milliseconds
        :param result: (InstanceRunResult) the result of the run
        :param result_type: (string) It identifies the system where the native result comes from like Airflow, Azkaban
        :param attempt: (int) the attempt number of this execution
        """
        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=str(self.urn),
            aspectName="dataProcessInstanceRunEvent",
            aspect=DataProcessInstanceRunEventClass(
                status=DataProcessRunStatusClass.COMPLETE,
                timestampMillis=end_timestamp_millis,
                result=DataProcessInstanceRunResultClass(
                    type=result,
                    nativeResultType=result_type
                    if result_type is not None
                    else self.orchestrator,
                ),
                attempt=attempt,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

    def emit_process_end(
        self,
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        end_timestamp_millis: int,
        result: InstanceRunResult,
        result_type: Optional[str] = None,
        attempt: Optional[int] = None,
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """
        Generate an DataProcessInstance finish event and emits is

        :param emitter: (Union[DatahubRestEmitter, DatahubKafkaEmitter]) the datahub emitter to emit generated mcps
        :param end_timestamp_millis: (int) the end time of the execution in milliseconds
        :param result: (InstanceRunResult) The result of the run
        :param result_type: (string) It identifies the system where the native result comes from like Airflow, Azkaban
        :param attempt: (int) the attempt number of this execution
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        for mcp in self.end_event_mcp(
            end_timestamp_millis=end_timestamp_millis,
            result=result,
            result_type=result_type,
            attempt=attempt,
        ):
            self._emit_mcp(mcp, emitter, callback)

    def generate_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Generates mcps from the object
        :rtype: Iterable[MetadataChangeProposalWrapper]
        """
        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=str(self.urn),
            aspectName="dataProcessInstanceProperties",
            aspect=DataProcessInstanceProperties(
                name=self.id,
                created=AuditStampClass(
                    time=int(time.time() * 1000),
                    actor="urn:li:corpuser:datahub",
                ),
                type=self.type,
                customProperties=self.properties,
                externalUrl=self.url,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        mcp = MetadataChangeProposalWrapper(
            entityType="dataProcessInstance",
            entityUrn=str(self.urn),
            aspectName="dataProcessInstanceRelationships",
            aspect=DataProcessInstanceRelationships(
                upstreamInstances=[str(urn) for urn in self.upstream_urns],
                parentTemplate=str(self.template_urn) if self.template_urn else None,
                parentInstance=str(self.parent_instance)
                if self.parent_instance is not None
                else None,
            ),
            changeType=ChangeTypeClass.UPSERT,
        )
        yield mcp

        yield from self.generate_inlet_outlet_mcp()

    @staticmethod
    def _emit_mcp(
        mcp: MetadataChangeProposalWrapper,
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """

        :param emitter: (Union[DatahubRestEmitter, DatahubKafkaEmitter]) the datahub emitter to emit generated mcps
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        if type(emitter).__name__ == "DatahubKafkaEmitter":
            assert callback is not None
            kafka_emitter = cast("DatahubKafkaEmitter", emitter)
            kafka_emitter.emit(mcp, callback)
        else:
            rest_emitter = cast("DatahubRestEmitter", emitter)
            rest_emitter.emit(mcp)

    def emit(
        self,
        emitter: Union["DatahubRestEmitter", "DatahubKafkaEmitter"],
        callback: Optional[Callable[[Exception, str], None]] = None,
    ) -> None:
        """

        :param emitter: (Union[DatahubRestEmitter, DatahubKafkaEmitter]) the datahub emitter to emit generated mcps
        :param callback: (Optional[Callable[[Exception, str], None]]) the callback method for KafkaEmitter if it is used
        """
        for mcp in self.generate_mcp():
            self._emit_mcp(mcp, emitter, callback)

    @staticmethod
    def from_datajob(
        datajob: DataJob,
        id: str,
        clone_inlets: bool = False,
        clone_outlets: bool = False,
    ) -> "DataProcessInstance":
        """
        Generates DataProcessInstance from a DataJob

        :param datajob: (DataJob) the datajob from generate the DataProcessInstance
        :param id: (str) the id for the DataProcessInstance
        :param clone_inlets: (bool) wheather to clone datajob's inlets
        :param clone_outlets: (bool) wheather to clone datajob's outlets
        :return: DataProcessInstance
        """
        dpi: DataProcessInstance = DataProcessInstance(
            orchestrator=datajob.flow_urn.get_orchestrator_name(),
            cluster=datajob.flow_urn.get_env(),
            template_urn=datajob.urn,
            id=id,
        )
        dpi._template_object = datajob

        if clone_inlets:
            dpi.inlets = datajob.inlets
        if clone_outlets:
            dpi.outlets = datajob.outlets
        return dpi

    @staticmethod
    def from_dataflow(dataflow: DataFlow, id: str) -> "DataProcessInstance":
        """
        Generates DataProcessInstance from a DataFlow

        :param dataflow: (DataFlow) the DataFlow from generate the DataProcessInstance
        :param id: (str) the id for the DataProcessInstance
        :return: DataProcessInstance
        """
        dpi = DataProcessInstance(
            id=id,
            orchestrator=dataflow.orchestrator,
            cluster=dataflow.cluster,
            template_urn=dataflow.urn,
        )
        dpi._template_object = dataflow
        return dpi

    def generate_inlet_outlet_mcp(self) -> Iterable[MetadataChangeProposalWrapper]:
        if self.inlets:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataProcessInstance",
                entityUrn=str(self.urn),
                aspectName="dataProcessInstanceInput",
                aspect=DataProcessInstanceInput(
                    inputs=[str(urn) for urn in self.inlets]
                ),
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

        if self.outlets:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataProcessInstance",
                entityUrn=str(self.urn),
                aspectName="dataProcessInstanceOutput",
                aspect=DataProcessInstanceOutput(
                    outputs=[str(urn) for urn in self.outlets]
                ),
                changeType=ChangeTypeClass.UPSERT,
            )
            yield mcp

        # Force entity materialization
        for iolet in self.inlets + self.outlets:
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=str(iolet),
                aspectName="status",
                aspect=StatusClass(removed=False),
                changeType=ChangeTypeClass.UPSERT,
            )

            yield mcp
