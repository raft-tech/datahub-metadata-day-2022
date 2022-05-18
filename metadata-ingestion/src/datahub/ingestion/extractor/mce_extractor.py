from typing import Iterable, Union

from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api import RecordEnvelope
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Extractor, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
    SystemMetadata,
)
from datahub.metadata.schema_classes import UsageAggregationClass

try:
    import black
except ImportError:
    black = None  # type: ignore


class WorkUnitRecordExtractor(Extractor):
    """An extractor that simply returns the data inside workunits back as records."""

    ctx: PipelineContext

    def configure(self, config_dict: dict, ctx: PipelineContext) -> None:
        self.ctx = ctx

    def get_records(
        self, workunit: WorkUnit
    ) -> Iterable[
        RecordEnvelope[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
                UsageAggregationClass,
            ]
        ]
    ]:
        if isinstance(workunit, MetadataWorkUnit):
            if isinstance(
                workunit.metadata,
                (
                    MetadataChangeEvent,
                    MetadataChangeProposal,
                    MetadataChangeProposalWrapper,
                ),
            ):
                workunit.metadata.systemMetadata = SystemMetadata(
                    lastObserved=get_sys_time(), runId=self.ctx.run_id
                )
                if (
                    isinstance(workunit.metadata, MetadataChangeEvent)
                    and len(workunit.metadata.proposedSnapshot.aspects) == 0
                ):
                    raise AttributeError("every mce must have at least one aspect")
            if not workunit.metadata.validate():

                invalid_mce = str(workunit.metadata)

                if black is not None:
                    invalid_mce = black.format_str(invalid_mce, mode=black.FileMode())

                raise ValueError(
                    f"source produced an invalid metadata work unit: {invalid_mce}"
                )

            yield RecordEnvelope(
                workunit.metadata,
                {
                    "workunit_id": workunit.id,
                },
            )
        elif isinstance(workunit, UsageStatsWorkUnit):
            if not workunit.usageStats.validate():

                invalid_usage_stats = str(workunit.usageStats)

                if black is not None:
                    invalid_usage_stats = black.format_str(
                        invalid_usage_stats, mode=black.FileMode()
                    )

                raise ValueError(
                    f"source produced an invalid usage stat: {invalid_usage_stats}"
                )
            yield RecordEnvelope(
                workunit.usageStats,
                {
                    "workunit_id": workunit.id,
                },
            )
        else:
            raise ValueError(f"unknown WorkUnit type {type(workunit)}")

    def close(self):
        pass
