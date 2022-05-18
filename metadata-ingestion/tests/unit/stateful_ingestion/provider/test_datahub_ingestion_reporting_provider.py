import types
import unittest
from datetime import datetime
from typing import Dict, List, Optional, Type
from unittest.mock import MagicMock, patch

from avrogen.dict_wrapper import DictWrapper

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.ingestion_job_reporting_provider_base import (
    IngestionReportingProviderBase,
    JobId,
    JobStateKey,
    ReportingJobStatesMap,
    ReportingJobStateType,
)
from datahub.ingestion.reporting.datahub_ingestion_reporting_provider import (
    DatahubIngestionReportingProvider,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig
from datahub.metadata.schema_classes import JobStatusClass


class TestDatahubIngestionReportingProvider(unittest.TestCase):
    # Static members for the tests
    pipeline_name: str = "test_pipeline"
    platform_instance_id: str = "test_platform_instance_1"
    job_names: List[JobId] = [JobId("job1"), JobId("job2")]
    run_id: str = "test_run"
    job_state_key: JobStateKey = JobStateKey(
        pipeline_name=pipeline_name,
        platform_instance_id=platform_instance_id,
        job_names=job_names,
    )

    def setUp(self) -> None:
        self._setup_mock_graph()
        self.provider = self._create_provider()
        assert self.provider

    def _setup_mock_graph(self) -> None:
        """
        Setup monkey-patched graph client.
        """
        self.patcher = patch(
            "datahub.ingestion.graph.client.DataHubGraph", autospec=True
        )
        self.addCleanup(self.patcher.stop)
        self.mock_graph = self.patcher.start()
        # Make server stateful ingestion capable
        self.mock_graph.get_config.return_value = {"statefulIngestionCapable": True}
        # Bind mock_graph's emit_mcp to testcase's monkey_patch_emit_mcp so that we can emulate emits.
        self.mock_graph.emit_mcp = types.MethodType(
            self.monkey_patch_emit_mcp, self.mock_graph
        )
        # Bind mock_graph's get_latest_timeseries_value to monkey_patch_get_latest_timeseries_value
        self.mock_graph.get_latest_timeseries_value = types.MethodType(
            self.monkey_patch_get_latest_timeseries_value, self.mock_graph
        )
        # Tracking for emitted mcps.
        self.mcps_emitted: Dict[str, MetadataChangeProposalWrapper] = {}

    def _create_provider(self) -> IngestionReportingProviderBase:
        ctx: PipelineContext = PipelineContext(
            run_id=self.run_id, pipeline_name=self.pipeline_name
        )
        ctx.graph = self.mock_graph
        return DatahubIngestionReportingProvider.create(
            {}, ctx, name=DatahubIngestionReportingProvider.__name__
        )

    def monkey_patch_emit_mcp(
        self, graph_ref: MagicMock, mcpw: MetadataChangeProposalWrapper
    ) -> None:
        """
        Mockey patched implementation of DatahubGraph.emit_mcp that caches the mcp locally in memory.
        """
        self.assertIsNotNone(graph_ref)
        self.assertEqual(mcpw.entityType, "dataJob")
        self.assertEqual(mcpw.aspectName, "datahubIngestionRunSummary")
        # Cache the mcpw against the entityUrn
        assert mcpw.entityUrn is not None
        self.mcps_emitted[mcpw.entityUrn] = mcpw

    def monkey_patch_get_latest_timeseries_value(
        self,
        graph_ref: MagicMock,
        entity_urn: str,
        aspect_name: str,
        aspect_type: Type[DictWrapper],
        filter_criteria_map: Dict[str, str],
    ) -> Optional[DictWrapper]:
        """
        Monkey patched implementation of DatahubGraph.get_latest_timeseries_value that returns the latest cached aspect
        for a given entity urn.
        """
        self.assertIsNotNone(graph_ref)
        self.assertEqual(aspect_name, "datahubIngestionRunSummary")
        self.assertEqual(aspect_type, ReportingJobStateType)
        self.assertEqual(
            filter_criteria_map,
            {
                "pipelineName": self.pipeline_name,
                "platformInstanceId": self.platform_instance_id,
            },
        )
        # Retrieve the cached mcpw and return its aspect value.
        mcpw = self.mcps_emitted.get(entity_urn)
        if mcpw:
            return mcpw.aspect
        return None

    def test_provider(self):

        # 1. Create the job reports
        job_reports: Dict[JobId, ReportingJobStateType] = {
            # A completed job
            self.job_names[0]: ReportingJobStateType(
                timestampMillis=int(datetime.utcnow().timestamp() * 1000),
                pipelineName=self.pipeline_name,
                platformInstanceId=self.platform_instance_id,
                runId=self.run_id,
                runStatus=JobStatusClass.COMPLETED,
                config=MySQLConfig().json(),
            ),
            # A skipped job
            self.job_names[1]: ReportingJobStateType(
                timestampMillis=int(datetime.utcnow().timestamp() * 1000),
                pipelineName=self.pipeline_name,
                platformInstanceId=self.platform_instance_id,
                runId=self.run_id,
                runStatus=JobStatusClass.SKIPPED,
                config=MySQLConfig().json(),
            ),
        }

        # 2. Set the provider's state_to_commit.
        self.provider.state_to_commit = job_reports

        # 3. Perform the commit
        # NOTE: This will commit the state to the in-memory self.mcps_emitted because of the monkey-patching.
        self.provider.commit()
        self.assertTrue(self.provider.committed)

        # 4. Get last committed state. This must match what has been committed earlier.
        # NOTE: This will retrieve from in-memory self.mcps_emitted because of the monkey-patching.
        last_state: Optional[ReportingJobStatesMap] = self.provider.get_last_state(
            self.job_state_key
        )
        assert last_state is not None
        self.assertEqual(len(last_state), 2)

        # 5. Validate individual job report values that have been committed and retrieved
        # against the original values.
        self.assertEqual(last_state, job_reports)
