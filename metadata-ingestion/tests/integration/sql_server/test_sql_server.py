import subprocess
import time

import pytest

from tests.test_helpers import mce_helpers
from tests.test_helpers.click_helpers import run_datahub_cmd
from tests.test_helpers.docker_helpers import wait_for_port


@pytest.mark.integration
def test_mssql_ingest(docker_compose_runner, pytestconfig, tmp_path, mock_time):
    test_resources_dir = pytestconfig.rootpath / "tests/integration/sql_server"

    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml", "sql-server"
    ) as docker_services:
        # Wait for SQL Server to be ready. We wait an extra couple seconds, as the port being available
        # does not mean the server is accepting connections.
        # TODO: find a better way to check for liveness.
        wait_for_port(docker_services, "testsqlserver", 1433)
        time.sleep(5)

        # Run the setup.sql file to populate the database.
        docker = "docker"
        command = f"{docker} exec testsqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'test!Password' -d master -i /setup/setup.sql"
        ret = subprocess.run(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        assert ret.returncode == 0

        # Run the metadata ingestion pipeline.
        config_file = (test_resources_dir / "mssql_to_file.yml").resolve()
        run_datahub_cmd(
            ["ingest", "-c", f"{config_file}"], tmp_path=tmp_path, check_result=True
        )

        # Verify the output.
        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=tmp_path / "mssql_mces.json",
            golden_path=test_resources_dir / "mssql_mces_golden.json",
        )
