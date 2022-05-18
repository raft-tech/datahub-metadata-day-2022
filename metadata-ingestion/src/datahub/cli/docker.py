import datetime
import itertools
import logging
import os
import pathlib
import platform
import subprocess
import sys
import tempfile
import time
from typing import List, NoReturn, Optional

import click
import requests

from datahub.cli.docker_check import (
    check_local_docker_containers,
    get_client_with_error,
)
from datahub.ingestion.run.pipeline import Pipeline
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)

NEO4J_AND_ELASTIC_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose.quickstart.yml"
)
ELASTIC_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose-without-neo4j.quickstart.yml"
)
M1_QUICKSTART_COMPOSE_FILE = (
    "docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml"
)

BOOTSTRAP_MCES_FILE = "metadata-ingestion/examples/mce_files/bootstrap_mce.json"

GITHUB_BASE_URL = "https://raw.githubusercontent.com/datahub-project/datahub/master"
GITHUB_NEO4J_AND_ELASTIC_QUICKSTART_COMPOSE_URL = (
    f"{GITHUB_BASE_URL}/{NEO4J_AND_ELASTIC_QUICKSTART_COMPOSE_FILE}"
)
GITHUB_ELASTIC_QUICKSTART_COMPOSE_URL = (
    f"{GITHUB_BASE_URL}/{ELASTIC_QUICKSTART_COMPOSE_FILE}"
)
GITHUB_M1_QUICKSTART_COMPOSE_URL = f"{GITHUB_BASE_URL}/{M1_QUICKSTART_COMPOSE_FILE}"
GITHUB_BOOTSTRAP_MCES_URL = f"{GITHUB_BASE_URL}/{BOOTSTRAP_MCES_FILE}"


@click.group()
def docker() -> None:
    """Helper commands for setting up and interacting with a local
    DataHub instance using Docker."""
    pass


def _print_issue_list_and_exit(
    issues: List[str], header: str, footer: Optional[str] = None
) -> NoReturn:
    click.secho(header, fg="bright_red")
    for issue in issues:
        click.echo(f"- {issue}")
    if footer:
        click.echo()
        click.echo(footer)
    sys.exit(1)


def docker_check_impl() -> None:
    issues = check_local_docker_containers()
    if not issues:
        click.secho("✔ No issues detected", fg="green")
    else:
        _print_issue_list_and_exit(issues, "The following issues were detected:")


@docker.command()
@telemetry.with_telemetry
def check() -> None:
    """Check that the Docker containers are healthy"""
    docker_check_impl()


def is_m1() -> bool:
    """Check whether we are running on an M1 machine"""
    try:
        return (
            platform.uname().machine == "arm64" and platform.uname().system == "Darwin"
        )
    except Exception:
        # Catch-all
        return False


def should_use_neo4j_for_graph_service(graph_service_override: Optional[str]) -> bool:
    if graph_service_override is not None:
        if graph_service_override == "elasticsearch":
            click.echo("Starting with elasticsearch due to graph-service-impl param\n")
            return False
        if graph_service_override == "neo4j":
            click.echo("Starting with neo4j due to graph-service-impl param\n")
            return True
        else:
            click.secho(
                graph_service_override
                + " is not a valid graph service option. Choose either `neo4j` or "
                "`elasticsearch`\n",
                fg="red",
            )
            raise ValueError(f"invalid graph service option: {graph_service_override}")
    with get_client_with_error() as (client, error):
        if error:
            click.secho(
                "Docker doesn't seem to be running. Did you start it?", fg="red"
            )
            raise error

        if len(client.volumes.list(filters={"name": "datahub_neo4jdata"})) > 0:
            click.echo(
                "Datahub Neo4j volume found, starting with neo4j as graph service.\n"
                "If you want to run using elastic, run `datahub docker nuke` and re-ingest your data.\n"
            )
            return True

        click.echo(
            "No Datahub Neo4j volume found, starting with elasticsearch as graph service.\n"
            "To use neo4j as a graph backend, run \n"
            "`datahub docker quickstart --quickstart-compose-file ./docker/quickstart/docker-compose.quickstart.yml`"
            "\nfrom the root of the datahub repo\n"
        )
        return False


@docker.command()
@click.option(
    "--version",
    type=str,
    default=None,
    help="Datahub version to be deployed. If not set, deploy using the defaults from the quickstart compose",
)
@click.option(
    "--build-locally",
    type=bool,
    is_flag=True,
    default=False,
    help="Attempt to build the containers locally before starting",
)
@click.option(
    "--quickstart-compose-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    default=[],
    multiple=True,
    help="Use a local docker-compose file instead of pulling from GitHub",
)
@click.option(
    "--dump-logs-on-failure",
    type=bool,
    is_flag=True,
    default=False,
    help="If true, the docker-compose logs will be printed to console if something fails",
)
@click.option(
    "--graph-service-impl",
    type=str,
    is_flag=False,
    default=None,
    help="If set, forces docker-compose to use that graph service implementation",
)
@telemetry.with_telemetry
def quickstart(
    version: str,
    build_locally: bool,
    quickstart_compose_file: List[pathlib.Path],
    dump_logs_on_failure: bool,
    graph_service_impl: Optional[str],
) -> None:
    """Start an instance of DataHub locally using docker-compose.

    This command will automatically download the latest docker-compose configuration
    from GitHub, pull the latest images, and bring up the DataHub system.
    There are options to override the docker-compose config file, build the containers
    locally, and dump logs to the console or to a file if something goes wrong.
    """

    running_on_m1 = is_m1()
    if running_on_m1:
        click.echo("Detected M1 machine")

    # Run pre-flight checks.
    issues = check_local_docker_containers(preflight_only=True)
    if issues:
        _print_issue_list_and_exit(issues, "Unable to run quickstart:")

    quickstart_compose_file = list(
        quickstart_compose_file
    )  # convert to list from tuple
    if not quickstart_compose_file:
        should_use_neo4j = should_use_neo4j_for_graph_service(graph_service_impl)
        if should_use_neo4j and running_on_m1:
            click.secho(
                "Running with neo4j on M1 is not currently supported, will be using elasticsearch as graph",
                fg="red",
            )
        github_file = (
            GITHUB_NEO4J_AND_ELASTIC_QUICKSTART_COMPOSE_URL
            if should_use_neo4j and not running_on_m1
            else GITHUB_ELASTIC_QUICKSTART_COMPOSE_URL
            if not running_on_m1
            else GITHUB_M1_QUICKSTART_COMPOSE_URL
        )

        with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as tmp_file:
            path = pathlib.Path(tmp_file.name)
            quickstart_compose_file.append(path)
            click.echo(f"Fetching docker-compose file {github_file} from GitHub")
            # Download the quickstart docker-compose file from GitHub.
            quickstart_download_response = requests.get(github_file)
            quickstart_download_response.raise_for_status()
            tmp_file.write(quickstart_download_response.content)
            logger.debug(f"Copied to {path}")

    # set version
    if version is not None:
        os.environ["DATAHUB_VERSION"] = version

    base_command: List[str] = [
        "docker-compose",
        *itertools.chain.from_iterable(
            ("-f", f"{path}") for path in quickstart_compose_file
        ),
        "-p",
        "datahub",
    ]

    # Pull and possibly build the latest containers.
    try:
        subprocess.run(
            [*base_command, "pull"],
            check=True,
        )
    except subprocess.CalledProcessError:
        click.secho(
            "Error while pulling images. Going to attempt to move on to docker-compose up assuming the images have "
            "been built locally",
            fg="red",
        )

    if build_locally:
        subprocess.run(
            [
                *base_command,
                "build",
                "--pull",
            ],
            check=True,
            env={
                **os.environ,
                "DOCKER_BUILDKIT": "1",
            },
        )

    # Start it up! (with retries)
    max_wait_time = datetime.timedelta(minutes=6)
    start_time = datetime.datetime.now()
    sleep_interval = datetime.timedelta(seconds=2)
    up_interval = datetime.timedelta(seconds=30)
    up_attempts = 0
    while (datetime.datetime.now() - start_time) < max_wait_time:
        # Attempt to run docker-compose up every minute.
        if (datetime.datetime.now() - start_time) > up_attempts * up_interval:
            click.echo()
            subprocess.run(base_command + ["up", "-d", "--remove-orphans"])
            up_attempts += 1

        # Check docker health every few seconds.
        issues = check_local_docker_containers()
        if not issues:
            break

        # Wait until next iteration.
        click.echo(".", nl=False)
        time.sleep(sleep_interval.total_seconds())
    else:
        # Falls through if the while loop doesn't exit via break.
        click.echo()
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as log_file:
            ret = subprocess.run(
                base_command + ["logs"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=True,
            )
            log_file.write(ret.stdout)

        if dump_logs_on_failure:
            with open(log_file.name, "r") as logs:
                click.echo("Dumping docker-compose logs:")
                click.echo(logs.read())
                click.echo()

        _print_issue_list_and_exit(
            issues,
            header="Unable to run quickstart - the following issues were detected:",
            footer="If you think something went wrong, please file an issue at https://github.com/datahub-project/datahub/issues\n"
            "or send a message in our Slack https://slack.datahubproject.io/\n"
            f"Be sure to attach the logs from {log_file.name}",
        )

    # Handle success condition.
    click.echo()
    click.secho("✔ DataHub is now running", fg="green")
    click.secho(
        "Ingest some demo data using `datahub docker ingest-sample-data`,\n"
        "or head to http://localhost:9002 (username: datahub, password: datahub) to play around with the frontend.",
        fg="green",
    )
    click.secho(
        "Need support? Get in touch on Slack: https://slack.datahubproject.io/",
        fg="magenta",
    )


@docker.command()
@click.option(
    "--path",
    type=click.Path(exists=True, dir_okay=False),
    help=f"The MCE json file to ingest. Defaults to downloading {BOOTSTRAP_MCES_FILE} from GitHub",
)
@telemetry.with_telemetry
def ingest_sample_data(path: Optional[str]) -> None:
    """Ingest sample data into a running DataHub instance."""

    if path is None:
        click.echo("Downloading sample data...")
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp_file:
            path = str(pathlib.Path(tmp_file.name))

            # Download the bootstrap MCE file from GitHub.
            mce_json_download_response = requests.get(GITHUB_BOOTSTRAP_MCES_URL)
            mce_json_download_response.raise_for_status()
            tmp_file.write(mce_json_download_response.content)
        click.echo(f"Downloaded to {path}")

    # Verify that docker is up.
    issues = check_local_docker_containers()
    if issues:
        _print_issue_list_and_exit(
            issues,
            header="Docker is not ready:",
            footer="Try running `datahub docker quickstart` first",
        )

    # Run ingestion.
    click.echo("Starting ingestion...")
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "file",
                "config": {
                    "filename": path,
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": "http://localhost:8080"},
            },
        }
    )
    pipeline.run()
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


@docker.command()
@telemetry.with_telemetry
@click.option(
    "--keep-data",
    type=bool,
    is_flag=True,
    default=False,
    help="Delete data volumes",
)
def nuke(keep_data: bool) -> None:
    """Remove all Docker containers, networks, and volumes associated with DataHub."""

    with get_client_with_error() as (client, error):
        if error:
            click.secho(
                "Docker doesn't seem to be running. Did you start it?", fg="red"
            )
            return

        click.echo("Removing containers in the datahub project")
        for container in client.containers.list(
            all=True, filters={"label": "com.docker.compose.project=datahub"}
        ):
            container.remove(v=True, force=True)

        if keep_data:
            click.echo("Skipping deleting data volumes in the datahub project")
        else:
            click.echo("Removing volumes in the datahub project")
            for volume in client.volumes.list(
                filters={"label": "com.docker.compose.project=datahub"}
            ):
                volume.remove(force=True)

        click.echo("Removing networks in the datahub project")
        for network in client.networks.list(
            filters={"label": "com.docker.compose.project=datahub"}
        ):
            network.remove()
