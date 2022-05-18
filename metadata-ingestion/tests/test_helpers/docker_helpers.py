import contextlib
import subprocess
from typing import Optional, Union

import pytest
import pytest_docker.plugin


def is_responsive(container_name: str, port: int, hostname: Optional[str]) -> bool:
    """A cheap way to figure out if a port is responsive on a container"""
    if hostname:
        cmd = f"docker exec {container_name} /bin/bash -c 'echo -n > /dev/tcp/{hostname}/{port}'"
    else:
        # use the hostname of the container
        cmd = f"docker exec {container_name} /bin/bash -c 'c_host=`hostname`;echo -n > /dev/tcp/$c_host/{port}'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


def wait_for_port(
    docker_services: pytest_docker.plugin.Services,
    container_name: str,
    container_port: int,
    hostname: str = None,
    timeout: float = 30.0,
) -> None:
    # import pdb

    # breakpoint()
    try:
        # port = docker_services.port_for(container_name, container_port)
        docker_services.wait_until_responsive(
            timeout=timeout,
            pause=0.5,
            check=lambda: is_responsive(container_name, container_port, hostname),
        )
    finally:
        # use check=True to raise an error if command gave bad exit code
        subprocess.run(f"docker logs {container_name}", shell=True, check=True)


@pytest.fixture(scope="module")
def docker_compose_runner(docker_compose_project_name, docker_cleanup):
    @contextlib.contextmanager
    def run(
        compose_file_path: Union[str, list], key: str
    ) -> pytest_docker.plugin.Services:
        with pytest_docker.plugin.get_docker_services(
            compose_file_path,
            f"{docker_compose_project_name}-{key}",
            docker_cleanup,
        ) as docker_services:
            yield docker_services

    return run
