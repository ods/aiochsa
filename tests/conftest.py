import pytest


@pytest.fixture(scope='session')
def dsn(docker_services):
    docker_services.start('clickhouse-server')
    port = docker_services.wait_for_service('clickhouse-server', 8123)
    return f'clickhouse://{docker_services.docker_ip}:{port}'
