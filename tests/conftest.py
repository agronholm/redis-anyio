from typing import cast

import pytest
from _pytest.fixtures import SubRequest


@pytest.fixture(
    params=[
        pytest.param(6379, id="redis6"),
        pytest.param(6380, id="redis7"),
    ]
)
def redis_port(request: SubRequest) -> int:
    return cast(int, request.param)


@pytest.fixture
def redis7_port() -> int:
    return 6380
