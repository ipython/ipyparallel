"""pytest fixtures"""

import pytest
from . import setup, teardown

@pytest.fixture(scope="session")
def cluster(request):
    setup()
    request.addfinalizer(teardown)
