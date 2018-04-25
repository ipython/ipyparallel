import pytest

from dask_jobqueue import JobQueueCluster


def test_jq_core_placeholder():
    # to test that CI is working
    pass


def test_errors():
    with pytest.raises(NotImplementedError) as info:
        JobQueueCluster()

    assert 'abstract class' in str(info.value)
