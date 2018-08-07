from __future__ import absolute_import, division, print_function

import pytest

from dask_jobqueue import JobQueueCluster


def test_errors():
    with pytest.raises(NotImplementedError) as info:
        JobQueueCluster(cores=4)

    assert 'abstract class' in str(info.value)


def test_threads_deprecation():
    with pytest.raises(ValueError) as info:
        JobQueueCluster(threads=4)

    assert all(word in str(info.value) for word in ['threads', 'core', 'processes'])
