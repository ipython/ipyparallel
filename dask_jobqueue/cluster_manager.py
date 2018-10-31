import logging

from tornado import gen
from distributed.deploy import Cluster
from distributed.utils import log_errors

logger = logging.getLogger(__name__)


class ClusterManager(Cluster):
    """ Intermediate Cluster object that should lead to a real ClusterManager

    This tries to improve upstream Cluster object and underlines needs for
    better decoupling between ClusterManager and Scheduler object

    This currently expects a local Scheduler defined on the object, but should
    eventually only rely on RPC calls on remote or local scheduler.
    It provides common methods and an IPython widget display.

    Clusters inheriting from this class should provide the following:

    1.  A local ``Scheduler`` object at ``.scheduler``. In the future, just
        a URL to local or remote scheduler.
    2.  scale_up and scale_down methods as defined below::

        def scale_up(self, n: int):
            ''' Brings total worker count up to ``n`` '''

        def scale_down(self, workers: List[str], n: int):
            ''' Close the workers with the given addresses or remove pending
                workers to match n running workers.
            '''
    3.  Optionally worker_key: Callable(WorkerState):
            ''' Callable mapping a WorkerState object to a group, see
                Scheduler.workers_to_close
            '''

    This will provide a general ``scale`` method as well as an IPython widget
    for display.

    Things the will need to change for the complete Cluster Manager Design:
    -   ClusterManager:
        - Use it's own event loop, or the notebook one.
        - Connect to a local or remote Scheduler through RPC, and then
          communicate with it.
        - Ability to start a local or remote scheduler.
    -   Scheduler
        - Provide some remote methods:
          - retire_workers(n: int): close enough workers ot have only n
            running at the end. Return the closed workers.

    Examples
    --------

    >>> from distributed.deploy import Cluster
    >>> class MyCluster(cluster):
    ...     def scale_up(self, n):
    ...         ''' Bring the total worker count up to n '''
    ...         pass
    ...     def scale_down(self, workers, n=None):
    ...         ''' Close the workers with the given addresses '''
    ...         pass

    >>> cluster = MyCluster()
    >>> cluster.scale(5)                       # scale manually
    >>> cluster.adapt(minimum=1, maximum=100)  # scale automatically
    """

    def __init__(self, adaptive_options={}):
        self._target_scale = 0
        self._adaptive_options = adaptive_options
        self._adaptive_options.setdefault('worker_key', self.worker_key)

    @gen.coroutine
    def _scale(self, n):
        """ Asynchronously called scale method

        This allows to do every operation with a coherent ocntext
        """
        with log_errors():
            # here we rely on a ClusterManager attribute to retrieve the
            # active and pending workers
            if n == self._target_scale:
                pass
            elif n > self._target_scale:
                self.scale_up(n)
            else:
                # TODO to_close may be empty if some workers are pending
                # This may not be useful to call scheduler methods in this case
                # Scheduler interface here may need to be modified
                to_close = self.scheduler.workers_to_close(
                    n=len(self.scheduler.workers) - n, minimum=n,
                    key=self.worker_key)
                logger.debug("Closing workers: %s", to_close)
                # Should  be an RPC call here
                yield self.scheduler.retire_workers(workers=to_close)
                # To close may be empty if just asking to remove pending
                # workers, so we should also give a target number
                self.scale_down(to_close, n)
            self._target_scale = n

    def scale(self, n):
        """ Scale cluster to n workers

        Parameters
        ----------
        n: int
            Target number of workers

        Example
        -------
        >>> cluster.scale(10)  # scale cluster to ten workers

        See Also
        --------
        Cluster.scale_up
        Cluster.scale_down
        """
        # TODO we should not rely on scheduler loop here, self should have its
        # own loop
        self.scheduler.loop.add_callback(self._scale, n)

    def worker_key(self, worker_state):
        ''' Callable mapping a WorkerState object to a group, see
            Scheduler.workers_to_close
        '''
        return worker_state
