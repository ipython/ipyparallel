import dask
import logging
import math
import os

from tornado import gen

from distributed.deploy.adaptive import Adaptive
from distributed.utils import (
    log_errors,
    ignoring,
    parse_bytes,
    PeriodicCallback,
    format_bytes,
)

logger = logging.getLogger(__name__)


class ClusterManager(object):
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
    4.  jobqueue_worker_spec dict attribute if scale(cores=...) or scale(memory=...)
        can be used by users.
            jobqueue_worker_spec = {'cores': 4, 'memory': '16 GB'}

    This will provide a general ``scale`` method as well as an IPython widget
    for display.

    Things the will need to change for the complete Cluster Manager Design:
    -   ClusterManager:
        - Use it's own event loop, or the notebook one.
        - Connect to a local or remote Scheduler through RPC, and then
          communicate with it.
        - Ability to start a local or remote scheduler.
        - Ability to work with different worker pools: in scale, adaptive,
          jobqueue_worker_spec...
    -   Scheduler
        - Provide some remote methods:
          - retire_workers(n: int): close enough workers ot have only n
            running at the end. Return the closed workers.
          - status of connected worker, e.g. scheduler_info()

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
    >>> cluster.scale(cores=100)               # scale manually to cores nb
    """

    def __init__(self, adaptive_options={}):
        self._target_scale = 0
        self._adaptive_options = adaptive_options
        self._adaptive_options.setdefault("worker_key", self.worker_key)

    def adapt(
        self,
        minimum_cores=None,
        maximum_cores=None,
        minimum_memory=None,
        maximum_memory=None,
        **kwargs
    ):
        """ Turn on adaptivity
        For keyword arguments see dask.distributed.Adaptive
        Instead of minimum and maximum parameters which apply to the number of
        worker, If Cluster object implements jobqueue_worker_spec attribute, one can
        use the following parameters:
        Parameters
        ----------
        minimum_cores: int
            Minimum number of cores for the cluster
        maximum_cores: int
            Maximum number of cores for the cluster
        minimum_memory: str
            Minimum amount of memory for the cluster
        maximum_memory: str
            Maximum amount of memory for the cluster
        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum=10, interval='500ms')
        >>> cluster.adapt(minimum_cores=24, maximum_cores=96)
        >>> cluster.adapt(minimum_memory='60 GB', maximum_memory= '1 TB')
        """
        with ignoring(AttributeError):
            self._adaptive.stop()
        if not hasattr(self, "_adaptive_options"):
            self._adaptive_options = {}
        if "minimum" not in kwargs:
            if minimum_cores is not None:
                kwargs["minimum"] = self._get_nb_workers_from_cores(minimum_cores)
            elif minimum_memory is not None:
                kwargs["minimum"] = self._get_nb_workers_from_memory(minimum_memory)
        if "maximum" not in kwargs:
            if maximum_cores is not None:
                kwargs["maximum"] = self._get_nb_workers_from_cores(maximum_cores)
            elif maximum_memory is not None:
                kwargs["maximum"] = self._get_nb_workers_from_memory(maximum_memory)
        self._adaptive_options.update(kwargs)
        self._adaptive = Adaptive(self.scheduler, self, **self._adaptive_options)
        return self._adaptive

    @property
    def scheduler_address(self):
        return self.scheduler.address

    @property
    def dashboard_link(self):
        template = dask.config.get("distributed.dashboard.link")
        host = self.scheduler.address.split("://")[1].split(":")[0]
        port = self.scheduler.services["bokeh"].port
        return template.format(host=host, port=port, **os.environ)

    @gen.coroutine
    def _scale(self, n=None, cores=None, memory=None):
        """ Asynchronously called scale method

        This allows to do every operation with a coherent context
        """
        with log_errors():
            if [n, cores, memory].count(None) != 2:
                raise ValueError(
                    "One and only one of n, cores, memory kwargs"
                    " should be used, n={}, cores={}, memory={}"
                    " provided.".format(n, cores, memory)
                )
            if n is None:
                if cores is not None:
                    n = self._get_nb_workers_from_cores(cores)
                elif memory is not None:
                    n = self._get_nb_workers_from_memory(memory)

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
                    n=len(self.scheduler.workers) - n, minimum=n, key=self.worker_key
                )
                logger.debug("Closing workers: %s", to_close)
                # Should  be an RPC call here
                yield self.scheduler.retire_workers(workers=to_close)
                # To close may be empty if just asking to remove pending
                # workers, so we should also give a target number
                self.scale_down(to_close, n)
            self._target_scale = n

    def scale(self, n=None, cores=None, memory=None):
        """ Scale cluster to n workers or to the given number of cores or
        memory
        number of cores and memory are converted into number of workers using
        jobqueue_worker_spec attribute.
        Parameters
        ----------
        n: int
            Target number of workers
        cores: int
            Target number of cores
        memory: str
            Target amount of available memory
        Example
        -------
        >>> cluster.scale(10)  # scale cluster to ten workers
        >>> cluster.scale(cores=100) # scale cluster to 100 cores
        >>> cluster.scale(memory='1 TB') # scale cluster to 1 TB memory
        See Also
        --------
        Cluster.scale_up
        Cluster.scale_down
        Cluster.jobqueue_worker_spec
        """
        # TODO we should not rely on scheduler loop here, self should have its
        # own loop
        self.scheduler.loop.add_callback(self._scale, n, cores, memory)

    def _widget_status(self):
        workers = len(self.scheduler.workers)
        cores = sum(ws.ncores for ws in self.scheduler.workers.values())
        memory = sum(ws.memory_limit for ws in self.scheduler.workers.values())
        memory = format_bytes(memory)
        text = """
<div>
  <style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
  </style>
  <table style="text-align: right;">
    <tr><th>Workers</th> <td>%d</td></tr>
    <tr><th>Cores</th> <td>%d</td></tr>
    <tr><th>Memory</th> <td>%s</td></tr>
  </table>
</div>
""" % (
            workers,
            cores,
            memory,
        )
        return text

    def _widget(self):
        """ Create IPython widget for display within a notebook """
        try:
            return self._cached_widget
        except AttributeError:
            pass

        from ipywidgets import (
            Layout,
            VBox,
            HBox,
            IntText,
            Button,
            HTML,
            Accordion,
            Text,
        )

        layout = Layout(width="150px")

        if "bokeh" in self.scheduler.services:
            link = self.dashboard_link
            link = '<p><b>Dashboard: </b><a href="%s" target="_blank">%s</a></p>\n' % (
                link,
                link,
            )
        else:
            link = ""

        title = "<h2>%s</h2>" % type(self).__name__
        title = HTML(title)
        dashboard = HTML(link)

        status = HTML(self._widget_status(), layout=Layout(min_width="150px"))

        request = IntText(0, description="Workers", layout=layout)
        scale = Button(description="Scale", layout=layout)
        request_cores = IntText(0, description="Cores", layout=layout)
        scale_cores = Button(description="Scale", layout=layout)
        request_memory = Text("O GB", description="Memory", layout=layout)
        scale_memory = Button(description="Scale", layout=layout)

        minimum = IntText(0, description="Minimum", layout=layout)
        maximum = IntText(0, description="Maximum", layout=layout)
        adapt = Button(description="Adapt", layout=layout)
        minimum_cores = IntText(0, description="Min cores", layout=layout)
        maximum_cores = IntText(0, description="Max cores", layout=layout)
        adapt_cores = Button(description="Adapt", layout=layout)
        minimum_mem = Text("0 GB", description="Min memory", layout=layout)
        maximum_mem = Text("0 GB", description="Max memory", layout=layout)
        adapt_mem = Button(description="Adapt", layout=layout)

        scale_hbox = [HBox([request, scale])]
        adapt_hbox = [HBox([minimum, maximum, adapt])]
        if hasattr(self, "jobqueue_worker_spec"):
            scale_hbox.append(HBox([request_cores, scale_cores]))
            scale_hbox.append(HBox([request_memory, scale_memory]))
            adapt_hbox.append(HBox([minimum_cores, maximum_cores, adapt_cores]))
            adapt_hbox.append(HBox([minimum_mem, maximum_mem, adapt_mem]))

        accordion = Accordion(
            [VBox(scale_hbox), VBox(adapt_hbox)], layout=Layout(min_width="500px")
        )
        accordion.selected_index = None
        accordion.set_title(0, "Manual Scaling")
        accordion.set_title(1, "Adaptive Scaling")

        box = VBox([title, HBox([status, accordion]), dashboard])

        self._cached_widget = box

        def adapt_cb(b):
            self.adapt(minimum=minimum.value, maximum=maximum.value)

        def adapt_cores_cb(b):
            self.adapt(
                minimum_cores=minimum_cores.value, maximum_cores=maximum_cores.value
            )

        def adapt_mem_cb(b):
            self.adapt(
                minimum_memory=minimum_mem.value, maximum_memory=maximum_mem.value
            )

        adapt.on_click(adapt_cb)
        adapt_cores.on_click(adapt_cores_cb)
        adapt_mem.on_click(adapt_mem_cb)

        def scale_cb(request, kwarg):
            def request_cb(b):
                with log_errors():
                    arg = request.value
                    with ignoring(AttributeError):
                        self._adaptive.stop()
                    local_kwargs = dict()
                    local_kwargs[kwarg] = arg
                    self.scale(**local_kwargs)

            return request_cb

        scale.on_click(scale_cb(request, "n"))
        scale_cores.on_click(scale_cb(request_cores, "cores"))
        scale_memory.on_click(scale_cb(request_memory, "memory"))

        def update():
            status.value = self._widget_status()

        pc = PeriodicCallback(update, 500, io_loop=self.scheduler.loop)
        self.scheduler.periodic_callbacks["cluster-repr"] = pc
        pc.start()

        return box

    def _ipython_display_(self, **kwargs):
        return self._widget()._ipython_display_(**kwargs)

    def worker_key(self, worker_state):
        """ Callable mapping a WorkerState object to a group, see
            Scheduler.workers_to_close
        """
        return worker_state

    def _get_nb_workers_from_cores(self, cores):
        return math.ceil(cores / self.jobqueue_worker_spec["cores"])

    def _get_nb_workers_from_memory(self, memory):
        return math.ceil(
            parse_bytes(memory) / parse_bytes(self.jobqueue_worker_spec["memory"])
        )

    @property
    def jobqueue_worker_spec(self):
        """ single worker process info needed for scaling on cores or memory """
        raise NotImplementedError(
            "{} class does not provide jobqueue_worker_spec "
            "attribute, needed for scaling with "
            "cores or memory kwargs.".format(self.__class__.__name__)
        )
