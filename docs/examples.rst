Example Deployments
===================

Deploying dask-jobqueue on different clusters requires a bit of customization.
Below, we provide a few example deployments:


Example PBS Deployment
----------------------

.. code-block:: python

   from dask_jobqueue import PBSCluster

   cluster = PBSCluster(processes=6, threads=4, memory="16GB")
   cluster.start_workers(10)

   from dask.distributed import Client
   client = Client(cluster)
