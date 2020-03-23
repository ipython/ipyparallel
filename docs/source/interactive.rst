Interactive Use
===============

Dask-jobqueue is most often used for interactive processing using tools like
IPython or Jupyter notebooks.  This page provides instructions on how to launch
an interactive Jupyter notebook server and Dask dashboard on your HPC system.

We recommend first doing these steps from a login node (nothing will be
computationally intensive) but at some point you may want to shift to a compute
or interactive node.

*Note: We also recommend the `JupyterHub <https://jupyter.org/hub>`_ project,
which allows HPC administrators to offer and control the process described
in this document automatically.  If you find this process valuable but tedious,
then you may want to ask your system administrators to support it with
JupyterHub.*

Install JupyterLab
------------------

We recommend using JupyterLab, and the `Dask JupyterLab Extension
<https://github.com/dask/dask-labextension>`_.  This will make it easy to get
Dask's dashboard through your Jupyter session.

These can be installed with the following steps:

.. code-block:: bash

   # Install JupyterLab and NodeJS (which we'll need to integrate Dask into JLab)
   conda install jupyterlab nodejs -c conda-forge -y

   # Install server-side pieces of the Dask-JupyterLab extension
   pip install dask_labextension

   # Integrate Dask-Labextension with Jupyter (requires NodeJS)
   jupyter labextension install dask-labextension

You can also use ``pip`` rather than ``conda``, but you will have to find some
other way to install NodeJS.

.. image:: https://github.com/dask/dask-labextension/raw/master/dask.png
   :width: 50%
   :alt: Dask JupyterLab Dashboard

Add A Password
--------------

For security, we recommend adding a password to your Jupyter notebook
configuration.

.. code-block:: bash

   jupyter notebook password

This is good both for security, and also means that you won't have to copy
around Jupyter tokens.


Start Jupyter
-------------

When you use Jupyter on your laptop you often just write ``jupyter notebook``
or ``jupyter lab``.  However, things are a bit different when starting a
notebook server on a separate machine.  As a first step, the following will
work:

.. code-block:: bash

   jupyter lab --no-browser --ip="*" --port 8888

Later, once we get SSH tunneling set up, you may want to come back and specify
a specific IP address or hostname for added security.


SSH Tunneling
-------------

If your personal machine is on the same network as your cluster, then you can
ignore this step.

If you are on a different network (like your home network), and have to SSH in,
then it can be difficult to have your local web browser connect to the Jupyter
server running on the HPC machine.  If your institution doesn't have something
like `JupyterHub <https://jupyter.org/hub>`_ set up, then the easiest way to
accomplish this is to use SSH tunneling.

Often a command like the following works:

.. code-block:: bash

   ssh -L 8888:login-node-hostname:8888 username@hpc.agency.gov

Where ``login-node-hostname`` and ``username@hpc.agency.gov`` are placeholders
that you need to fill in:

-  ``login-node-hostname`` is the name of the node from which you are
    running your Jupyter server, designated ``hostname`` below.

    .. code-block:: bash

      username@hostname$ jupyter lab --no-browser --ip="*" --port 8888

    You might also run ``echo $HOSTNAME`` on that machine as well to see the
    host name.

-   ``hpc.agency.gov`` is the address that you usually use to
    ssh into the cluster.

So in a real example this might look like the following:

.. code-block:: bash

    alice@login2.summit $ jupyter lab --no-browser --ip="login2" --port 8888
    alice@laptop        $ ssh -L 8888:login2:8888 alice@summit.olcf.ornl.gov

Additionally, if port ``8888`` is busy then you may want to choose a different
port, like ``9999``.  Someone else may be using this port, particularly if they
are setting up their own Jupyter server on this machine.

You can now visit ``http://localhost:8888`` on your local browser to access the
Jupyter server.


Viewing the Dask Dashboard
--------------------------

When you start a Dask Jobqueue cluster you also start a Dask dashboard.  This
dashboard is valuable to help you understand the state of your computation and
cluster.

Typically, the dashboard is served on a separate port from Jupyter, and so can
be used whether you choose to use Jupyter or not. If you want to open up a
connection to see the dashboard you can do so with SSH Tunneling as described
above. The dashboard's default port is at ``8787``, and is configurable by
using the ``scheduler_options`` parameter in the Dask Jobqueue cluster object.
For example ``scheduler_options={'dashboard_address': ':12435'}`` would use
12435 for the web dasboard port.

However, Jupyter is also able to proxy the dashboard connection through the
Jupyter server, allowing you to access the dashboard at
``http://localhost:8888/proxy/8787/status``.  This requires no additional SSH
tunneling.  Additionally, if you place this address into the Dask Labextension
search bar (click the Dask logo icon on the left side of your Jupyter session)
then you can access the plots directly within Jupyter Lab, rather than open up
another tab.

Configuration
-------------

Finally, you may want to update the dashboard link that is displayed in the
notebook, shown from Cluster and Client objects. In order to do this,
edit dask config file, either ``~/.config/dask/jobqueue.yaml`` or
``~/.config/dask/distributed.yaml``, and add the following:

.. code-block:: yaml

   distributed.dashboard.link: "/proxy/{port}/status" # for user launched notebook
   distributed.dashboard.link: "/user/{JUPYTERHUB_USER}/proxy/{port}/status" # for jupyterhub launched notebook
