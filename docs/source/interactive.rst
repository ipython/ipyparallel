Interactive Use
===============

Dask-jobqueue is most often used for interactive processing using tools like
IPython or Jupyter notebooks.  This page provides instructions on how to launch
an interactive Jupyter notebook server and Dask dashboard on your HPC system.


Using Jupyter
-------------

It is convenient to run a Jupyter notebook server on the HPC for use with
dask-jobqueue. You may already have a Jupyterhub instance available on your
system, which can be used as is. Otherwise, documentation for starting your own
Jupyter notebook server is available at `Pangeo documentation
<http://pangeo-data.org/setup_guides/hpc.html#configure-jupyter>`_.

Once Jupyter is installed and configured, using a Jupyter notebook is done by:

- Starting a Jupyter notebook server on the HPC (it is often good practice to
  run/submit this as a job to an interactive queue, see Pangeo docs for more
  details).

.. code-block:: bash

   $ jupyter notebook --no-browser --ip=`hostname` --port=8888

- Reading the output of the command above to get the ip or hostname of your
  notebook, and use SSH tunneling on your local machine to access the notebook.
  This must only be done in the probable case where you don't have direct
  access to the notebook URL from your computer browser.

.. code-block:: bash

   $ ssh -N -L 8888:x.x.x.x:8888 username@hpc_domain

Now you can go to ``http://localhost:8888`` on your browser to access the
notebook server.


Viewing the Dask Dashboard
--------------------------

Whether or not you are using dask-jobqueue in Jupyter, IPython or other tools,
at one point you will want to have access to Dask dashboard. Once you've
started a cluster and connected a client to it using commands described in
`Example`_), inspecting ``client`` object will give you the Dashboard URL,
for example ``http://172.16.23.102:8787/status``. The Dask Dashboard may be
accessible by clicking the link displayed, otherwise, you'll have to use SSH
tunneling:

.. code-block:: bash

    # General syntax
    $ ssh -fN your-login@scheduler-ip-address -L port-number:localhost:port-number
    # As applied to this example:
    $ ssh -fN username@172.16.23.102 -L 8787:localhost:8787

Now, you can go to ``http://localhost:8787`` on your browser to view the
dashboard. Note that you can do SSH tunneling for both Jupyter and Dashboard in
one command.

A good example of using Jupyter along with dask-jobqueue and the Dashboard is
availaible below:

.. raw:: html

   <iframe width="560" height="315"
           src="https://www.youtube.com/embed/nH_AQo8WdKw?rel=0"
           frameborder="0" allow="autoplay; encrypted-media"
           allowfullscreen></iframe>


Dask Dashboard with Jupyter
---------------------------

If you are using dask-jobqueue within Jupyter, one user friendly solution to
see the Dashboard is to use `nbserverproxy
<https://github.com/jupyterhub/nbserverproxy>`_. As the dashboard HTTP end
point is launched inside the same node as Jupyter, this is a great solution for
viewing it without having to do SSH tunneling. You just need to install
``nbserverproxy`` in the Python environment you use for launching the notebook,
and activate it as indicated in the docs:

.. code-block:: bash

   pip install nbserverproxy
   jupyter serverextension enable --py nbserverproxy

Then, once started, the dashboard will be accessible from your notebook URL
by adding the path ``/proxy/8787/status``, replacing 8787 by any other
port you use or the dashboard is bind to if needed. Sor for example:

 - ``http://localhost:8888/proxy/8787/status`` with the example above
 - ``http://myjupyterhub.org/user/username/proxy/8787/status`` if using
   JupyterHub

Note that if using Jupyterhub, the service admin should deploy nbserverproxy
on the environment used for starting singleuser notebook, but each user may
have to activate the nbserverproxy extension.

Finally, you may want to update the dashboard link that is displayed in the
notebook, shown from Cluster and Client objects. In order to do this,
edit dask config file, either ``~/.config/dask/jobqueue.yaml`` or
``~/.config/dask/distributed.yaml``, and add the following:

.. code-block:: yaml

   distributed.dashboard.link: "/proxy/{port}/status" # for user launched notebook
   distributed.dashboard.link: "/user/{JUPYTERHUB_USER}/proxy/{port}/status" # for jupyterhub launched notebook
