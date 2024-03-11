import os

c = get_config()  # noqa

c.Cluster.controller_ip = '0.0.0.0'
c.Cluster.engine_launcher_class = 'SSH'

ssh_key = os.path.join(os.path.dirname(__file__), "id_rsa")
c.Cluster.controller_ip = '0.0.0.0'
c.Cluster.engine_launcher_class = 'SSH'
c.SSHEngineSetLauncher.scp_args = c.SSHLauncher.ssh_args = [
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "StrictHostKeyChecking=no",
    "-i",
    ssh_key,
]
c.SSHEngineSetLauncher.engines = {"ciuser@127.0.0.1:2222": 4}
c.SSHEngineSetLauncher.remote_python = "/opt/conda/bin/python3"
c.SSHEngineSetLauncher.remote_profile_dir = "/home/ciuser/.ipython/profile_default"
c.SSHEngineSetLauncher.engine_args = ['--debug']
