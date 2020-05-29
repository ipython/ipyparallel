import os
import shutil
import socket
import sys
import re
import psutil

import pytest

import dask

from distributed.security import Security
from distributed import Client

from dask_jobqueue import (
    JobQueueCluster,
    PBSCluster,
    MoabCluster,
    SLURMCluster,
    SGECluster,
    LSFCluster,
    OARCluster,
    HTCondorCluster,
)
from dask_jobqueue.core import Job
from dask_jobqueue.local import LocalCluster

from dask_jobqueue.sge import SGEJob

all_clusters = [
    PBSCluster,
    MoabCluster,
    SLURMCluster,
    SGECluster,
    LSFCluster,
    OARCluster,
    HTCondorCluster,
]


def create_cluster_func(cluster_cls, **kwargs):
    if cluster_cls is HTCondorCluster:
        # HTCondorCluster has a mandatory disk argument
        kwargs.setdefault("disk", "1GB")
    return cluster_cls(**kwargs)


def test_errors():
    match = re.compile("Job type.*job_cls", flags=re.DOTALL)
    with pytest.raises(ValueError, match=match):
        JobQueueCluster(cores=4)


def test_command_template():
    with PBSCluster(cores=2, memory="4GB") as cluster:
        assert (
            "%s -m distributed.cli.dask_worker" % (sys.executable)
            in cluster._dummy_job._command_template
        )
        assert " --nthreads 1" in cluster._dummy_job._command_template
        assert " --memory-limit " in cluster._dummy_job._command_template
        assert " --name " in cluster._dummy_job._command_template

    with PBSCluster(
        cores=2,
        memory="4GB",
        death_timeout=60,
        local_directory="/scratch",
        extra=["--preload", "mymodule"],
    ) as cluster:
        assert " --death-timeout 60" in cluster._dummy_job._command_template
        assert " --local-directory /scratch" in cluster._dummy_job._command_template
        assert " --preload mymodule" in cluster._dummy_job._command_template


@pytest.mark.parametrize("Cluster", all_clusters)
def test_shebang_settings(Cluster):
    if Cluster is HTCondorCluster:
        pytest.skip(
            "HTCondorCluster has a peculiar submit script and does not have a shebang"
        )
    default_shebang = "#!/usr/bin/env bash"
    python_shebang = "#!/usr/bin/python"
    with create_cluster_func(
        Cluster, cores=2, memory="4GB", shebang=python_shebang
    ) as cluster:
        job_script = cluster.job_script()
        assert job_script.startswith(python_shebang)
        assert "bash" not in job_script
    with create_cluster_func(Cluster, cores=2, memory="4GB") as cluster:
        job_script = cluster.job_script()
        assert job_script.startswith(default_shebang)


@pytest.mark.parametrize("Cluster", all_clusters)
def test_dashboard_link(Cluster):
    with create_cluster_func(Cluster, cores=1, memory="1GB") as cluster:
        assert re.match(r"http://\d+\.\d+\.\d+.\d+:\d+/status", cluster.dashboard_link)


def test_forward_ip():
    ip = "127.0.0.1"
    with PBSCluster(
        walltime="00:02:00",
        processes=4,
        cores=8,
        memory="28GB",
        name="dask-worker",
        scheduler_options={"host": ip},
    ) as cluster:
        assert cluster.scheduler.ip == ip

    default_ip = socket.gethostbyname("")
    with PBSCluster(
        walltime="00:02:00", processes=4, cores=8, memory="28GB", name="dask-worker"
    ) as cluster:
        assert cluster.scheduler.ip == default_ip


@pytest.mark.parametrize("Cluster", [])
@pytest.mark.parametrize(
    "qsub_return_string",
    [
        "{job_id}.admin01",
        "Request {job_id}.asdf was sumbitted to queue: standard.",
        "sbatch: Submitted batch job {job_id}",
        "{job_id};cluster",
        "Job <{job_id}> is submitted to default queue <normal>.",
        "{job_id}",
    ],
)
def test_job_id_from_qsub_legacy(Cluster, qsub_return_string):
    original_job_id = "654321"
    qsub_return_string = qsub_return_string.format(job_id=original_job_id)
    with create_cluster_func(Cluster, cores=1, memory="1GB") as cluster:
        assert original_job_id == cluster._job_id_from_submit_output(qsub_return_string)


@pytest.mark.parametrize("job_cls", [SGEJob])
@pytest.mark.parametrize(
    "qsub_return_string",
    [
        "{job_id}.admin01",
        "Request {job_id}.asdf was sumbitted to queue: standard.",
        "sbatch: Submitted batch job {job_id}",
        "{job_id};cluster",
        "Job <{job_id}> is submitted to default queue <normal>.",
        "{job_id}",
    ],
)
def test_job_id_from_qsub(job_cls, qsub_return_string):
    original_job_id = "654321"
    qsub_return_string = qsub_return_string.format(job_id=original_job_id)
    job = job_cls(cores=1, memory="1GB")
    assert original_job_id == job._job_id_from_submit_output(qsub_return_string)


@pytest.mark.parametrize("Cluster", [])
def test_job_id_error_handling_legacy(Cluster):
    # non-matching regexp
    with create_cluster_func(Cluster, cores=1, memory="1GB") as cluster:
        with pytest.raises(ValueError, match="Could not parse job id"):
            return_string = "there is no number here"
            cluster._job_id_from_submit_output(return_string)

    # no job_id named group in the regexp
    with create_cluster_func(Cluster, cores=1, memory="1GB") as cluster:
        with pytest.raises(ValueError, match="You need to use a 'job_id' named group"):
            return_string = "Job <12345> submitted to <normal>."
            cluster.job_id_regexp = r"(\d+)"
            cluster._job_id_from_submit_output(return_string)


@pytest.mark.parametrize("job_cls", [SGEJob])
def test_job_id_error_handling(job_cls):
    # non-matching regexp
    job = job_cls(cores=1, memory="1GB")
    with pytest.raises(ValueError, match="Could not parse job id"):
        return_string = "there is no number here"
        job._job_id_from_submit_output(return_string)

    # no job_id named group in the regexp
    job = job_cls(cores=1, memory="1GB")
    with pytest.raises(ValueError, match="You need to use a 'job_id' named group"):
        return_string = "Job <12345> submitted to <normal>."
        job.job_id_regexp = r"(\d+)"
        job._job_id_from_submit_output(return_string)


def test_log_directory(tmpdir):
    shutil.rmtree(tmpdir.strpath, ignore_errors=True)
    with PBSCluster(cores=1, memory="1GB"):
        assert not os.path.exists(tmpdir.strpath)

    with PBSCluster(cores=1, memory="1GB", log_directory=tmpdir.strpath):
        assert os.path.exists(tmpdir.strpath)


@pytest.mark.skip
def test_jobqueue_cluster_call(tmpdir):
    cluster = PBSCluster(cores=1, memory="1GB")

    path = tmpdir.join("test.py")
    path.write('print("this is the stdout")')

    out = cluster._call([sys.executable, path.strpath])
    assert out == "this is the stdout\n"

    path_with_error = tmpdir.join("non-zero-exit-code.py")
    path_with_error.write('print("this is the stdout")\n1/0')

    match = (
        "Command exited with non-zero exit code.+"
        "Exit code: 1.+"
        "stdout:\nthis is the stdout.+"
        "stderr:.+ZeroDivisionError"
    )

    match = re.compile(match, re.DOTALL)
    with pytest.raises(RuntimeError, match=match):
        cluster._call([sys.executable, path_with_error.strpath])


@pytest.mark.parametrize("Cluster", all_clusters)
def test_cluster_has_cores_and_memory(Cluster):
    base_regex = r"{}.+".format(Cluster.__name__)
    with pytest.raises(ValueError, match=base_regex + r"cores=\d, memory='\d+GB'"):
        create_cluster_func(Cluster)

    with pytest.raises(ValueError, match=base_regex + r"cores=\d, memory='1GB'"):
        create_cluster_func(Cluster, memory="1GB")

    with pytest.raises(ValueError, match=base_regex + r"cores=4, memory='\d+GB'"):
        create_cluster_func(Cluster, cores=4)


@pytest.mark.asyncio
async def test_config_interface():
    net_if_addrs = psutil.net_if_addrs()
    interface = list(net_if_addrs.keys())[0]
    with dask.config.set({"jobqueue.local.interface": interface}):
        cluster = LocalCluster(cores=1, memory="2GB", asynchronous=True)
        await cluster
        expected = "'interface': {!r}".format(interface)
        assert expected in str(cluster.scheduler_spec)
        cluster.scale(1)
        assert expected in str(cluster.worker_spec)


# TODO where to put these tests
def test_job_without_config_name():
    class MyJob(Job):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    with pytest.raises(ValueError, match="config_name.+MyJob"):
        MyJob(cores=1, memory="1GB")

    class MyJobWithNoneConfigName(MyJob):
        config_name = None

    with pytest.raises(ValueError, match="config_name.+MyJobWithNoneConfigName"):
        MyJobWithNoneConfigName(cores=1, memory="1GB")

    with pytest.raises(ValueError, match="config_name.+MyJobWithNoneConfigName"):
        JobQueueCluster(job_cls=MyJobWithNoneConfigName, cores=1, memory="1GB")


def test_cluster_without_job_cls():
    class MyCluster(JobQueueCluster):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    with pytest.raises(ValueError, match="job_cls.+MyCluster"):
        MyCluster(cores=1, memory="1GB")


@pytest.mark.parametrize("Cluster", all_clusters)
def test_default_number_of_worker_processes(Cluster):
    with create_cluster_func(Cluster, cores=4, memory="4GB") as cluster:
        assert " --nprocs 4" in cluster.job_script()
        assert " --nthreads 1" in cluster.job_script()

    with create_cluster_func(Cluster, cores=6, memory="4GB") as cluster:
        assert " --nprocs 3" in cluster.job_script()
        assert " --nthreads 2" in cluster.job_script()


@pytest.mark.parametrize("Cluster", all_clusters)
def test_scheduler_options(Cluster):
    net_if_addrs = psutil.net_if_addrs()
    interface = list(net_if_addrs.keys())[0]
    port = 8804

    with create_cluster_func(
        Cluster,
        cores=1,
        memory="1GB",
        scheduler_options={"interface": interface, "port": port},
    ) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        assert scheduler_options["interface"] == interface
        assert scheduler_options["port"] == port


@pytest.mark.parametrize("Cluster", all_clusters)
def test_scheduler_options_interface(Cluster):
    net_if_addrs = psutil.net_if_addrs()
    scheduler_interface = list(net_if_addrs.keys())[0]
    worker_interface = "worker-interface"
    scheduler_host = socket.gethostname()

    with create_cluster_func(
        Cluster, cores=1, memory="1GB", interface=scheduler_interface
    ) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        worker_options = cluster.new_spec["options"]
        assert scheduler_options["interface"] == scheduler_interface
        assert worker_options["interface"] == scheduler_interface

    with create_cluster_func(
        Cluster,
        cores=1,
        memory="1GB",
        interface=worker_interface,
        scheduler_options={"interface": scheduler_interface},
    ) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        worker_options = cluster.new_spec["options"]
        assert scheduler_options["interface"] == scheduler_interface
        assert worker_options["interface"] == worker_interface

    with create_cluster_func(
        Cluster,
        cores=1,
        memory="1GB",
        interface=worker_interface,
        scheduler_options={"host": scheduler_host},
    ) as cluster:
        scheduler_options = cluster.scheduler_spec["options"]
        assert scheduler_options.get("interface") is None
        assert scheduler_options["host"] == scheduler_host
        assert worker_options["interface"] == worker_interface


@pytest.mark.parametrize("Cluster", all_clusters)
def test_cluster_error_scheduler_arguments_should_use_scheduler_options(Cluster):
    scheduler_host = socket.gethostname()
    message_template = "pass {!r} through 'scheduler_options'"

    message = message_template.format("host")
    with pytest.raises(ValueError, match=message):
        with create_cluster_func(Cluster, cores=1, memory="1GB", host=scheduler_host):
            pass

    message = message_template.format("dashboard_address")
    with pytest.raises(ValueError, match=message):
        with create_cluster_func(
            Cluster, cores=1, memory="1GB", dashboard_address=":8787"
        ):
            pass


@pytest.mark.parametrize("Cluster", all_clusters)
def test_import_scheduler_options_from_config(Cluster):

    net_if_addrs = psutil.net_if_addrs()

    config_scheduler_interface = list(net_if_addrs.keys())[0]
    config_scheduler_port = 8804

    pass_scheduler_interface = list(net_if_addrs.keys())[1]

    scheduler_options = {
        "interface": config_scheduler_interface,
        "port": config_scheduler_port,
    }

    default_config_name = Cluster.job_cls.config_name

    with dask.config.set(
        {"jobqueue.%s.scheduler-options" % default_config_name: scheduler_options}
    ):

        with create_cluster_func(Cluster, cores=2, memory="2GB") as cluster:
            scheduler_options = cluster.scheduler_spec["options"]
            assert scheduler_options.get("interface") == config_scheduler_interface
            assert scheduler_options.get("port") == config_scheduler_port

        with create_cluster_func(
            Cluster,
            cores=2,
            memory="2GB",
            scheduler_options={"interface": pass_scheduler_interface},
        ) as cluster:
            scheduler_options = cluster.scheduler_spec["options"]
            assert scheduler_options.get("interface") == pass_scheduler_interface
            assert scheduler_options.get("port") is None


@pytest.mark.parametrize("Cluster", all_clusters)
def test_wrong_parameter_error(Cluster):
    match = re.compile(
        "unexpected keyword argument.+wrong_parameter.+"
        "{}.+job_kwargs.+cores.+memory.+"
        "wrong_parameter.+wrong_parameter_value".format(Cluster.__name__),
        re.DOTALL,
    )
    with pytest.raises(ValueError, match=match):
        create_cluster_func(
            Cluster, cores=1, memory="1GB", wrong_parameter="wrong_parameter_value",
        )


def test_security():
    dirname = os.path.dirname(__file__)
    key = os.path.join(dirname, "key.pem")
    cert = os.path.join(dirname, "ca.pem")
    security = Security(
        tls_ca_file=cert,
        tls_scheduler_key=key,
        tls_scheduler_cert=cert,
        tls_worker_key=key,
        tls_worker_cert=cert,
        tls_client_key=key,
        tls_client_cert=cert,
        require_encryption=True,
    )

    with LocalCluster(
        cores=1, memory="1GB", security=security, protocol="tls"
    ) as cluster:
        assert cluster.security == security
        assert cluster.scheduler_spec["options"]["security"] == security
        job_script = cluster.job_script()
        assert "--tls-key {}".format(key) in job_script
        assert "--tls-cert {}".format(cert) in job_script
        assert "--tls-ca-file {}".format(cert) in job_script

        cluster.scale(jobs=1)
        with Client(cluster, security=security) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11
