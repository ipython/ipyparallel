from .pbs import PBSJob, PBSCluster


class MoabJob(PBSJob):
    submit_command = "msub"
    cancel_command = "canceljob"
    scheduler_name = "moab"


class MoabCluster(PBSCluster):
    __doc__ = PBSCluster.__doc__.replace("PBSCluster", "MoabCluster")
    job_cls = MoabJob
