import warnings

warnings.warn(f"{__name__} is deprecated in ipyparallel 7. Use ipyparallel.cluster")

from ipyparallel.cluster.app import IPCluster, IPClusterStart, main  # noqa

IPClusterApp = IPCluster
launch_new_instance = main

if __name__ == "__main__":
    main()
