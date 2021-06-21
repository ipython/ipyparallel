import warnings

warnings.warn(f"{__name__} is deprecated in ipyparallel 7. Use ipyparallel.engine")

from ipyparallel.engine.app import IPEngine, main

IPEngineApp = IPEngine
launch_new_instance = main

if __name__ == "__main__":
    main()
