import warnings

warnings.warn(f"{__name__} is deprecated in ipyparallel 7. Use ipyparallel.controller")

from ipyparallel.controller.app import IPController, main

IPControllerApp = IPController
launch_new_instance = main

if __name__ == "__main__":
    main()
