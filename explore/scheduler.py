from ipyparallel.apps import ipclusterapp as app


def main():
    app.launch_new_instance(['start', '-n', '16', '--debug', '--profile=asv'])


if __name__ == '__main__':
    main()
