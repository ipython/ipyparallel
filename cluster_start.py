from subprocess import Popen
import sys
import atexit


def main():

    ps = [
        Popen(
            ['ipcontroller', '--debug', '--profile=asv'],
            stdout=sys.stdout,
            stderr=sys.stdout,
            stdin=sys.stdin,
        )
    ]

    for i in range(1):
        ps.append(
            Popen(
                ['ipengine', '--debug', '--profile=asv'],
                stdout=sys.stdout,
                stderr=sys.stdout,
                stdin=sys.stdin,
            )
        )

    def clean_up():
        for p in ps:
            p.kill()
    atexit.register(clean_up)

    for process in ps:
        process.wait()



if __name__ == '__main__':
    main()
