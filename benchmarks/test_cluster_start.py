from subprocess import Popen
import time


p  = Popen(['python', 'cluster_start.py', '3', 'loool', '20'])

for i in range(20):
    print('sleeping')
    time.sleep(1)

print('woke up')
p.kill()

for i in range(20):
    print('sleeping again')
    time.sleep(1)

print('woke up')