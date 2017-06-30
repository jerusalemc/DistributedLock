import subprocess
import time

for idx in range(1, 3):
    result = subprocess.Popen(["./client.py", "--cid", str(idx)])
    out, err = result.communicate()
    # time.sleep(3)


