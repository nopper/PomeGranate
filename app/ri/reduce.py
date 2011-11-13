import time
import subprocess
from utils import Logger, get_id

class ReducerRI(Logger):
    def __init__(self, conf):
        super(ReducerRI, self).__init__("ReducerRI")

        self.input_path = conf["map-output"]
        self.reduce_exec = conf["reduce-executable"]

    def execute(self, files):
        reduce_idx = files[0]
        files      = files[1]

        args = [self.reduce_exec, self.input_path, str(reduce_idx)]

        for fid in files:
            args.append(str(fid))

        self.info("Executing %s" % str(args))

        start = time.time()
        process = subprocess.Popen(args, shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        results = files

        for line in process.stdout.readlines():
            if not line.startswith("=> "):
                continue

            fname, fsize = line[3:].split(' ', 1)
            fsize = int(fsize)

            results.insert(0, (get_id(fname), fsize))
            break

        return ((fsize, time.time() - start), results)

Reducer = ReducerRI
