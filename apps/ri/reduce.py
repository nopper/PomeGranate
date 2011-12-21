import time
import subprocess
from pomegranate.utils import get_id
from pomegranate.reducer import Reducer as BaseReducer

class ReducerRI(BaseReducer):
    def __init__(self, conf):
        super(ReducerRI, self).__init__(conf, "ReducerRI")

        self.reduce_exec = conf["reduce-executable"]

    def execute(self, files):
        reduce_idx = files[0]
        files      = files[1]

        args = [self.reduce_exec,
                str(self.vfs.master_id), str(self.vfs.worker_id),
                self.output_path, str(reduce_idx)]

        self.vfs.pull_remote_files(reduce_idx, files)

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
            self.vfs.push_local_file(fname)
            break

        return ((fsize, time.time() - start), results)

Reducer = ReducerRI
