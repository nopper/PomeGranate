import time
import subprocess
from pomegranate.utils import get_id
from pomegranate.mapper import Mapper as BaseMapper

class MapperRI(BaseMapper):
    def __init__(self, conf):
        super(MapperRI, self).__init__(conf, "MapperRI")

        self.map_exec = conf["map-executable"]
        self.num_reducer = conf["num-reducer"]
        self.limit_size = int(conf["limit-size"])

        self.info("Limit size is %d" % self.limit_size)

    def execute(self, inp):
        archive, archiveid = self.vfs.pull_remote_file(inp)

        args = [self.map_exec,
                str(self.vfs.master_id), str(self.vfs.worker_id),
                str(self.num_reducer), archive, self.output_path,
                str(self.limit_size)]

        self.info("Processing archive ID=%d name=%s" % (archiveid, archive))
        self.info("Executing %s" % str(args))

        totsize = 0
        start = time.time()

        process = subprocess.Popen(args, shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        results = []
        filenames = []

        for line in process.stdout.readlines():
            if not line.startswith("=> "):
                continue

            fname, rid, fsize = line[3:].split(' ', 2)
            rid = int(rid)
            fsize = int(fsize)

            totsize += fsize
            filenames.append(fname)
            results.append((rid, get_id(fname), fsize))

        self.info("Map finished. Result is %s" % str(results))

        for fname in filenames:
            self.vfs.push_local_file(fname)

        return ((totsize, time.time() - start), results)

Mapper = MapperRI
