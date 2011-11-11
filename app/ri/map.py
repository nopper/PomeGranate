import subprocess
from utils import Logger, get_id

class MapperRI(Logger):
    def __init__(self, conf):
        super(MapperRI, self).__init__("MapperRI")

        self.map_exec = conf["map-executable"]
        self.num_reducer = conf["num-reducer"]
        self.output_path = conf["map-output"]
        self.limit_size = int(conf["limit-size"])

        self.info("Limit size is %d" % self.limit_size)

    def execute(self, inp):
        archive, archiveid = inp
        self.info("Processing arhive ID=%d name=%s" % (archiveid, archive))

        args = [self.map_exec,
                self.num_reducer, archive, self.output_path, self.limit_size]

        process = subprocess.Popen(args, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        results = []
        for line in process.stdout.readlines():
            if not line.startswith("=> "):
                continue

            fname, rid, fsize = line[3:].split(' ', 2)
            results.append((int(rid), get_id(fname), int(fsize)))

        self.info("Map finished. Result is %s" % str(results))

        return results

Mapper = MapperRI
