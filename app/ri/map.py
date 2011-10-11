import os
import json

from utils import Logger
from collections import defaultdict

class MapperRI(Logger):
    def __init__(self, conf):
        super(MapperRI, self).__init__("MapperRI")

        self.num_reducer = conf["num-reducer"]
        self.output_path = conf["map-output"]

        self.dict = defaultdict(list)

    def map(self, fname, docid):
        file = open(fname)

        for line in file.readlines():
            for word in line.strip().split(' '):
                yield (word, docid)

    def execute(self, result):
        fname, docid = result

        for k, v in self.map(fname, docid):
            self.dict[k].append(v)


        files = []
        for id in xrange(self.num_reducer):
            path = os.path.join(self.output_path, "part-r%d-%d" % (id, docid))
            files.append(open(path, "w"))


        # Qui potremmo avere una situazione in cui l'intero dizionario non
        # fitti all'interno della memoria. Di conseguenza dovremo flushare piu
        # volte su diversi file per uno stesso intervallo. Di conseguenza il
        # master dovra tenere in considerazione questo fatto andando ad
        # avvisare il reducer in maniera consistente. Esempio:

        # La map non riesce a tenere in memoria i valori per l'intervallo a-f e
        # per un dato archivio in input produce piu file contenenti i risultati
        # i parziali:

        # a-f +--> a-f-000
        #     |--> a-f-001
        #     \--> a-f-002
        # g-n
        # n-z

        # Il risultato dovra quindi essere:
        # [[a-f-000, a-f-001, a-f-002], g-n, n-z]

        # Di conseguenza qui sarebbe piu' pratico avere come nome file:
        # [reduce-r0-0001, reduce-r0-0002, reduce-r0-0003, reduce-r1-0000,
        # reduce-r2-0000]

        # Here we have to sort by keys and write to a temporary file than
        # returin the name of the file.
        for k in sorted(self.dict):
            docids = ','.join(map(str, self.dict[k]))
            files[hash(k) % self.num_reducer].write("%s %s\n" % (k, docids))

        map(lambda x: x.close(), files)
        return 1

Mapper = MapperRI
