import os
import json
import struct

from utils import Logger, create_file, get_id
from collections import defaultdict

class MapperRI(Logger):
    def __init__(self, conf):
        super(MapperRI, self).__init__("MapperRI")

        self.num_reducer = conf["num-reducer"]
        self.output_path = conf["map-output"]
        self.limit_size = int(conf["limit-size"])
        self.continuation_id = [0, ] * self.num_reducer

        self.info("Limit size is %d" % self.limit_size)

        self.dict = defaultdict(list)

    def map(self, fname, docid):
        file = open(fname)

        for line in file.readlines():
            for word in line.strip().split(' '):
                yield (word, docid)


    def flush_dict(self, main_dict, docid_list):
        handles = []
        num_reducer = self.num_reducer

        for idx in range(num_reducer):
            cont = self.continuation_id[idx]
            handle = create_file(self.output_path, idx)
            handles.append((handle, get_id(handle.name)))

        for term in sorted(main_dict):
            tmpdict = main_dict[term]
            handle = handles[hash(term) % num_reducer][0]

            for docid in docid_list:
                occur = tmpdict.get(docid, None)

                if occur is not None:
                    handle.write(struct.pack("I", len(term)))
                    handle.write(struct.pack("%ds" % len(term), term))
                    handle.write(struct.pack("III", 1, docid, occur))
                    handle.write('\n')

        # Let's close all the opened files and in case some file is empty just
        # delete it
        files = []

        for rid, (handle, fid) in enumerate(handles):
            if handle.tell() == 0:
                handle.close()
                os.unlink(handle.name)
            else:
                handle.close()
                files.append((rid, fid, os.stat(handle.name).st_size))

        # The return list is something like [(rid, fid, fsize)]
        return files

    def execute(self, result):
        self.info("Executing %s" % str(result))
        fname, docid = result

        limit_size = self.limit_size
        docpair_size = struct.calcsize("I") * 2

        # This dictionary is in the form {term1: {docid: occ, ..}, term2: .. }
        main_dict = {}

        # Here we also have an auxiliary list to avoid a second phase of
        # sorting by docid
        prev_docid = -1
        docid_list = []
        word_bytes = 0

        result = []


        for cword, cdocid in self.map(fname, docid):
            doc_dct = main_dict.get(cword, None)

            if doc_dct is None:
                doc_dct = {}
                main_dict[cword] = doc_dct
                word_bytes += len(cword)

            doc_dct[cdocid] = doc_dct.get(cdocid, 0) + 1

            if prev_docid != cdocid:
                docid_list.append(cdocid)
                prev_docid = cdocid
                word_bytes += docpair_size

            if word_bytes >= limit_size:
                result.extend(self.flush_dict(main_dict, docid_list))
                main_dict = defaultdict(defaultdict(int))
                docid_list = []
                prev_docid = -1

        result.extend(self.flush_dict(main_dict, docid_list))


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

        self.info("Map finished. Result is %s" % str(result))
        return result

Mapper = MapperRI
