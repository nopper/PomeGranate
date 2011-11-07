import os
import glob
import json
import os.path
import contextlib
import mmap
import struct

from utils import Logger, create_file, get_id
from heapq import heappush, heappop, merge

class ReduceReader(Logger):
    def __init__(self, fname):
        super(ReduceReader, self).__init__("ReduceReader")
        self.path = fname

    def iterate(self):
        with open(self.path, 'rb', 1024 * 1024) as f:
            with contextlib.closing(mmap.mmap(f.fileno(), 0,
                                    access=mmap.ACCESS_READ)) as m:

                fsize = m.size()

                while m.tell() < fsize:
                    termlen = struct.unpack("I", m.read(4))[0]
                    term = struct.unpack("%ds" % (termlen), m.read(termlen))[0]
                    num_tuples = struct.unpack("I", m.read(4))[0]

                    for _ in range(num_tuples):
                        str = m.read(8)
                        docid, occ = struct.unpack("II", str)
                        yield (term, docid, occ)

                    m.read(1)

class ReducerRI(Logger):
    def __init__(self, conf):
        super(ReducerRI, self).__init__("ReducerRI")

        self.input_path = conf["map-output"]

    def reduce(self, reduce_idx, files):
        handles = []

        self.info("Reducing %s" % (str(files)))

        for fname in files:
            reader = ReduceReader(os.path.join(self.input_path, fname))
            handles.append(reader.iterate())

        # In the case we are reading from two different files something like:
        # file1 -> hello 1 3
        # file2 -> hello 1 2
        # --> hello 1 5

        handle = create_file(self.input_path, reduce_idx, "output")

        prev_word = None
        prev_docid = -1
        occur = 0

        num_docs = 0
        position = 0

        for item in merge(*handles):
            word, docid, cocc = item

            same_id = prev_docid == docid
            same_word = prev_word == word

            if same_id and same_word:
                occur += cocc
            elif not same_id and same_word:

                if prev_docid != -1:
                    handle.write(struct.pack("II", prev_docid, occur))

                num_docs += 1
                prev_docid = docid
                occur = cocc
            else:
                # Se definito prev_word dovrei tornare alla testina iniziale e
                # scrivere numero docs

                if position > 0:
                    handle.seek(position, 0)
                    handle.write(struct.pack("I", num_docs))
                    handle.seek(0, 2)

                header = struct.pack("I%dsI" % len(word), len(word), word, 0)

                if handle.tell() == 0:
                    handle.write(header)
                else:
                    handle.write("\n" + header)

                position = handle.tell() - 4

                handle.write(struct.pack("II", docid, cocc))

                prev_word = word
                occur = cocc
                num_docs = 1

        handle.close()
        return (get_id(handle.name), os.stat(handle.name).st_size)

    def execute(self, files):
        reduce_idx = files[0]
        files      = set(files[1])

        inputs = []
        results = []
        expr = os.path.join(self.input_path, "map-r{:06d}-*".format(reduce_idx))

        for fname in glob.glob(expr):
            # TODO: usa get_id?
            fid = int(os.path.basename(fname).split("-", 3)[2][1:])

            print "Reducing for %d: checking if %s is inside %s" % (reduce_idx, fname, str(files))

            if fid in files:
                files.remove(fid)
                inputs.append(fname)
                results.append(fid)

        results.insert(0, self.reduce(reduce_idx, inputs))
        self.info("Reduce finished. Result is %s" % str(results))

        return results

# Reduce logaritmico
# Nel caso in cui il limite di file per processo venga superato sarebbe
# interessante aggiungere la possibilita' di eseguire un reduce logaritmico sui
# dati in input. Abbiamo due opzioni:
# num_reduce = log_<maxfile> (numfile)
# 1) Dare al master la possibilita' di splittare il lavoro di reduce su piu
#    reduce. + Questo come pro avrebbe il fatto che piu reduce possono lavorare
#    in parallelo per creare risultati parziali.
# 2) Dare al reducer l'intelligenza di discernere e a costo di perdere un ciclo
# di lavoro tornare un marcatore del tipo EAGAIN, <num-reduce> al master che
# gentilmente si prendera' briga di pushare nel dispatcher altrettanti reducer
# 3) Tenere l'intelligenza sempre sul reducer pero se non vi e' la possibilita'
# di startare subito una reduce dato il limite di file tornare il marcatore
# (evito di aspettare un giro di reduce), ma perdo tempo nello scambio
# messaggi.

Reducer = ReducerRI
