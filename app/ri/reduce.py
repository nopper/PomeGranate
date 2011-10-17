import glob
import json
import os.path
import contextlib
import mmap
import struct

from utils import Logger, create_file
from heapq import heappush, heappop, merge

class KeyValueReader(Logger):
    def __init__(self, fname, delete=False, converter=None):
        """
        Create a KeyValueReader instance an object cabable of reading
        <key,value> pairs from an input file.

        The default converter just reads line in the form 'key value\\n'

        @param fname the path of the file containing k,v
        @param delete a boolean indicating whether to delete the file after the
                      scan
        @param converter a function for converting a line input into k,v pairs
        """
        super(KeyValueReader, self).__init__("KeyValueReader")

        self.path = fname
        self.delete = delete
        self.converter = converter or KeyValueReader.conv_key_value

    @staticmethod
    def conv_key_value(line):
        return line.split(' ', 1)

    def iterate(self):
        with open(self.path, 'r') as f:
            with contextlib.closing(mmap.mmap(f.fileno(), 0,
                                    access=mmap.ACCESS_READ)) as m:
                line = m.readline()

                while line:
                    yield (self.converter(line.strip()))
                    line = m.readline()

        if self.delete:
            log.info("Removing file %s" % self.path)
            os.unlink(self.path)

def convert_line(line):
    word, docid, occ = line.split(' ', 2)
    return (word, int(docid), int(occ))

class ReducerRI(Logger):
    def __init__(self, conf):
        super(ReducerRI, self).__init__("ReducerRI")

        self.input_path = conf["map-output"]

    def reduce(self, files):
        handles = []

        self.info("Reducing %s" % (str(files)))

        for fname in files:
            reader = KeyValueReader(
                os.path.join(self.input_path, fname),
                converter=convert_line
            )

            handles.append(reader.iterate())

        # In the case we are reading from two different files something like:
        # file1 -> hello 1 3
        # file2 -> hello 1 2
        # --> hello 1 5

        handle = create_file(self.input_path, 0, "output")

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

    def execute(self, idx):
        expr = os.path.join(self.input_path, "map-r{:06d}-*".format(idx))
        self.reduce(glob.glob(expr))

        self.info("Reduce finished")

        return 1

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
