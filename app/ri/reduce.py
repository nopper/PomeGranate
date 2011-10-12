import glob
import json
import os.path
from utils import Logger

class ReducerRI(Logger):
    def __init__(self, conf):
        super(ReducerRI, self).__init__("ReducerRI")

        self.input_path = conf["map-output"]

    def reduce(self, key, list):
        pass

    def execute(self, idx):
        self.info("AAAA")
        expr = os.path.join(self.input_path, "part-r%d-*" % idx)
        print glob.glob(expr)

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
