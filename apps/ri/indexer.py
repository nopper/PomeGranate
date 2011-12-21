import os
import os.path

from pomegranate.input import Input
from pomegranate.master import Master, start_mapreduce

class InputRI(Input):
    def __init__(self, fconf):
        super(InputRI, self).__init__(fconf, "InputRI")

    def input(self):
        for id, file in enumerate(sorted(os.listdir(self.input_path))):
            yield(os.path.join(self.input_prefix, file), id)

class MasterRI(Master):
    def __init__(self, nick, fconf):
        super(MasterRI, self).__init__(nick, fconf)

        self.num_reducer = int(self.conf["num-reducer"])

    def on_map_finished(self, msg):
        # Here we are pushing self.num_reducer reducer tasks
        self.num_reducer -= 1

        if self.num_reducer >= 0:
            ret = self.num_reducer
        else:
            ret = None

        return ret

Master = MasterRI
Input = InputRI

if __name__ == "__main__":
    start_mapreduce(MasterRI)
