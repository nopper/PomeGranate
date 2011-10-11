import os
import os.path

from master import Master, start_mapreduce

class MasterRI(Master):
    def __init__(self, fconf):
        super(MasterRI, self).__init__(fconf)

        self.input_path = self.conf["input-path"]
        self.num_reducer = int(self.conf["num-reducer"])

    def input(self):
        files = os.listdir(self.input_path)

        yield(len(files))

        for id, file in enumerate(files):
            yield(os.path.join(self.input_path, file), id)

    def on_map_finished(self, msg):
        self.num_reducer -= 1

        if self.num_reducer >= 0:
            ret = self.num_reducer
        else:
            ret = None

        return ret

if __name__ == "__main__":
    start_mapreduce(MasterRI)
