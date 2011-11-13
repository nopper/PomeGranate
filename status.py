import time
from threading import Lock

class ApplicationStatus(object):
    PHASE_MAP    = 0
    PHASE_REDUCE = 1
    PHASE_MERGE  = 2

    def __init__(self):
        self.map_assigned = 0
        self.map_completed = 0
        self.map_faulted = 0

        self.reduce_assigned = 0
        self.reduce_completed = 0
        self.reduce_faulted = 0

        self.map_file = 0
        self.map_file_size = 0

        self.reduce_file = 0
        self.reduce_file_size = 0

        self.start_time = int(time.time())
        self.now_time = self.start_time

        self.faults = 0

        self.phase = 0

        self.amount = 0
        self.samples = 0
        self.graph = []
        self.lastpoint = 0

        self.masters = {}
        self.lastlog = []

    def add_graph_point(self):
        ts = time.time() - self.start_time
        total = (self.reduce_file_size + self.map_file_size)

        if self.lastpoint != total:
            self.graph.append((total / (1024 ** 2 * ts), ts))
            self.lastpoint = total

    def update_master_status(self, nick, status={}):
        empty = self.masters.get(nick, {
            'rtt': 0,
            'proc': 0,
            'finished': (0, 0),
            'ongoing': (0, 0),
            'files': ((0, 0), (0, 0)),
            'status': 'online',
            'avg': 0,
        })

        for key, value in status.items():
            if key in empty:
                empty[key] = value

        self.masters[nick] = empty

    def push_log(self, record):
        self.lastlog.append(record.getMessage())

    def get_last_messages(self):
        return self.lastlog

    def get_average(self):
        ts = time.time() - self.start_time

        if ts > 1:
            total = (self.reduce_file_size + self.map_file_size) / (1024 ** 2)
            return "%.2f" % (total / ts)

        return "N/A"

    def get_processes(self):
        ngroup = len(self.masters)
        nproc = 0

        for nick in self.masters:
            nproc += self.masters[nick]['proc']

        return "%d groups, %d processes" % (ngroup, nproc)

    def get_elapsed(self):
        secs = self.now_time - self.start_time
        mins, secs = divmod(secs, 60)
        hours, mins = divmod(mins, 60)
        return '%02d:%02d:%02d' % (hours, mins, secs)

    def get_phase(self):
        if self.phase == ApplicationStatus.PHASE_MAP: return "Map"
        if self.phase == ApplicationStatus.PHASE_REDUCE: return "Reduce"
        if self.phase == ApplicationStatus.PHASE_MERGE: return "Merge"

    def get_masters(self):
        def get_triple(x):
            return "%d/%d/%d" % (x[0], x[1], x[0] + x[1])

        result = []

        for master, d in sorted(self.masters.items()):
            files = d['files'][1]
            bound = 1024.0 ** 2
            size_str = "%.2f/%.2f/%.2f" % (files[0] / bound,
                                           files[1] / bound,
                                           (files[0] + files[1]) / bound)
            status = d['status']

            if status == 'online':
                status = '<span class="label success">online</span>'
            elif status == 'dead':
                status = '<span class="label error">dead</span>'

            result.append([
                master,
                d['rtt'],
                "%.2f" % (d['avg'] / bound),
                d['proc'],
                get_triple(d['finished']),
                get_triple(d['ongoing']),
                "%s files, %s MBs" % (
                    get_triple(d['files'][0]),
                    size_str),
                status
            ])

        return result

    def get_status_str(self, f, s):
        return "%d files, %.2f MBs" % (f, s / (1024 ** 2))

    def get_map_status(self):
        return self.get_status_str(self.map_file, self.map_file_size)

    def get_reduce_status(self):
        return self.get_status_str(self.reduce_file, self.reduce_file_size)

    def get_total_status(self):
        return self.get_status_str(self.reduce_file + self.map_file,
                                   self.reduce_file_size + self.map_file_size)

    def serialize(self):
        overview = [self.get_elapsed(),
                    self.get_phase(),
                    self.get_map_status(),
                    self.get_reduce_status(),
                    self.get_total_status(),
                    self.get_processes(),
                    self.get_average()]

        inputs = [self.map_assigned,
                  self.reduce_assigned,
                  self.reduce_completed,
                  self.map_completed,
                  self.map_faulted,
                  self.reduce_faulted,
                  self.map_assigned + self.reduce_assigned,
                  self.map_completed + self.reduce_completed,
                  self.map_faulted + self.reduce_faulted]

        return {
            'inputs': inputs,
            'overview': overview,
            'masters': self.get_masters(),
            'lastlog': self.get_last_messages(),
            'graph': self.graph,
        }

class MasterStatus(object):
    """
    This object is just a memento describing the status of the Master
    """
    def __init__(self):
        self._lock = Lock()
        self._nproc = 0

        self._map_finished = 0
        self._map_ongoing = 0

        self._reduce_finished = 0
        self._reduce_ongoing = 0

        self._map_file = 0
        self._map_file_size = 0

        self._reduce_file = 0
        self._reduce_file_size = 0

        self._time = 0
        self._bandwidth = 0

    @property
    def nproc(self):
        with self._lock:
            return self._nproc

    @nproc.setter
    def nproc(self, value):
        with self._lock:
            self._nproc = value

    def increase(self, **kwargs):
        with self._lock:
            for key, value in kwargs.items():
                name = '_' + key
                setattr(self, name, getattr(self, name) + value)

    def serialize(self):
        with self._lock:
            avg = 0

            if self._time > 0:
                avg = (self._bandwidth / self._time)

            return {
                "avg": avg,
                "proc": self._nproc,
                "finished": (self._map_finished, self._reduce_finished),
                "ongoing": (self._map_ongoing, self._reduce_ongoing),
                "files": ((self._map_file, self._reduce_file),
                          (self._map_file_size, self._reduce_file_size))
            }
