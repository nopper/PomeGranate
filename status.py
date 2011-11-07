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

        self.faults = 0

        self.phase = 0
        self.start_time = 0
        self.size_per_sec = 0
        self.eta = 0

        self.masters = {}
        self.lastlog = []

    def update_master_status(self, nick, status={}):
        empty = {
            'rtt': 0,
            'proc': 0,
            'finished': (0, 0),
            'ongoing': (0, 0),
            'files': ((0, 0), (0, 0)),
            'status': 'online',
        }

        for key, value in status.items():
            if key in empty:
                empty[key] = value

        self.masters[nick] = empty

    def push_log(self, record):
        self.lastlog.append(record.getMessage())

    def get_last_messages(self):
        return self.lastlog

    def get_eta(self):
        return "N/A"

    def get_processes(self):
        ngroup = len(self.masters)
        nproc = 0

        for nick in self.masters:
            nproc += self.masters[nick]['proc']

        return "%d groups, %d processes" % (ngroup, nproc)

    def get_percentage(self):
        return "Unknown"

    def get_phase(self):
        if self.phase == ApplicationStatus.PHASE_MAP: return "Map"
        if self.phase == ApplicationStatus.PHASE_REDUCE: return "Reduce"
        if self.phase == ApplicationStatus.PHASE_MERGE: return "Merge"

    def get_masters(self):
        def get_triple(x):
            print x
            return "%d/%d/%d" % (x[0], x[1], x[0] + x[1])

        result = []

        for master, d in sorted(self.masters.items()):
            print d
            files = d['files'][1]
            bound = 1024.0 ** 2
            size_str = "%.2f/%.2f/%.2f" % (files[0] / bound,
                                           files[1] / bound,
                                           (files[0] + files[1]) / bound)
            result.append([
                master,
                d['rtt'],
                d['proc'],
                get_triple(d['finished']),
                get_triple(d['ongoing']),
                "%s files, %s MBs" % (
                    get_triple(d['files'][0]),
                    size_str),
                d['status']
            ])

        return result

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
        return {
            "proc": self._nproc,
            "finished": (self._map_finished, self._reduce_finished),
            "ongoing": (self._map_ongoing, self._reduce_ongoing),
            "files": ((self._map_file, self._reduce_file),
                      (self._map_file_size, self._reduce_file_size))
        }
