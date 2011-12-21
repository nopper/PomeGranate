from fsdfs.filesystem import Filesystem as DFSFilesystem

class Filesystem(DFSFilesystem):
    def __init__(self, config):
        config['replicatorIdleTime'] = 1
        DFSFilesystem.__init__(self, config)

    def getReplicationRules(self, filepath):
        return {"n": 1}
