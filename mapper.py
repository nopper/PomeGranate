"""
This is a really generic interface. Not optimized nor anything. Just avoid
using this in real code. Try to implement an optimized version by your own.
"""

class Mapper(object):
    def __init__(self, conf):
        """
        @param conf is a dictionary containing key,values
        """
        pass

    def map(self, key, value):
        """
        This must be implemented.
        """
        raise Exception("Implement me")

    def execute(self, result):
        """
        The result of this function will be sent through MPI so please for
        christ's sake try to avoid using long messages, or simply override this
        function by providing a more clever function.
        """
        out = []
        key, value = result

        for k, v in self.map(key, value):
            out.append((k, v))

        return out
