"""
This is a really generic interface. Not optimized nor anything. Just avoid
using this in real code. Try to implement an optimized version by your own.
"""

class Reducer(object):
    def __init__(self, fconf):
        pass

    def reduce(self, key, list):
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
        key, list = result

        for v in self.reduce(key, list):
            out.append(v)

        return out
