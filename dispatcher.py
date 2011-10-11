"""
This module holds the definition of WorkDispatcher a class which is responsible
for managing mappers and reducers, and simplify the code.
"""
TYPE_MAP    = 0
TYPE_REDUCE = 1
TYPE_DUMMY  = 2

class WorkerStatus(object):
    """
    Simple placeholder class to have a more readable code.
    """
    def __init__(self, type, state):
        self.type = type
        self.state = state

    def __str__(self):
        if self.type == TYPE_MAP:
            return "<Map working on (%s)>" % (str(self.state))
        if self.type == TYPE_REDUCE:
            return "<Reduce working on (%s)>" % (str(self.state))
        if self.type == TYPE_DUMMY:
            return "<Dummy sleeping %.2f secs>" % (self.state)

    def __repr__(self):
        return str(self)

class WorkDispatcher(object):
    """
    The class holds both a generator generating <key,value> pairs from the
    input-module function and keep also track of the work that the reducers
    must execute.
    """
    STAGE_MAP    = 0
    STAGE_REDUCE = 1
    STAGE_SLEEP  = 2
    STAGE_END    = 3

    def __init__(self, generator):
        self.generator = generator
        self.remaining_map = self.generator.next()
        self.num_reducer = 0

        self.partial = []
        self.stage = WorkDispatcher.STAGE_MAP

    def finished(self):
        return self.generator is None and   \
               self.remaining_map == 0 and \
               self.num_reducer == 0 and   \
               not self.partial

    def push_work(self, work):
        """
        Push a given work to the stack of works to be executed

        Please note that is not only limited to the reduce phase. Indeed the
        partial attribute has not visibility of the instances present in
        itself. Therefore you can easily push a WorkerStatus instance relative
        to a map phase. In this case the instance will have priority and pushed
        directly in the head of the queue.

        @param work a WorkerStatus instance
        """

        if work.type == TYPE_MAP:
            # Try to prioritize the faulty map
            self.partial.insert(0, work)
            self.remaining_map += 1
        else:
            self.partial.append(work)

    def map_finished(self):
        self.remaining_map -= 1

        if self.remaining_map == 0:
            self.stage = WorkDispatcher.STAGE_REDUCE

    def reduce_finished(self):
        self.num_reducer -= 1

    def pop_work(self):
        """
        This method pop a work. The policy is first all mappers
        than all reducers.

        @return a WorkerStatus instance or None if there is nothing left
        """
        if self.stage == WorkDispatcher.STAGE_MAP:
            try:
                data = self.generator.next()
                return WorkerStatus(TYPE_MAP, data)
            except StopIteration:
                self.generator = None

                # This is an implicit barrier
                if self.remaining_map > 0:
                    self.stage = WorkDispatcher.STAGE_SLEEP
                else:
                    self.stage = WorkDispatcher.STAGE_REDUCE

        elif self.stage == WorkDispatcher.STAGE_SLEEP:
            return WorkerStatus(TYPE_DUMMY, 1)

        elif self.stage == WorkDispatcher.STAGE_REDUCE:
            if len(self.partial) == 0:

                if not self.finished():
                    return WorkerStatus(TYPE_DUMMY, 1)
                else:
                    return None

            self.num_reducer += 1
            return self.partial.pop(0)
