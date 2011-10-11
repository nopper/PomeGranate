def input():
    yield (len(range(5)))
    for k, v in zip(range(5), range(5)):
        yield (k, v)

def map(k, v):
    return (k, range(v))

def reduce(k, l):
    count = 0
    for i in l:
        count += i
    return (k, count)
