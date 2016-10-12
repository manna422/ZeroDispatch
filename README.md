# ZeroDispatch
## About
ZeroDispatch is a module which uses pyZMQ and the multiprocessing library for [Celery](http://www.celeryproject.org)-like dispatch queues. It is not meant to be a replacement and is not recommended for use in production.

## Installation
There are no exisiting plans to publish this to pypi.
```bash
pip install https://github.com/manna422/ZeroDispatch/zipball/master
```

## Example

```python
from pprint import pprint
from ZeroDispatch import Dispatcher

d = Dispatcher()

@d.run_dispatch
def squarer(a):
    return a ** 2


if __name__ == '__main__':

    for i in xrange(1000):
        # this function will be run in the dispatch queues
        squarer(i)

    results = []
    while len(results) < 1000:
        message = d.result_receiver.recv_pyobj()
        results.append(message)
        pprint(message)

```
