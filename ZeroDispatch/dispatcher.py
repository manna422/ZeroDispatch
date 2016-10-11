import marshal
import multiprocessing
from functools import wraps

import zmq

from worker import Worker


class Dispatcher(object):
    def __init__(self):
        self.task_id = 0
        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.bind('tcp://*:9001')

        self.ack_receiver = self.context.socket(zmq.PULL)
        self.ack_receiver.bind('tcp://*:9002')

        self.result_receiver = self.context.socket(zmq.PULL)
        self.result_receiver.bind('tcp://*:9003')

        cpu_core_count = multiprocessing.cpu_count()
        self.workers = set([])
        for i in xrange(cpu_core_count):
            worker = multiprocessing.Process(target=Worker)
            self.workers.add(worker)
            worker.start()


    def __del__(self):
        for worker in self.workers:
            worker.terminate()
            worker.join()

        self.sender.close()
        self.ack_receiver.close()
        self.result_receiver.close()


    def dispatch(self, func):
        def wrapper(*args, **kwargs):
            self._dispatch(func, *args, **kwargs)
        return wrapper



    def _dispatch(self, func, *args, **kwargs):
        self.task_id += 1

        task_dict = {}
        task_dict['task_id'] = self.task_id
        task_dict['func_name'] = marshal.dumps(func.func_name)
        task_dict['func_code'] = marshal.dumps(func.func_code)
        task_dict['func_args'] = args
        task_dict['func_kwargs'] = kwargs
        self.sender.send_pyobj(task_dict)

        response = self.ack_receiver.recv_pyobj()
        if (response['task_id'] != self.task_id
            and response['message'] != 'ack'):
            raise Exception




##########
## TEST ##
##########

d = Dispatcher()

@d.dispatch
def squarer(a):
    from time import sleep
    return a ** 2


if __name__ == '__main__':
    from time import sleep
    from pprint import pprint

    for i in xrange(10000):
        print i
        squarer(i)

    results = []
    while len(results) < 10000:
        message = d.result_receiver.recv_pyobj()
        results.append(message)
        pprint(message)

