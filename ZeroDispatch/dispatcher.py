import marshal
import multiprocessing
from functools import wraps

import zmq


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

        self.workers = set([])

    def dispatch(self, func, *args, **kwargs):
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


def squarer(a):
    from time import sleep
    return a ** 2

if __name__ == '__main__':
    from time import sleep
    from pprint import pprint

    d = Dispatcher()
    for i in xrange(10000):
        d.dispatch(squarer, i)

    results = set([])
    while len(results) < 10000:
        message = d.result_receiver.recv_pyobj()
        results.add(message)
        pprint(message)
        

