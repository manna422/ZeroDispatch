import os
import marshal
import types

from datetime import datetime
from pprint import pprint

import zmq

class Worker(object):
    def __init__(self):
        self.pid = os.getpid()
        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.connect('tcp://localhost:9001')
        ack_sender = context.socket(zmq.PUSH)
        ack_sender.connect('tcp://localhost:9002')
        result_sender = context.socket(zmq.PUSH)
        result_sender.connect('tcp://localhost:9003')

        self.running = True

        while self.running:
            received = receiver.recv_pyobj()

            # send ack
            ack_message = {
                'message': 'ack',
                'worker':self.pid,
                'time':datetime.now(),
                'task_id':received['task_id']
            }
            ack_sender.send_pyobj(ack_message)

            func_code = marshal.loads(received['func_code'])
            func_name = received['func_name']
            func_args = received['func_args']
            func_kwargs = received['func_kwargs']

            worker_func = types.FunctionType(func_code, globals(), func_name)
            result = worker_func(
                *func_args,
                **func_kwargs
            )


            # send result of task
            result_message = {
                'message': 'result',
                'worker':self.pid,
                'task_id':received['task_id'],
                'time':datetime.now(),
                'result':result
            }
            pprint(result_message)
            result_sender.send_pyobj(result_message)



if __name__ == '__main__':
    Worker()
