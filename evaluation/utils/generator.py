import numpy as np
import uuid

from utils.messages_pb2 import Read, Update, Transfer


class OperationGenerator():
    def __init__(self, record_count, distribution, read, update, transfer):
        self.record_count = record_count
        self.distribution = distribution
        if self.distribution == 'zipfian':
            self.zipf_generator = ZipfGenerator(record_count, 0.99)
        self.read = read / 100
        self.update = update / 100
        self.transfer = transfer / 100

    def next_operation(self):
        r = np.random.uniform()
        if r < self.read:
            read = Read()
            id = self.next_id()
            read.id = id
            return 'read', id, read
        elif r < self.read + self.update:
            update = Update()
            id = self.next_id()
            update.id = id
            update.updates["field0"] = str(uuid.uuid4())
            return 'update', id, update
        else:
            transfer = Transfer()
            transfer.incoming_id = self.next_id()
            transfer.outgoing_id = self.next_id()
            while transfer.incoming_id == transfer.outgoing_id:
                transfer.outgoing_id = self.next_id()
            transfer.amount = 1
            return 'transfer', str(uuid.uuid4()), transfer

    def next_id(self):
        if self.record_count == 0:
            return str(uuid.uuid4())
        if self.distribution == "uniform":
            return str(int(np.random.uniform(0, self.record_count)) + 1)
        elif self.distribution == "zipfian":
            return str(self.zipf_generator.next(1)[0])


class ZipfGenerator:
    def __init__(self, n, alpha):
        tmp = np.power(np.arange(1, n+1), -alpha)
        zeta = np.r_[0.0, np.cumsum(tmp)]
        self.dist_map = [x / zeta[-1] for x in zeta]

    def next(self, number_of_samples=1):
        u = np.random.random(number_of_samples)
        v = np.searchsorted(self.dist_map, u)
        return v
