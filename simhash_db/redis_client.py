#! /usr/bin/env python

'''Our code to connect to the Riak backend'''

import redis
import struct
from . import BaseClient


class Client(BaseClient):
    '''Our Redis backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)
        self.client = redis.Redis(*args, **kwargs)
        self.name = name

    def delete(self):
        '''Delete this database of simhashes'''
        for num in range(self.num_tables):
            self.client.delete('%s.%s' % (self.name, num))

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        with self.client.pipeline() as pipe:
            for num in range(self.num_tables):
                name = '%s.%s' % (self.name, num)
                for hsh in hashes:
                    permuted = self.corpus.tables[num].permute(hsh)
                    pipe.zadd(name, struct.pack('!Q', hsh), permuted)
            pipe.execute()

    def find_in_table(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        low = ranges[table_num][0]
        high = ranges[table_num][1]
        name = '%s.%s' % (self.name, table_num)
        results = [struct.unpack('!Q', h)[0] for h in
            self.client.zrangebyscore(name, low, high)]
        return [h for h in results if
            self.corpus.distance(h, hsh) <= self.num_bits]

    def find_one(self, hash_or_hashes):
        '''Find one near-duplicate for the provided query (or queries)'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        results = []
        for hsh in hashes:
            ranges = self.ranges(hsh)
            found = []
            for i in range(self.num_tables):
                found = self.find_in_table(hsh, i, ranges)
                if found:
                    results.append(found[0])
                    break
            if not found:
                results.append(None)

        if not hasattr(hash_or_hashes, '__iter__'):
            return results[0]
        return results

    def find_all(self, hash_or_hashes):
        '''Find all near-duplicates for the provided query (or queries)'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        results = []
        for hsh in hashes:
            ranges = self.ranges(hsh)
            found = []
            for i in range(self.num_tables):
                fnd = self.find_in_table(hsh, i, ranges)
                found.extend(fnd)
            found = list(set(found))
            results.append(found)

        if not hasattr(hash_or_hashes, '__iter__'):
            return results[0]
        return results
