#! /usr/bin/env python

'''Our code to connect to the Riak backend'''

import riak
import struct
from . import BaseClient


class Client(BaseClient):
    '''Our Riak backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)
        kwargs['transport_class'] = riak.RiakPbcTransport
        kwargs['port'] = kwargs.get('port', 8087)
        self.client = riak.RiakClient(*args, **kwargs)
        self.bucket_names = ['%s-%i' % (self.name, i) for i in range(
            self.num_tables)]
        self.buckets = [self.client.bucket(name) for name in self.bucket_names]

    def delete(self):
        '''Delete this database of simhashes'''
        for bucket in self.buckets:
            for key in bucket.get_keys():
                key.delete()

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        for hsh in hashes:
            permutations = self.permute(hsh)
            permutations = [struct.pack('!Q', p) for p in permutations]
            for i in range(self.num_tables):
                self.buckets[i].new_binary(permutations[i], '')

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
                low, high = ranges[i]
                low = struct.pack('!Q', low)
                high = struct.pack('!Q', high)
                bucket = self.bucket_names[i]
                found = self.client.index(bucket, '$key', low, high).run()
                found = [self.corpus.tables[i].unpermute(
                    struct.unpack('!Q', f[0])[0]) for f in found]
                found = [f for f in found if
                    self.corpus.distance(f, hsh) < self.num_bits]
                if found:
                    # If we found /anything/, we should return it immediately
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
                low, high = ranges[i]
                low = struct.pack('!Q', low)
                high = struct.pack('!Q', high)
                bucket = self.bucket_names[i]
                tmp = self.client.index(bucket, '$key', low, high).run()
                tmp = [self.corpus.tables[i].unpermute(
                    struct.unpack('!Q', f[0])[0]) for f in tmp]
            found = list(set([f for f in found
                if self.corpus.distance(f, hsh) < self.num_bits]))
            results.append(found)
        return results
