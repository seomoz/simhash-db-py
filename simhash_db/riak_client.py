#! /usr/bin/env python

'''Our code to connect to the Riak backend'''

import riak
import struct
from . import BaseClient


def pack_as_signed(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!q', struct.pack('!Q', integer))[0]


class Client(BaseClient):
    '''Our Riak backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)
        kwargs['transport_class'] = riak.RiakPbcTransport
        kwargs['port'] = kwargs.get('port', 8087)
        self.client = riak.RiakClient(*args, **kwargs)
        self.bucket = self.client.bucket(name)

    def delete(self):
        '''Delete this database of simhashes'''
        for key in self.bucket.get_keys():
            self.bucket.get_binary(key).delete()

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        for hsh in hashes:
            permutations = self.permute(hsh)
            obj = riak.RiakObject(self.client, self.bucket, str(hsh))
            for i in range(self.num_tables):
                obj.add_index('%s_int' % str(i), permutations[i])
            obj.set_content_type('application/simhash')
            obj.set_encoded_data('')
            obj.store()

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
                found = [int(f) for f in self.bucket.get_index(
                    '%s_int' % str(i), low, high)]
                found = [f for f in found if
                    self.corpus.distance(hsh, f) < self.num_bits]
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
                found.extend([int(f) for f in self.bucket.get_index(
                    '%s_int' % str(i), low, high)])
            found = list(set([f for f in found
                if self.corpus.distance(f, hsh) < self.num_bits]))
            results.append(found)

        if not hasattr(hash_or_hashes, '__iter__'):
            return results[0]
        return results
