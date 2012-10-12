#! /usr/bin/env python

'''The base client, exclusing backends'''

import simhash


class BackendUnsupported(Exception):
    '''An error to throw if the provided backend is unsupported'''
    pass


class BaseClient(object):
    '''The interface that all the clients must support, and a couple helper
    functions'''
    def __init__(self, name, num_blocks, num_bits):
        self.name = name
        self.num_blocks = num_blocks
        self.num_bits = num_bits
        self.corpus = simhash.Corpus(self.num_blocks, self.num_bits)
        self.num_tables = len(self.corpus.tables)

    def ranges(self, hsh):
        '''For a given hash, return a list of all the ranges that have to be
        searched in each of the tables'''
        return [(hsh & t.search_mask, hsh | ((2 ** 64 - 1) ^ t.search_mask))
            for t in self.corpus.tables]

    def permute(self, hsh):
        '''Return all the permutations of the provided hash'''
        return [table.permute(hsh) for table in self.corpus.tables]

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        pass

    def find_one(self, hash_or_hashes):
        '''Find one near-duplicate for the provided query (or queries)'''
        pass

    def find_all(self, hash_or_hashes):
        '''Find all near-duplicates for the provided query (or queries)'''
        pass


def Client(backend, num_blocks, num_bits, *args, **kwargs):
    '''A factory to return the appropriate client'''
    # Import the appropriate module
    if backend == 'cassandra':
        from .cassandra_client import Client as CassClient
        return CassClient(num_blocks, num_bits, *args, **kwargs)
    elif backend == 'mongo':
        from .mongo_client import Client as MongoClient
        return MongoClient(num_blocks, num_bits, *args, **kwargs)
    elif backend == 'riak':
        from .riak_client import Client as RiakClient
        return RiakClient(num_blocks, num_bits, *args, **kwargs)
    else:
        raise BackendUnsupported('The %s backend is not supported' % backend)
