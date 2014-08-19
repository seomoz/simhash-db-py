#! /usr/bin/env python

'''The base client, exclusing backends'''

import simhash


class GeneralException(Exception):
    '''Anything goes wrong'''
    pass


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
        permutations = self.permute(hsh)
        return [(
            permutations[i] & self.corpus.tables[i].search_mask,
            permutations[i] | (
                (2 ** 64 - 1) ^ self.corpus.tables[i].search_mask)
        ) for i in range(len(permutations))]

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


def Client(backend, name, num_blocks, num_bits, *args, **kwargs):
    '''A factory to return the appropriate client'''
    # Import the appropriate module
    if backend == 'cassandra':
        from .cassandra_client import Client as CassClient
        return CassClient(name, num_blocks, num_bits, *args, **kwargs)
    elif backend == 'mongo':
        from .mongo_client import Client as MongoClient
        return MongoClient(name, num_blocks, num_bits, *args, **kwargs)
    elif backend == 'riak':
        from .riak_client import Client as RiakClient
        return RiakClient(name, num_blocks, num_bits, *args, **kwargs)
    elif backend == 'judy':
        from .judy_client import Client as JudyClient
        return JudyClient(name, num_blocks, num_bits, *args, **kwargs)
    elif backend == 'redis':
        from .redis_client import Client as RedisClient
        return RedisClient(name, num_blocks, num_bits, *args, **kwargs)
    elif backend == 'hbase':
        from .hbase_client import Client as HbaseClient
        return HbaseClient(name, num_blocks, num_bits, *args, **kwargs)
    else:
        raise BackendUnsupported('The %s backend is not supported' % backend)
