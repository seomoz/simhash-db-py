#! /usr/bin/env python

'''Our code to connect to the Riak backend'''

import simhash
from . import BaseClient


class Client(BaseClient):
    '''Our in-memory Judy-trie based backend client'''
    def delete(self):
        '''Delete this database of simhashes'''
        self.corpus = simhash.Corpus(self.num_blocks, self.num_bits)

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        if not hasattr(hash_or_hashes, '__iter__'):
            return self.corpus.insert(hash_or_hashes)
        return self.corpus.insert_bulk(hash_or_hashes)

    def find_one(self, hash_or_hashes):
        '''Find one near-duplicate for the provided query (or queries)'''
        if not hasattr(hash_or_hashes, '__iter__'):
            return self.corpus.find_first(hash_or_hashes) or None
        return [i or None for i in self.corpus.find_first_bulk(hash_or_hashes)]

    def find_all(self, hash_or_hashes):
        '''Find all near-duplicates for the provided query (or queries)'''
        if not hasattr(hash_or_hashes, '__iter__'):
            return self.corpus.find_all(hash_or_hashes) or []
        return [i or [] for i in self.corpus.find_all_bulk(hash_or_hashes)]
