#! /usr/bin/env python

'''Our code to connect to the Riak backend'''

import pymongo
from . import BaseClient


class Client(BaseClient):
    '''Our Mongo backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)
        self.client = pymongo.Connection(*args, **kwargs)
        self.name = name
        self.docs = getattr(self.client, name).documents

        # Create the indexes (if they exist it's ok)
        for i in range(self.num_tables):
            self.docs.create_index(str(i), pymongo.ASCENDING)

    def delete(self):
        '''Delete this database of simhashes'''
        self.client.drop_database(self.name)

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        # Construct the docs, and then we'll do an insert
        docs = [
            dict((str(i), int(self.corpus.tables[i].permute(hsh)))
                for i in range(self.num_tables)) for hsh in hashes
        ]
        # And now insert them
        self.docs.insert(docs)

    def find_in_table(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        results = self.docs.find({str(table_num): {
            '$gt': ranges[table_num][0],
            '$lt': ranges[table_num][1]
        }})
        results = [self.corpus.tables[table_num].unpermute(
            int(d[str(table_num)])) for d in results]
        return [h for h in results if
            self.corpus.distance(h, hsh) < self.num_bits]

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
                found.extend(self.find_in_table(hsh, i, ranges))
            found = list(set(found))
            results.append(found)

        if not hasattr(hash_or_hashes, '__iter__'):
            return results[0]
        return results
