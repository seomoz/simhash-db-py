#! /usr/bin/env python

'''Our code to connect to the MongoDB backend'''

from gevent import monkey
monkey.patch_all()

import struct
import pymongo
from pymongo import common
from . import BaseClient
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser

pymongo.common.VALIDATORS['months'] = pymongo.common.validate_positive_integer


def unsigned_to_signed(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!q', struct.pack('!Q', integer))[0]


def signed_to_unsigned(integer):
    '''Convert an unsigned integer into a signed integer with the same bits'''
    return struct.unpack('!Q', struct.pack('!q', integer))[0]


class Client(BaseClient):
    '''Our Mongo backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)
        self.months = kwargs.pop('months', None)
        self.client = pymongo.Connection(*args, **kwargs)
        self.namePrefix = name + '-'
        if self.months is None:
            self.names = [name]
            self.docsList = [getattr(self.client, name).documents]
        else:
            today = datetime.now()
            self.names = [self.namePrefix
                          + (today -
                             relativedelta(months=i)).strftime(
                                 '%Y-%m')
                          for i in range(self.months)]
            self.docsList = [getattr(self.client, n).documents
                             for n in self.names]

        # Create the indexes (if they exist it's ok)
        for j in range(len(self.names)):
            for i in range(self.num_tables):
                self.docsList[j].create_index(str(i), pymongo.ASCENDING)

    def delete(self):
        '''Delete this database of simhashes'''
        for name in self.names:
            self.client.drop_database(name)

    def delete_old(self):
        '''Delete data that's older than the retention period.'''
        if self.months is None:
            return

        names = self.client.database_names()
        today = datetime.now()
        cutoff = today - relativedelta(months=self.months)
        for name in names:
            if name.startswith(self.namePrefix):
                dbDateString = name[len(self.namePrefix):]
                dbDate = dateutil.parser.parse(dbDateString)
                if dbDate < cutoff:
                    self.client.drop_database(name)

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        # Construct the docs, and then we'll do an insert
        docs = [
            dict((
                str(i),
                unsigned_to_signed(int(self.corpus.tables[i].permute(hsh)))
            ) for i in range(self.num_tables)) for hsh in hashes
        ]
        # And now insert them
        self.docsList[0].insert(docs)

    def find_in_table(self, docs, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        low = unsigned_to_signed(ranges[table_num][0])
        high = unsigned_to_signed(ranges[table_num][1])
        results = docs.find({str(table_num): {
            '$gt': low,
            '$lt': high
        }})
        results = [self.corpus.tables[table_num].unpermute(
            signed_to_unsigned(int(d[str(table_num)]))) for d in results]
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
            for docs in self.docsList:
                for i in range(self.num_tables):
                    found = self.find_in_table(docs, hsh, i, ranges)
                    if found:
                        results.append(found[0])
                        break
                if found:
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
            for docs in self.docsList:
                for i in range(self.num_tables):
                    found.extend(self.find_in_table(docs, hsh, i, ranges))
            found = list(set(found))
            results.append(found)

        if not hasattr(hash_or_hashes, '__iter__'):
            return results[0]
        return results
