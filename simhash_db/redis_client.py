#! /usr/bin/env python

'''Our code to connect to the Redis backend'''

import redis
import struct
from . import BaseClient
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta
import dateutil.parser


class Client(BaseClient):
    '''Our Redis backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)
        self.months = kwargs.pop('months', None)
        self.weeks = kwargs.pop('weeks', None)
        if (self.months is not None) and (self.weeks is not None):
            raise ValueError

        self.client = redis.Redis(*args, **kwargs)
        self.name_prefix = name + '-'

        if (self.months is None) and (self.weeks is None):
            self.names = [name]
            self.retention_seconds = 0
        elif self.weeks is not None:
            today = datetime.now()
            wd = today.weekday()
            self.names = [self.name_prefix
                          + (today -
                             relativedelta(weeks=i) -
                             relativedelta(days=wd)).strftime(
                                 '%Y-%m-%d')
                          for i in range(self.weeks)]
            self.retention_seconds = self.weeks * 7 * 86400
        else:
            today = datetime.now()
            self.names = [self.name_prefix
                          + (today -
                             relativedelta(months=i)).strftime(
                                 '%Y-%m')
                          for i in range(self.months)]
            self.retention_seconds = self.months * 31 * 86400

        self.expiration_set = False

    def delete(self):
        '''Delete this database of simhashes'''
        for name in self.names:
            for num in range(self.num_tables):
                self.client.delete('%s.%s' % (name, num))

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        if self.retention_seconds > 0 and not self.expiration_set:
            for num in range(self.num_tables):
                name = '%s.%s' % (self.names[0], num)
                if self.client.ttl(name) <= 0:
                    self.client.expire(name, 1000 * self.retention_seconds)
                self.expiration_set = True

        with self.client.pipeline() as pipe:
            for num in range(self.num_tables):
                name = '%s.%s' % (self.names[0], num)
                for hsh in hashes:
                    permuted = self.corpus.tables[num].permute(hsh)
                    pipe.zadd(name, struct.pack('!Q', hsh), permuted)
            pipe.execute()

    def find_in_table(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        low = ranges[table_num][0]
        high = ranges[table_num][1]
        results = []
        for name in self.names:
            table_name = '%s.%s' % (name, table_num)
            results.extend([struct.unpack('!Q', h)[0] for h in
                            self.client.zrangebyscore(table_name, low, high)])

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
