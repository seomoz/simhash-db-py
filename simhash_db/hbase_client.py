#! /usr/bin/env python

'''Our code to connect to the HBase backend. It uses the happybase
package, which depends on the Thrift service that (for now) is
part of HBase.'''

from gevent import monkey
monkey.patch_all()

import struct
import happybase
import Hbase_thrift
from . import BaseClient


def column_name(integer):
    '''Convert an integer to a column name.'''
    return 'f%02d:c' % integer


class Client(BaseClient):
    '''Our HBase backend client'''
    def __init__(self, name, num_blocks, num_bits, *args, **kwargs):
        BaseClient.__init__(self, name, num_blocks, num_bits)

        # Time to live in seconds
        ttl = kwargs.pop('ttl', None)
        if ttl is None:
            raise ValueError

        self.connection = happybase.Connection(**kwargs)
        families = {column_name(i): dict(time_to_live=ttl)
                    for i in range(self.num_tables)}
        try:
            self.connection.create_table(name, families)
        except Hbase_thrift.AlreadyExists:
            pass
        self.table = self.connection.table(name)

    def delete(self):
        '''Delete this database of simhashes'''
        if self.table is not None:
            self.connection.delete_table(self.name, disable=True)
            self.table = None

    def insert(self, hash_or_hashes):
        '''Insert one (or many) hashes into the database'''
        if self.table is None:
            return

        hashes = hash_or_hashes
        if not hasattr(hash_or_hashes, '__iter__'):
            hashes = [hash_or_hashes]

        for hsh in hashes:
            for i in range(self.num_tables):
                row_key = struct.pack('!Q',
                                      long(self.corpus.tables[i].permute(hsh)))
                self.table.put(row_key, {column_name(i): None})

    def find_in_table(self, hsh, table_num, ranges):
        '''Return all the results found in this particular table'''
        low = struct.pack('!Q', ranges[table_num][0])
        high = struct.pack('!Q', ranges[table_num][1])
        pairs = self.table.scan(row_start=low, row_stop=high,
                                columns=[column_name(table_num)])
        results = [struct.unpack('!Q', k)[0] for k, v in pairs]
        results = [self.corpus.tables[table_num].unpermute(d)
                   for d in results]
        return [h for h in results if
                self.corpus.distance(h, hsh) <= self.num_bits]

    def find_one(self, hash_or_hashes):
        '''Find one near-duplicate for the provided query (or queries)'''
        if self.table is None:
            return None

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
        if self.table is None:
            return None

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
