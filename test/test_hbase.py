#! /usr/bin/env python

'''Make sure the Hbase client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client


class HbaseTest(BaseTest, unittest.TestCase):
    '''Test the Hbase client'''
    def make_client(self, name, num_blocks, num_bits):
        return Client('hbase', name, num_blocks, num_bits, ['localhost'],
                      ttl=3600)


if __name__ == '__main__':
    unittest.main()
