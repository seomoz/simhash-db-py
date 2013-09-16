#! /usr/bin/env python

'''Make sure the Cassandra client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client


class CassandraTest(BaseTest, unittest.TestCase):
    '''Test the Cassandra client'''
    def make_client(self, name, num_blocks, num_bits):
        return Client('cassandra', name, num_blocks, num_bits)


if __name__ == '__main__':
    unittest.main()
