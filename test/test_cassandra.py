#! /usr/bin/env python

'''Make sure the Cassandra client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client

class CassandraTest(BaseTest, unittest.TestCase):
    '''Test the Cassandra client'''
    def setUp(self):
        self.client = Client('cassandra', 'testing', 6, 3)

    def tearDown(self):
        self.client.delete()

if __name__ == '__main__':
    unittest.main()
