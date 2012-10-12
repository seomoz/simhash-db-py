#! /usr/bin/env python

'''Make sure the Riak client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client

class RiakTest(BaseTest, unittest.TestCase):
    '''Test the Riak client'''
    def setUp(self):
        self.client = Client('riak', 'testing', 6, 3)

    def tearDown(self):
        self.client.delete()

if __name__ == '__main__':
    unittest.main()
