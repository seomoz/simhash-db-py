#! /usr/bin/env python

'''Make sure the Mongo client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client


class MongoTest(BaseTest, unittest.TestCase):
    '''Test the Mongo client'''
    def make_client(self, name, num_blocks, num_bits):
        return Client('mongo', name, num_blocks, num_bits, ['localhost'])


if __name__ == '__main__':
    unittest.main()
