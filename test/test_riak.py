#! /usr/bin/env python

'''Make sure the Riak client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client


class RiakTest(BaseTest, unittest.TestCase):
    '''Test the Riak client'''
    def make_client(self, name, num_blocks, num_bits):
        return Client('riak', name, num_blocks, num_bits)


if __name__ == '__main__':
    unittest.main()
