#! /usr/bin/env python

'''Make sure the Mongo client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client


class JudyTest(BaseTest, unittest.TestCase):
    '''Test the Judy client'''
    def make_client(self, name, num_blocks, num_bits):
        return Client('judy', name, num_blocks, num_bits)


if __name__ == '__main__':
    unittest.main()
