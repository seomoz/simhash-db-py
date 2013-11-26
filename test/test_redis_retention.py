#! /usr/bin/env python

'''Make sure the Redis client is sane with retention'''

import unittest
from test import BaseTest
from simhash_db import Client


class RedisTestRetention(BaseTest, unittest.TestCase):
    '''Test the Redis client'''
    def make_client(self, name, num_blocks, num_bits):
        return Client('redis', name, num_blocks, num_bits, weeks=3)


if __name__ == '__main__':
    unittest.main()
