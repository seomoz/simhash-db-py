#! /usr/bin/env python

'''Make sure the Redis client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client


class RedisTest(BaseTest, unittest.TestCase):
    '''Test the Redis client'''
    def make_client(self, name, num_blocks, num_bits):
        return Client('redis', name, num_blocks, num_bits)


if __name__ == '__main__':
    unittest.main()
