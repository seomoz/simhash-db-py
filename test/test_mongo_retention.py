#! /usr/bin/env python

'''Make sure the Mongo client is sane'''

import unittest
from test import BaseTest
from simhash_db import Client


class MongoTest(BaseTest, unittest.TestCase):
    '''Test the Mongo client'''
    def setUp(self):
        self.client = Client('mongo', 'testing', 6, 3, host='localhost',
                             weeks=3)

    def tearDown(self):
        self.client.delete()

if __name__ == '__main__':
    unittest.main()
