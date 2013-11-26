#! /usr/bin/env python

'''Base Tests'''

import unittest
from simhash_db import Client


class BaseTest(object):
    def setUp(self):
        self.client = self.make_client('testing', 6, 3)

    def tearDown(self):
        self.client.delete()

    def test_basic(self):
        '''Make sure we can use the thing in the most basic way'''
        # Insert a single item
        self.client.insert(1)

        # When giving a single query, we should get a single response back.
        # If we provide a list, then we should get a list back
        self.assertEqual(self.client.find_one(1), 1)
        self.assertEqual(self.client.find_one([1]), [1])

        # If we ask for a set of items that do not have near-duplicates, we
        # should get None's back
        self.assertEqual(self.client.find_one(31), None)
        self.assertEqual(self.client.find_one([31]), [None])

        # And now we should insert an array and expect to see all the results
        # in there
        self.client.insert([2, 4, 8, 16, 32])

        # We should see a fair number of these
        self.assertEqual(set(self.client.find_all(1)), set([
            1, 2, 4, 8, 16, 32]))
        self.assertEqual(set(self.client.find_all([1])[0]), set([
            1, 2, 4, 8, 16, 32]))

    def test_random(self):
        '''Test this out with a random sample'''
        import random
        samples = [random.randint(0, 2 ** 60) for i in range(100)]
        self.client.insert(samples)

        # Now our queries, flip one bit in each of our samples
        queries = [(1 << random.randint(0, 60)) ^ s for s in samples]
        results = self.client.find_all(queries)
        for result in results:
            self.assertTrue(len(result) >= 1)

    def test_delete(self):
        '''Test out that we can in fact delete the database and have it be
        completely gone'''
        self.client.insert(1)
        self.assertEqual(self.client.find_one(1), 1)

        # Delete
        self.client.delete()

        # And make sure we find nothing
        self.assertEqual(self.client.find_one(1), None)

    def test_exact(self):
        '''Make sure that we can support exact match'''
        self.client.delete()
        self.client = self.make_client('testing', 1, 0)

        hashes = [1, 2, 4, 8, 16, 32]
        self.client.insert(hashes)
        for hsh in hashes:
            self.assertEqual(set(self.client.find_all(hsh)), set([hsh]))
