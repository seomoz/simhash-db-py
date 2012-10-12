#! /usr/bin/env python

'''Our Cassandra client'''

# For the time being, this has not been cleaned up into a real client. This is
# actually just a place for our old benchmarking code to live until we need to
# turn this into a real client

# #! /usr/bin/python2.7

# import os
# import time
# import psutil
# import random
# import struct
# import argparse

# from pycassa.pool import MaximumRetryException

# parser = argparse.ArgumentParser(description='Run benchmarks on Cassandra')
# parser.add_argument('count', type=int,
#     help='How many thousands of keys should be inserted per process')

# args = parser.parse_args()

# #########################################################
# # Generate all the seeds
# #########################################################
# # This is how many child processes we'll fork off
# count = 2 * psutil.NUM_CPUS

# # This is the configuration we'll use for insertions
# seeds = []

# def make_seeds():
#     return [(
#         random.randint(0, 2 ** 64),
#         random.randint(1, 1000)
#     ) for i in range(args.count)]

# for i in range(count):
#     seeds.append(make_seeds())


# #########################################################
# # Child process methods
# #########################################################
# def get_cf():
#     print '    Initializing connection...'
#     from pycassa import ColumnFamily
#     from pycassa.pool import ConnectionPool
#     pool = ConnectionPool('simhash', [
#         '10.6.165.53:9160',
#         '10.68.138.8:9160',
#         '10.50.210.63:9160',
#         '10.193.218.111:9160'])
#     col = ColumnFamily(pool, 'hashes')
#     print '    Completed connection'
#     return col

# def insert(i):
#     col = get_cf()
#     for start, interval in seeds[i]:
#         end = start + (interval * 1000)
#         try:
#             col.batch_insert(dict((struct.pack('!Q', j), {'p': ''})
#                 for j in range(start, end, interval)))
#         except MaximumRetryException:
#             pass

#     print '    Done with insertions'
#     exit(0)

# def read(i):
#     col = get_cf()
#     for start, interval in seeds[i]:
#         end = start + (interval * 100)
#         try:
#             existing = [o for o in col.get_range(
#                 struct.pack('!Q', start), struct.pack('!Q', end))]
#         except MaximumRetryException:
#             pass
    
#     print '    Done with reads'
#     exit(0)

# #########################################################
# # Time the insertion process
# #########################################################
# # Now we'll fork off each of the child processes
# start = -time.time()
# insertions = []
# for i in range(count):
#     pid = os.fork()
#     if pid == 0:
#         insert(i)
#     else:
#         print '    Started %i' % pid

# # Now we just wait for all of our child processes to finish up
# for i in range(count):
#     pid, statues = os.wait()
#     s = start + time.time()
#     insertions.append(s)
#     print '%i finished in %fs' % (pid, s)


# #########################################################
# # Time the scan process
# #########################################################
# # Now we'll fork off each of the child processes to test reading
# start = -time.time()
# for i in range(count):
#     pid = os.fork()
#     if pid == 0:
#         insert(i)
#     else:
#         print '    Started %i' % pid

# # Now we just wait for all of our child processes to finish up
# reads = []
# for i in range(count):
#     pid, statues = os.wait()
#     s = start + time.time()
#     reads.append(s)
#     print '%i finished in %fs' % (pid, s)    

# #########################################################
# # Print summary information
# #########################################################
# count = 1000 * args.count
# print 'Insertion:'
# print '    Times (min / avg / max): %fs / %fs / %fs' % (
#     min(insertions), sum(insertions) / len(insertions), max(insertions))
# print '    Rate  (min / avg / max): %fs / %fs / %fs' % (
#     count / max(insertions), count * len(insertions) / sum(insertions),
#     count / min(insertions))


# print 'Reads:'
# print '    Times (min / avg / max): %fs / %fs / %fs' % (
#     min(reads), sum(reads) / len(reads), max(reads))
# print '    Rate  (min / avg / max): %fs / %fs / %fs' % (
#     count / max(reads), count * len(reads) / sum(reads),
#     count / min(reads))