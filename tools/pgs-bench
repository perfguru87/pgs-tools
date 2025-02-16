#!/usr/bin/python3

# (C) https://github.com/perfguru87/pgs-tools
# Apache-2.0 license

import os
import sys
import time
import logging
try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "lib"))
    from pgs_db import DB
    from pgs_common import configure_logging
except ImportError:
    from pgs_tools.pgs_db import DB
    from pgs_tools.pgs_common import configure_logging

############# Begin of PostgreSQL benchmark code ###########


class PgBench:
    def __init__(self, con):
        self.con = con

    def _loop(self, timeout, query, chunk=None):
        if not chunk:
            chunk = 100
        begin = time.time()
        end = begin + timeout
        loops = 0

        cur = self.con.cursor()

        while time.time() < end:
            for i in range(0, chunk):
                logging.debug(query)
                cur.execute(query)
            loops += chunk

        end = time.time()
        if end == begin:
            return 0
        return int(loops / (end - begin))

    def test(self, testcase, timeout=5, minscore=None, chunk=None):
        score, minscore, msg, metrics = getattr(self, testcase)(timeout, minscore, chunk)

        ret = "%-50s: %5d %s" % (msg, score, metrics)
        ret = "%65s - should be > %d, " % (ret, minscore)
        if score < minscore / 4:
            return False, ret + "VERY SLOW"
        if score < minscore:
            return False, ret + "SLOW"
        if score > minscore * 2:
            return True, ret + "VERY GOOD"
        return True, ret + "GOOD"

    def sequential_select(self, timeout, minscore, chunk):
        if not minscore:
            minscore = 10000
        rate = self._loop(timeout, "SELECT 1", chunk)
        return rate, minscore, "sequential select test 'SELECT 1'", "selects/sec"

    def sequential_commit(self, timeout, minscore, chunk):
        if not minscore:
            minscore = 5000
        cur = self.con.cursor()
        value = 'a' * 255

        table = "postgresql_commit_benchmark"
        cur.execute("DROP TABLE IF EXISTS %s" % table)
        cur.execute("CREATE TABLE %s (test_column varchar(256))" % table)

        rate = self._loop(timeout, "BEGIN; INSERT INTO %s (test_column) VALUES ('%s'); COMMIT" % (table, value), chunk)

        cur.execute("DROP TABLE IF EXISTS %s" % table)

        return rate, minscore, "sequential commit test 'BEGIN; INSERT ...; COMMIT'", "commits/sec"


############# End of PostgreSQL benchmark code ###########

def main():
    from optparse import OptionParser, OptionGroup

    print(os.path.split(sys.argv[0])[1] + " version 1.0")

    test_description = "%prog [options]"

    p = OptionParser(test_description)
    p.add_option("-v", "--verbose", action="store_true", help="enable verbose mode")
    p.add_option("-t", "--testtime", type=int, default=5, help="test time (sec), default is %default")

    DB.add_options(p)

    opts, args = p.parse_args()

    configure_logging(opts.verbose)

    db = DB(opts)
    print("Connecting to %s ..." % str(db))
    con = db.connect()
    pb = PgBench(con)

    statuses = []

    for testcase in ["sequential_select", "sequential_commit"]:
        status, msg = pb.test(testcase, timeout=opts.testtime)
        statuses.append(status)
        print(msg)

    if False in statuses:
        sys.exit(-1)


if __name__ == "__main__":
    main()
