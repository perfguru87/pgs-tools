#!/usr/bin/python3

# (C) https://github.com/perfguru87/pgs-tools
# Apache-2.0 license

import os
import sys

VERSION = '0.1'

try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "lib"))
    from pgs_db import DB
    import pgs_report as R
    from pgs_report import RTableCol as C
    from pgs_db_report import DBReport
except ImportError as e:
    from pgs_tools.pgs_db import DB
    import pgs_tools.pgs_report as R
    from pgs_tools.pgs_report import RTableCol as C
    from pgs_tools.pgs_db_report import DBReport

from optparse import OptionParser, OptionGroup
import logging


class PgPs(DBReport):
    def __init__(self, idle_in_trans, idle, query_width):
        DBReport.__init__(self)
        self.idle_in_trans = idle_in_trans if idle_in_trans else False
        self.idle = idle if idle else False
        self.query_width = query_width

    def print_session_stats(self):
        section = "The longest transactions and sessions per database, user and state, sorted by transaction duration"

        if (self.vermajor_a == 9) and (self.vermajor_b <= 1):
            q = """
            select datname, usename,
                   case when waiting then 'true' else 'false' end locked,
                   case current_query when '<IDLE>' then 'idle'
                                      when '<IDLE> in transaction' then 'idle in transaction'
                                      else 'active' end state,
                   count(*),
                   to_char(min(backend_start), 'yyyy-mm-dd hh24:mi:ss') session_start_time_min,
                   date_trunc('second', now()) - date_trunc('second', min(backend_start)) session_duration,
                   to_char(min(xact_start), 'yyyy-mm-dd hh24:mi:ss') transaction_start_time_min,
                   date_trunc('second', now()) - date_trunc('second', min(xact_start)) transaction_duration
              from pg_stat_activity
             where procpid != pg_backend_pid()
             group by datname, usename, locked, state
             order by transaction_duration desc nulls last, session_duration desc
            """
        elif (self.vermajor_a == 9) and (self.vermajor_b <= 5):
            q = """
            select datname, usename,
                   case when waiting then 'true' else 'false' end locked,
                   state, count(*),
                   to_char(min(backend_start), 'yyyy-mm-dd hh24:mi:ss') session_start_time_min,
                   date_trunc('second', now()) - date_trunc('second', min(backend_start)) session_duration,
                   to_char(min(xact_start), 'yyyy-mm-dd hh24:mi:ss') transaction_start_time_min,
                   date_trunc('second', now()) - date_trunc('second', min(xact_start)) transaction_duration
              from pg_stat_activity
             where pid != pg_backend_pid()
             group by datname, usename, locked, state
             order by transaction_duration desc nulls last, session_duration desc
            """
        elif (self.vermajor_a == 9) and (self.vermajor_b > 5) or (self.vermajor_a > 9):
            q = """
            select datname, usename,
                   case when wait_event_type is null then 'false' else 'true' end as locked,
                   state, count(*),
                   to_char(min(backend_start), 'yyyy-mm-dd hh24:mi:ss') session_start_time_min,
                   date_trunc('second', now()) - date_trunc('second', min(backend_start)) session_duration,
                   to_char(min(xact_start), 'yyyy-mm-dd hh24:mi:ss') transaction_start_time_min,
                   date_trunc('second', now()) - date_trunc('second', min(xact_start)) transaction_duration
              from pg_stat_activity
             where pid != pg_backend_pid()
             group by datname, usename, locked, state
             order by transaction_duration desc nulls last, session_duration desc
            """

        ret = self.execute_fetchall(q)

        columns = ['DATABASE', 'USERNAME', 'LOCKED', 'STATE', 'COUNT', 'SESSION START\nTIME MIN',
                   'OLDEST SESSION\nDURATION', 'TX_START_TIME_MIN', 'OLDEST TXN\nDURATION*']

        self.add_table(section, columns, ret)

    def print_session_details(self):
        section = "Sessions [active: True, idle in transaction: %s, idle: %s]" % (self.idle_in_trans, self.idle)

        if (self.vermajor_a == 9) and (self.vermajor_b <= 1):
            if self.idle_in_trans:
                filter = "1 = 1" if self.idle else "current_query != '<IDLE>'"
            else:
                filter = "current_query != '<IDLE> in transaction'" if self.idle else "current_query not like '<IDLE>%'"

            q = """
                select procpid pid, datname, usename, client_addr, to_char(backend_start, 'yyyy-mm-dd hh24:mi:ss') backend_start,
                       date_trunc('second', now() - query_start) query_runtime,
                       case when waiting then 'true' else 'false' end locked,
                       current_query query
                  from pg_stat_activity
                 where procpid != pg_backend_pid()
                   and %s
                 order by datname, current_query, query_runtime desc
            """ % filter
        elif (self.vermajor_a == 9) and (self.vermajor_b == 2):
            if self.idle_in_trans:
                filter = "1 = 1" if self.idle else "state != 'idle'"
            else:
                filter = "state != 'idle in transaction'" if self.idle else "state not in ('idle', 'idle in transaction')"

            q = """
                select pid, datname, usename, client_addr, to_char(backend_start, 'yyyy-mm-dd hh24:mi:ss') backend_start,
                       date_trunc('second', now() - query_start) query_runtime,
                       case when waiting then 'true' else 'false' end locked,
                       query
                  from pg_stat_activity
                 where pid != pg_backend_pid()
                   and %s
                 order by datname, state, query_runtime desc
            """ % filter
        elif (self.vermajor_a == 9) and (self.vermajor_b > 5) or (self.vermajor_a > 9):
            if self.idle_in_trans:
                filter = "1 = 1" if self.idle else "state != 'idle'"
            else:
                filter = "state != 'idle in transaction'" if self.idle else "state not in ('idle', 'idle in transaction')"

            q = """
                select pid, datname, usename, client_addr, to_char(backend_start, 'yyyy-mm-dd hh24:mi:ss') backend_start,
                       date_trunc('second', now() - query_start) query_runtime,
                       case when wait_event_type is null then 'false'
                            else 'true (' || wait_event_type || ')' end as locked,
                       query
                  from pg_stat_activity
                 where pid != pg_backend_pid()
                   and %s
                 order by datname, state, query_runtime desc;
            """ % filter

        ret = self.execute_fetchall(q)

        columns = ['PID', 'DB*', 'USERNAME', 'CLIENT ADDR', 'SESSION START',
                   'QUERY\nRUNTIME***', 'LOCKED', C('QUERY**', width=-self.query_width, wrap=True)]

        self.add_table(section, columns, ret)


def main():
    print("pgs-ps version: %3s" % VERSION)

    test_description = "%prog [options]"

    p = OptionParser(test_description)
    p.add_option("-t", "--idle-in-transaction", action="store_true", help="show idle in transaction sessions")
    p.add_option("-i", "--idle", action="store_true", help="show idle sessions")
    p.add_option("-W", "--width", type=int, default=37, help="num of characters in query text (default %default)")

    DBReport.add_options(p, DB)

    opts, args = p.parse_args()

    pi = PgPs(opts.idle_in_transaction, opts.idle, opts.width)
    pi.init(opts, DB)

    pi.print_session_stats()
    pi.print_session_details()
    # todo: add locks information

    pi.finish()


if __name__ == "__main__":
    main()
