#!/usr/bin/python3

# (C) https://github.com/perfguru87/pgs-tools
# Apache-2.0 license

import os
import sys
import time
import subprocess
import datetime

try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "lib"))
    from pgs_db import DB
    from pgs_common import configure_logging
except ImportError:
    from pgs_tools.pgs_db import DB
    from pgs_tools.pgs_common import configure_logging

from optparse import OptionParser, OptionGroup
import logging

#########################################################################################
# pgs-warmupper.py specific code
#########################################################################################

VERSION = '1.1'
MB = 1024 * 1024
RELWIDTH = 30


def run(cmdline):
    logging.debug("executing: %s" % cmdline)
    p = subprocess.Popen(cmdline, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_text, stderRText = p.communicate()
    p.wait()
    status = p.returncode

    logging.debug("stdout: %s" % stdout_text)
    if stderRText:
        logging.debug("stderr: %s" % stderRText)
    return (status, stdout_text, stderRText)


class Relation:
    type = ""

    def __init__(self, name, warmupper):
        self.name = name.strip().strip("\"")
        self.size = 0
        self.warmupper = warmupper

        self.warmed_up = False

        self.read_size = 0
        self.read_size_delta = 0

    def update_stats(self, size, read_size):
        if size:
            self.size = size
        if self.read_size:
            self.read_size_delta = read_size - self.read_size
        else:
            self.read_size = read_size

    def warmup(self):
        if self.warmed_up:
            return 0

        if not self.size:
            return 0

        filenode = DB.execute_fetchval(self.warmupper.con, """
            select cl.relfilenode
            from pg_class cl
                join pg_namespace nsp on cl.relnamespace = nsp.oid
            WHERE cl.relname = '%s'""" % self.name)

        if not filenode:
            filenode = "unknown"

        size = self.size
        fname = None

        if self.warmupper.data_dir:
            _, fname, _ = run("find %s -name %s" % (self.warmupper.data_dir, filenode))
            if fname:
                fname = fname.strip()
                if os.path.exists(fname):
                    size = os.path.getsize(fname)
                else:
                    fname = None

        name = self.name
        if len(name) > RELWIDTH:
            name = name[0:RELWIDTH - 3] + "..."

        print(self.warmupper.fmt1 %
              (datetime.datetime.now().strftime("%m-%d %H:%M:%S"), self.type, name, filenode, self.read_size_delta / MB, size / MB), end="")
        sys.stdout.flush()

        rate_str = "-/-"
        if fname:
            if self.warmupper.dry_run:
                time_str = "dry run"
                ret = 0
            else:
                t = time.time()
                run("dd if='%s' bs=8M of=/dev/null" % fname)
                t = time.time() - t
                time_str = "%.1f" % t
                if t:
                    rate_str = "%.1f" % ((size / MB) / t)
                ret = size
        else:
            time_str = "skipped"
            ret = 0

        print(self.warmupper.fmt2 % (time_str, rate_str, "%1.f" % ((self.warmupper.total_warmed_size + ret) / MB)))

        self.warmed_up = True
        return ret


INDEXES = {}


class Index(Relation):
    type = "index"

    @staticmethod
    def get(name, warmupper):
        global INDEXES
        if name not in INDEXES:
            INDEXES[name] = Index(name, warmupper)
        return INDEXES[name]


TABLES = {}


class Table(Relation):
    type = "table"

    def __init__(self, name, warmupper):
        Relation.__init__(self, name, warmupper)

        self.indexes = {}

    def alloc_index(self, name, warmupper):
        if name not in self.indexes:
            self.indexes[name] = Index.get(name, warmupper)
        return self.indexes[name]

    @staticmethod
    def get(name, warmupper):
        global TABLES
        if name not in TABLES:
            TABLES[name] = Table(name, warmupper)
        return TABLES[name]


class Warmupper:
    def __init__(self, con, warmup_threshold, dry_run, db_is_local):
        self.con = con
        self.warmup_threshold = warmup_threshold
        self.dry_run = dry_run
        self.db_is_local = db_is_local
        self.warmed_tables = {}
        self.warmed_indexes = {}
        self.total_warmed_size = 0
        self._header_printed = False

        self.blk_size = 0
        self.data_dir = ""
        self.total_size = 0
        self.tables_size = 0
        self.indexes_size = 0

        self.fmt1 = "%%14s %%-6s %%-%ds %%10s %%12s %%12s" % RELWIDTH
        self.fmt2 = "%10s %10s %14s"

        self.init()

    def init(self):
        self.blk_size = int(DB.execute_fetchval(self.con, "SHOW block_size"))
        try:
            self.data_dir = DB.execute_fetchval(self.con, "show data_directory")
        except Exception as e:
            logging.error(e)
            print("WARNING: forcing --dry-run mode, no actual warming up will be executed!!!")
            self.con.commit()
            self.data_dir = None
            self.dry_run = True

        self.total_size, self.tables_size, self.indexes_size = DB.execute_fetchone(self.con, """
            SELECT
                SUM(total), SUM(relation), SUM(indexes)
            FROM
            (
                SELECT
                    schema,
                    sum(pg_total_relation_size(qual_table))::bigint AS total,
                    sum(pg_relation_size(qual_table))::bigint AS relation,
                    sum(pg_indexes_size(qual_table))::bigint AS indexes
                FROM
            (
                SELECT
                    schemaname AS schema,
                    tablename AS table,
                    ('"'||schemaname||'"."'||tablename||'"')::regclass AS qual_table
                FROM
                    pg_tables
                WHERE
                    schemaname NOT LIKE 'pg_%'
            ) s
            GROUP BY schema
            ORDER BY total DESC
        ) s""")

    def get_ram_size(self):
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')

    def print_db_summary(self):
        print("")
        print("DB summary:")
        print("  Data block size:    %d bytes" % self.blk_size)
        print("  Data directory:     %s" % self.data_dir)
        if self.db_is_local:
            print("  Total RAM size:     %1.f MBytes" % (self.get_ram_size() / MB))
        print("  Total DB size:      %1.f MBytes" % (self.total_size / MB))
        print("  - tables size:      %1.f MBytes" % (self.tables_size / MB))
        print("  - indexes size:     %1.f MBytes" % (self.indexes_size / MB))
        print("")

    def update_stats(self):
        rows = DB.execute_fetchall(self.con,
                                   """
            SELECT relname,
                   pg_relation_size(relid) table_size_bytes,
                   heap_blks_read
              FROM pg_statio_user_tables
             where schemaname not like 'pg_temp%'
            """)
        for table, table_size, read_size in rows:
            t = Table.get(table, self)
            t.update_stats(table_size, int(read_size) * self.blk_size)

        rows = DB.execute_fetchall(self.con,
                                   """
            SELECT relname,
                   indexrelname,
                   pg_relation_size(indexrelid) AS index_size_bytes,
                   idx_blks_read
              FROM pg_statio_user_indexes
             where schemaname not like 'pg_temp%'
            """)

        for table, index, index_size, read_size in rows:
            t = Table.get(table, self)
            i = t.alloc_index(index, self)
            i.update_stats(index_size, int(read_size) * self.blk_size)

        self.con.commit()

    def warmup(self, relations=None):
        if not relations:
            relations = []
        _warmup = {}
        for r in relations:
            _warmup[r] = True

        for t in TABLES.values():
            if t.name in _warmup or t.read_size_delta >= self.warmup_threshold:
                self.print_header()
                self.total_warmed_size += t.warmup()
        for i in INDEXES.values():
            if i.name in _warmup or i.read_size_delta >= self.warmup_threshold:
                self.print_header()
                self.total_warmed_size += i.warmup()

    def print_header(self):
        if self._header_printed:
            return
        self._header_printed = True

        self.print_sep_line("=")
        print(self.fmt1 % ("MM-DD HH:MM:SS", " TYPE ", "NAME", "FILENODE", "READ SINCE", " SIZE ON "), end="")
        print(self.fmt2 % ("  WARMUP", " WARMUP", "TOTAL WARMED"))
        print(self.fmt1 % ("              ", "      ", "    ", "        ", "START (MB)", "DISK (MB)"), end="")
        print(self.fmt2 % ("TIME (s)", "   MB/s", "   SIZE (MB)"))
        self.print_sep_line()

    def print_sep_line(self, s="-"):
        print(s * 127)

    def loop(self, delay, count):

        print("Started @ %s\n" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        print("Monitoring the tables/indexes being actively used, highly loaded ones will be warmed up")
        print("Press Ctrl-C to exit anytime...\n")

        if not count:
            count = -1

        while count:
            self.update_stats()
            self.warmup()
            time.sleep(delay)
            if count > 0:
                count -= 1


def main():
    p = OptionParser(usage="usage: %prog [options]", version=VERSION)
    p.add_option("-v", "--verbose", action="store_true", help="enable verbose mode")
    p.add_option("-d", "--delay", type=int, default=2, help="delay between database poll (sec)")
    p.add_option("-n", "--count", type=int, default=0, help="exit after COUNT iterations")
    p.add_option("-r", "--relation", action="append", help="comma separated list of tables or indexes to warmup and exit")
    p.add_option("--dry-run", action="store_true", help="skip actual files warmup")
    p.add_option("-t", "--threshold", type=int, default=1,
                 help="threshold of data read in MegaBytes to trigger warmup procedure (default is %default)")

    DB.add_options(p)

    opts, args = p.parse_args()

    configure_logging(opts.verbose)

    db = DB(opts)
    print("Connecting to %s ..." % str(db))
    con = db.connect()

    relations = []
    if opts.relation:
        for r in opts.relation:
            for x in r.split(","):
                relations.append(x)

    w = Warmupper(con, opts.threshold * MB, opts.dry_run, opts.db_host == "127.0.0.1")

    w.print_db_summary()

    try:
        if opts.relation:
            print("Warming up the tables and indexes passed by -r option ...\n")
            w.print_header()
            w.update_stats()
            w.warmup(relations)
            w.print_sep_line()
            print("Done")
        else:
            w.loop(opts.delay, opts.count)
    except KeyboardInterrupt as e:
        print("")
        w.print_sep_line()


if __name__ == "__main__":
    main()
