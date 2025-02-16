# (C) https://github.com/perfguru87/pgs-tools
# Apache-2.0 license

import sys
import logging
import optparse
import functools
import psycopg2
import datetime

from pgs_db import DB
import pgs_report as R
from pgs_common import configure_logging


def exception_wrapper(function):
    @functools.wraps(function)
    def wrapper(self, *args, **kwargs):
        try:
            exc_info = sys.exc_info()
            return function(self, *args, **kwargs)
        except psycopg2.Error as e:
            exc_type, exc_value, _ = sys.exc_info()
            self.con.cursor().execute("rollback")
            if "raise_exception" in kwargs and kwargs['raise_exception']:
                raise
            if self._exit_on_fail:
                raise
            return self.track_error(["Exception in: %s %s" % (str(*args), str(kwargs)),
                                    "%s: %s" % (exc_type, exc_value)])
    return wrapper


class DBReport:
    def __init__(self):
        self.vermajor_a = 0
        self.vermajor_b = 0
        self.verminor = 0
        self.issuperuser = False
        self.report = None
        self.con = None
        self.now = datetime.datetime.now()

        self._print_sqls = False
        self._formats = []
        self._exit_on_fail = False
        self._errors = []

    def init(self, opts, dbclass=DB):
        configure_logging(opts.verbose)

        db = dbclass(opts)
        print("Connecting to %s ..." % str(db))

        self._print_sqls = True if opts.html else opts.sql
        if opts.html:
            self._formats.append((R.FORMAT_HTML, sys.stdout if opts.html == "-" else open(opts.html, 'w')))
        if opts.json:
            self._formats.append((R.FORMAT_JSON, sys.stdout if opts.json == "-" else open(opts.json, 'w')))
        self._exit_on_fail = opts.exit_on_fail

        self.con = db.connect(track_history=self._print_sqls)

        self.report = R.Report(width=R.HTML_WIDTH if opts.html else None)

        self.vermajor_a = self.con.db.vermajor_a
        self.vermajor_b = self.con.db.vermajor_b
        self.verminor = self.con.db.verminor
        self.issuperuser = self.execute_fetchval("select usesuper from pg_user where usename = CURRENT_USER")

        rows = []
        rows.append(self.execute_fetchval("SELECT version()"))
        rows.append(self.execute_fetchval("""
                                          SELECT 'Current database server time: ' ||
                                          to_char(now(), 'yyyy-mm-dd hh24:mi:ss')
                                          """))
        rows.append(self.execute_fetchval("""
                                          SELECT 'Database server start time: ' ||
                                          to_char(pg_postmaster_start_time(), 'yyyy-mm-dd hh24:mi:ss') || ' ('
                                          || date_trunc('second', now()) -
                                          date_trunc('second',pg_postmaster_start_time()) || ')'
                                          """))
        self.add_table("PostgreSQL summary", None, rows)

    def flush(self):
        formats = self._formats if len(self._formats) else [(R.FORMAT_TEXT, sys.stdout)]
        for f, stream in formats:
            self.report.flush(format=f, filestream=stream)

    def track_error(self, err):
        self._errors.append(err)
        logging.warning("\n".join(err))
        return []

    @exception_wrapper
    def execute_fetchval(self, query, raise_exception=False):
        return DB.execute_fetchval(self.con, query)

    @exception_wrapper
    def execute_fetchall(self, query, raise_exception=False):
        return DB.execute_fetchall(self.con, query)

    @exception_wrapper
    def execute_fetchone(self, query, raise_exception=False):
        return DB.execute_fetchone(self.con, query)

    @exception_wrapper
    def execute_fetch(self, query, raise_exception=False):
        return DB.execute_fetch(self.con, query)

    def log_section(self, section):
        if len(self._formats):
            print("  Generating: %s ... OK" % section)

    def add_section(self, section, parent=None):
        self.log_section(section)
        s = R.RSection(section, "SQL time: " if self._print_sqls else "")
        if not parent:
            parent = self.report
        parent.add_node(s)
        return s

    def add_table(self, section, columns, rows, hint=None, col_sep="  ", autowidth=True, left_aligned_cols=None):
        if isinstance(section, R.RSection):
            s = section
        else:
            self.log_section(section)
            s = self.report.add_section(section, "SQL time: " if self._print_sqls else "")

        t = R.RTable(autowidth=autowidth, col_sep=col_sep, left_aligned_cols=left_aligned_cols)
        if columns:
            t.add_header(columns)
        for r in rows:
            t.add_row(r)
        if self._print_sqls:
            s.add_node(R.RSqlQueryList(self.con))
        s.add_node(t)
        if hint:
            s.add_node(R.RText(hint))

        if isinstance(section, str):
            self.flush()

    def finish(self):
        warning = "WARNING: report generated with %d problems" % len(self._errors)
        if len(self._errors):
            s = self.report.add_section("Warnings")
            s.add_node(R.RText(warning))
            if len(self._formats):
                s.add_node(R.RText("\n".join(["\n".join(err) for err in self._errors])))
        self.report.add_node(R.RFooter())
        self.flush()
        if len(self._errors) > 0:
            if len(self._formats):
                print(warning)
            sys.exit(-1)
        if len(self._formats):
            print("Done")

    @staticmethod
    def add_options(option_parser, dbclass=DB):
        dbclass.add_options(option_parser)

        g = optparse.OptionGroup(option_parser, "Report format")
        g.add_option("-v", "--verbose", action="store_true", help="enable verbose format")
        g.add_option("-e", "--exit-on-fail", action="store_true", help="exit on first exception")
        g.add_option("-s", "--sql", action="store_true", help="print SQLs that were used to obtain the stats")
        g.add_option("-w", "--html", type="str", help="save report to HTML file")
        g.add_option("-j", "--json", type="str", help="save report to JSON file")
        g.add_option("-L", "--visible-lines", type=int, default=25, help="number of lines visible in HTML by default (default %default)")
        option_parser.add_option_group(g)
