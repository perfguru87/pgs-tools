#!/usr/bin/python3

# TODO: fix bloat -> library

# (C) https://github.com/perfguru87/pgs-tools
# Apache-2.0 license

# TODO
# * reflect somehow 'HINT: Tables with dead tuples > vacuum threshold must be vacuumed'
# * do not start vacuum if table locked/recently accessed
# * do not start vacuum if there are open sessions

import os
import sys
import time
import re

try:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "lib"))
    from pgs_db import DB
    from pgs_common import configure_logging
except ImportError:
    from pgs_tools.db import DB
    from pgs_tools.pgs_common import configure_logging

from optparse import OptionParser, OptionGroup
import logging

opts = None
con = None


def get_yn():
    while True:
        ch = raw_input()
        if ch == 'y' or ch == 'n':
            return ch


def table_name_is_valid(s, search=re.compile(r'[^A-Za-z_]').search):
    return not bool(search(s))


def vacuum(tables):
    pattern = r'[^\.a-z0-9]'

    print("Vacuuming, please wait (it is safe to kill it anytime) ...\n")

    old_isolation_level = con.isolation_level
    con.set_isolation_level(0)

    try:
        for table in tables:

            if not table_name_is_valid(table):
                print("  skipping table '%s' because it has invalid table name" % table)
                continue

            t = time.time()
            print("  vacuuming '%s'..." % table, end="")
            sys.stdout.flush()
            DB.execute(con, "VACUUM FULL \"%s\"" % table)
            print("done in %.1f sec" % (time.time() - t))
    finally:
        con.set_isolation_level(old_isolation_level)

    print("\nDone")


def pg_vacuum(db, con):
    # see: https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql
    q_after_12 = """select Res1.schema,
                       Res1.tablename,
                       Res1.bloat_size::bigint bloat_bytes,
                       pg_size_pretty(Res1.bloat_size::bigint) bloat_human,
                       pg_size_pretty(Res1.tt_size) table_human,
                       pg_size_pretty(Res1.tt_size-Res1.bloat_size::bigint) clear_table_human,
                       case Res1.tt_size when 0
                                         then '0'
                                         else round(Res1.bloat_size::numeric/Res1.tt_size*100, 2) end bloat_perc,
                       Res1.tbltuples,
                       Res1.tt_size table_bytes,
                       to_char(Res1.last_vacuum, 'yyyy-mm-dd hh24:mi') last_vacuum,
                       to_char(Res1.last_autovacuum, 'yyyy-mm-dd hh24:mi') last_autovacuum,
                       to_char(Res1.last_analyze, 'yyyy-mm-dd hh24:mi') last_analyze,
                       to_char(Res1.last_autoanalyze, 'yyyy-mm-dd hh24:mi') last_autoanalyze
                    from (select res0.schema,
                               res0.tablename,
                               pg_table_size(res0.toid) tt_size,
                               pg_total_relation_size(res0.toid) to_size,
                               GREATEST((res0.heappages + res0.toastpages - (ceil(res0.reltuples/
                                  ((res0.bs-res0.page_hdr) * res0.fillfactor/((4 + res0.tpl_hdr_size
                                     + res0.tpl_data_size + (2 * res0.ma)
                                     - CASE WHEN res0.tpl_hdr_size%res0.ma = 0
                                            THEN res0.ma ELSE res0.tpl_hdr_size%res0.ma END
                                     - CASE WHEN ceil(res0.tpl_data_size)::int%res0.ma = 0
                                            THEN res0.ma ELSE ceil(res0.tpl_data_size)::int%res0.ma end)*100)))
                              + ceil(res0.toasttuples/4))) * res0.bs, 0) AS bloat_size,
                              st.last_vacuum,
                              st.last_autovacuum,
                              st.last_analyze,
                              st.last_autoanalyze,
                              res0.reltuples tbltuples
                              from (select tbl.oid toid, ns.nspname as schema,
                                       tbl.relname as tablename,
                                       tbl.reltuples,
                                       tbl.relpages as heappages,
                                       coalesce(substring(array_to_string(tbl.reloptions, ' ')
                                             FROM '%fillfactor=#"__#"%' FOR '#')::smallint, 100) AS fillfactor,
                                       coalesce(toast.relpages, 0) AS toastpages,
                                       coalesce(toast.reltuples, 0) AS toasttuples,
                                       current_setting('block_size')::numeric AS bs,
                                       24 as page_hdr,
                                       CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64'
                                            THEN 8
                                            ELSE 4 END AS ma,
                                       bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na,
                                       23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0
                                                 THEN ( 7 + count(*) ) / 8
                                                 ELSE 0::int END + CASE WHEN bool_or(att.attname = 'oid' and att.attnum < 0) THEN 4 ELSE 0 END AS tpl_hdr_size,
                                       sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size
                                      from pg_class tbl
                                      JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
                                      join pg_attribute as att ON att.attrelid = tbl.oid
                                      LEFT JOIN pg_class AS toast ON toast.oid = tbl.reltoastrelid
                                      left JOIN pg_stats AS s ON s.schemaname=ns.nspname
                                            AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
                                      where tbl.relkind = 'r' and ns.nspname not in ('pg_catalog', 'information_schema')
                                        and att.attnum > 0 AND NOT att.attisdropped
                                      group by tbl.oid, ns.nspname, tbl.relname, tbl.reltuples, tbl.relpages,
                                               fillfactor, toastpages, toasttuples) Res0
                              left join pg_stat_all_tables st on Res0.schema = st.schemaname and Res0.tablename = st.relname) Res1
                    WHERE bloat_size > {{BLOAT_MB}}"""

    # see: https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat-82-84.sql
    q_before_12 = """select Res1.schema,
                       Res1.tablename,
                       Res1.bloat_size::bigint bloat_bytes,
                       pg_size_pretty(Res1.bloat_size::bigint) bloat_human,
                       pg_size_pretty(Res1.tt_size) table_human,
                       pg_size_pretty(Res1.tt_size-Res1.bloat_size::bigint) clear_table_human,
                       case Res1.tt_size when 0
                                         then '0'
                                         else round(Res1.bloat_size::numeric/Res1.tt_size*100, 2) end bloat_perc,
                       Res1.tbltuples,
                       Res1.tt_size table_bytes,
                       to_char(Res1.last_vacuum, 'yyyy-mm-dd hh24:mi') last_vacuum,
                       to_char(Res1.last_autovacuum, 'yyyy-mm-dd hh24:mi') last_autovacuum,
                       to_char(Res1.last_analyze, 'yyyy-mm-dd hh24:mi') last_analyze,
                       to_char(Res1.last_autoanalyze, 'yyyy-mm-dd hh24:mi') last_autoanalyze
                    from (select res0.schema,
                               res0.tablename,
                               pg_table_size(res0.toid) tt_size,
                               pg_total_relation_size(res0.toid) to_size,
                               GREATEST((res0.heappages + res0.toastpages - (ceil(res0.reltuples/
                                  ((res0.bs-res0.page_hdr) * res0.fillfactor/((4 + res0.tpl_hdr_size
                                     + res0.tpl_data_size + (2 * res0.ma)
                                     - CASE WHEN res0.tpl_hdr_size%res0.ma = 0
                                            THEN res0.ma ELSE res0.tpl_hdr_size%res0.ma END
                                     - CASE WHEN ceil(res0.tpl_data_size)::int%res0.ma = 0
                                            THEN res0.ma ELSE ceil(res0.tpl_data_size)::int%res0.ma end)*100)))
                              + ceil(res0.toasttuples/4))) * res0.bs, 0) AS bloat_size,
                              st.last_vacuum,
                              st.last_autovacuum,
                              st.last_analyze,
                              st.last_autoanalyze,
                              res0.reltuples tbltuples
                              from (select tbl.oid toid, ns.nspname as schema,
                                       tbl.relname as tablename,
                                       tbl.reltuples,
                                       tbl.relpages as heappages,
                                       coalesce(substring(array_to_string(tbl.reloptions, ' ')
                                             FROM '%fillfactor=#"__#"%' FOR '#')::smallint, 100) AS fillfactor,
                                       coalesce(toast.relpages, 0) AS toastpages,
                                       coalesce(toast.reltuples, 0) AS toasttuples,
                                       current_setting('block_size')::numeric AS bs,
                                       24 as page_hdr,
                                       CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64'
                                            THEN 8
                                            ELSE 4 END AS ma,
                                       bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na,
                                       23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0
                                                 THEN ( 7 + count(*) ) / 8
                                                 ELSE 0::int END + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,
                                       sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size
                                      from pg_class tbl
                                      JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
                                      join pg_attribute as att ON att.attrelid = tbl.oid
                                      LEFT JOIN pg_class AS toast ON toast.oid = tbl.reltoastrelid
                                      left JOIN pg_stats AS s ON s.schemaname=ns.nspname
                                            AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
                                      where tbl.relkind = 'r' and ns.nspname not in ('pg_catalog', 'information_schema')
                                        and att.attnum > 0 AND NOT att.attisdropped
                                      group by tbl.oid, ns.nspname, tbl.relname, tbl.reltuples, tbl.relpages,
                                               fillfactor, toastpages, toasttuples, tbl.relhasoids) Res0
                              left join pg_stat_all_tables st on Res0.schema = st.schemaname and Res0.tablename = st.relname) Res1
                    WHERE bloat_size > {{BLOAT_MB}}"""

    q = """
    SELECT r.tablename, r.bloat_bytes, r.bloat_human, r.table_human, r.clear_table_human, r.bloat_perc, r.tbltuples,
            r.last_vacuum, r.last_autovacuum, r.last_analyze, r.last_autoanalyze
    FROM (%s) r
    WHERE bloat_perc >= {{BLOAT_PERC}}
    order by bloat_perc desc
    """ % (q_after_12 if db.vermajor_a >= 12 else q_before_12)

    print("\nSearching for tables with: %d%% bloat AND %dMB bloat data size in given schema with %d tables" %
          (opts.bloat_perc, opts.bloat_mb, DB.execute_fetchval(con, "SELECT COUNT(*) FROM pg_class")))

    ret = DB.execute_fetchall(con, q.replace("{{BLOAT_MB}}", str(opts.bloat_mb * 1024 * 1024)).replace("{{BLOAT_PERC}}", str(opts.bloat_perc)))
    if not ret or not len(ret):
        print("OK, all tables are good, vacuum is not needed")
        return

    print("found %d potential candidates for vacuum:" % len(ret))

    print("")
    print("  TABLE                             BLOAT_SZ   ACTUAL    CLEAR BLOAT%       ROWS" +
          "           VACUUM      AUTO_VACUUM          ANALYZE     AUTO_ANALYZE")

    print("                                               TBL_SZ   TBL_SZ                  " +
          "             LAST             LAST             LAST             LAST")

    print("  -------------------------------- --------- -------- -------- ------ ----------" +
          " ---------------- ---------------- ---------------- ----------------")

    for r in ret:
        table_str = r[0]
        if len(table_str) > 32:
            table_str = table_str[0:32-3] + "..."
        print("  %-32s %9s %8s %8s %6.1f %10s %16s %16s %16s %16s" % tuple([table_str] + list(r[2:])))

    print("  -------------------------------- ---------")
    print("                 Total bloat size: %7dMB" % int(round(sum([r[1] for r in ret]) / (1024 * 1024), 0)))

    if not opts.vacuum:
        print("")
        print("Run the script with --vacuum option to do the vacuuming, use --yes to do it for all the tables above")
        return

    print("")

    selected = []

    for r in ret:
        table = r[0]
        ch = None
        while ch != "y" and ch != 'n':
            try:
                q = "Do vacuum full on '%s' to release %d MB on disk ? ..." % (table, r[1] / (1024 * 1024))
                print(q + "." * (80 - len(q)) + " [y/n] ", end="")
                if opts.yes:
                    ch = 'y'
                else:
                    ch = get_yn()
            except KeyboardInterrupt as e:
                print("")
                return

            if ch == 'y':
                selected.append(table)
                print("  scheduling '%s'" % table)

    if selected:
        print("")
        vacuum(selected)
    else:
        print("")
        print("Exiting, nothing to do")


def main():
    global opts
    global con

    print(os.path.split(sys.argv[0])[1] + " version 1.0")

    p = OptionParser("%prog [options]")
    p.add_option("-v", "--verbose", action="store_true", help="enable verbose mode")
    p.add_option("-y", "--yes", action="store_true", help="don't ask and do vacuum for all problematic tables")
    p.add_option("-a", "--vacuum", action="store_true", help="do the vacuuming")
    p.add_option("-m", "--bloat-mb", type="int", default=100, help="table bloat threshold in megabytes (default %default)")
    p.add_option("-p", "--bloat-perc", type="int", default=50, help="table bloat threshold in percent (default %default)")
    p.add_option("-t", "--vacuum-table", action="append", help="vacuum given table and exit (multiple options accepted)")

    DB.add_options(p)

    opts, args = p.parse_args()

    configure_logging(opts.verbose)

    db = DB(opts)
    print("Connecting to %s ..." % str(db))
    con = db.connect()

    if opts.vacuum_table and len(opts.vacuum_table):
        vacuum(opts.vacuum_table)
        return

    pg_vacuum(db, con)


if __name__ == "__main__":
    main()
