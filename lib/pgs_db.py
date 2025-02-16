# (C) https://github.com/perfguru87/pgs-tools
# Apache-2.0 license

import os
import sys
import psycopg2
import logging
import optparse
import time
import socket
import copy
import sqlparse

##############################################################################################################
# Simple 'DB' class which can be used for trivial short-living connections
##############################################################################################################

DEF_DB_PORT = 5432
DEF_DB_PASS = ""


def db_fatal_error(msg):
    sys.stderr.write(msg + "\n")
    sys.exit(-1)


class DBQuery:
    def __init__(self, query, dur_sec):
        self.query = query.strip()
        self.dur_sec = dur_sec

    def format(self):
        return sqlparse.format(self.query, reindent=True, keyword_case='upper')


class _DBConnection:
    def __init__(self, db, conn_no, autocommit=True, ro_mode=False,
                 fatal_error_cb=None, reconnect_attempts=5, connection_timeout=3, track_history=False):
        self.db = db
        self.conn_no = conn_no

        self._fatal_error_cb = fatal_error_cb

        self.con = None

        self._autocommit = autocommit or ro_mode
        self._ro_mode = ro_mode
        self._reconnect_attempts = reconnect_attempts
        self._connection_timeout = connection_timeout
        self._isolation_level = None

        self.reconnect()

        self._track_history = track_history
        self.history = []

    def __str__(self):
        return str(self.db) + " conn:%d" % self.conn_no

    def fatal_error(self, msg):
        return self._fatal_error_cb(msg) if self._fatal_error_cb else db_fatal_error(msg)

    def clear_history(self):
        self.history = []

    def connection(self):
        return self.con

    def commit(self):
        self.con.commit()

    def rollback(self):
        self.con.rollback()

    def cursor(self):
        return self.con.cursor() if self.con else None

    def close(self):
        if self.con and self.con.closed == 0:
            self.con.close()
            self.con = None

    def __del__(self):
        self.close()

    def closed(self):
        if self.con is not None and self.con.closed == 0:
            return False
        else:
            return True

    @property
    def isolation_level(self):
        if self.con:
            return self.con.isolation_level
        return None

    def set_isolation_level(self, lvl):
        if self.con:
            self._isolation_level = lvl
            self.con.set_isolation_level(lvl)

    def check_connection(self):
        if not self.db.loc.db_host or self.db.loc.db_host == "/tmp/":
            return
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self._connection_timeout)
        s.connect((self.db.loc.db_host, self.db.loc.db_port))
        s.close()

    @staticmethod
    def reconnectable(action):
        def decorator(func):
            def looper(self, *args):

                msg = None

                attempts = 0
                while True:

                    try:
                        # normal path
                        return func(self, *args)

                    except socket.error as e:
                        msg = "%s: %s" % (str(self), e)
                        logging.debug(msg)

                    except psycopg2.Error as e:
                        msg = "%s: %s error%s%s: %s" % (str(self), action,
                                                        (" (pgerror: %s)" % e.pgerror) if e.pgerror else "",
                                                        (" (pgcode: %s)" % e.pgcode) if e.pgcode else "",
                                                        str(e))
                        logging.debug(msg)

                        if isinstance(e, psycopg2.OperationalError) or self.closed():
                            if "pg_hba" in str(e):
                                # seems like connection is not possible
                                self.fatal_error(e.message)
                                raise

                            # networking/connection error, restartable...
                            pass

                        else:
                            # not a connection error, re-raising...
                            self.connection().rollback()
                            if self.cursor():
                                self.cursor().close()
                            raise

                    attempts += 1
                    if attempts >= self._reconnect_attempts:
                        break

                    time.sleep(1)

                msg = "%s: maximum number of %s attempts reached, aborting%s" % \
                      (str(self), action, ("\n" + msg) if msg else "")
                self.fatal_error(msg)
                raise RuntimeError(msg)

            return looper

        return decorator


class DBConnection(_DBConnection):

    @_DBConnection.reconnectable("DB connection")
    def reconnect(self):

        self.close()

        app_name = os.path.basename(os.path.splitext(sys.argv[0])[0])
        loc = self.db.loc

        self.check_connection()
        con = psycopg2.connect('host=%s port=%s dbname=%s user=%s password=%s application_name=%s %s' %
                               (loc.db_host, loc.db_port, loc.db_name, loc.db_user, loc.db_pass, app_name,
                                "sslmode='require'" if loc.db_ssl else ""))

        self.con = con

        if self._autocommit:
            try:
                self.con.autocommit = True
            except Exception as e:
                # https://www.postgresql.org/message-id/CA+mi_8bC83oscK2YgR6NwXVDHStF=O-z7RMLJ_iACvpet2EJTA@mail.gmail.com
                self.set_isolation_level(0)
        elif self._isolation_level is not None:
            self.set_isolation_level(self._isolation_level)

        return self.con

    @_DBConnection.reconnectable("DB query")
    def execute_fetch(self, query, fetchfn, *args):
        rv = None
        cur = None
        if self.closed():
            self.reconnect()
        realcon = self.connection()

        if self._ro_mode:
            q = " " + query.upper()
            if " INSERT " in q or " DELETE " in q or " UPDATE " in q:
                raise psycopg2.Error("read/only mode violation attempts: %s" % query)

        cur = realcon.cursor()

        s = str(self.db) + ": " + query

        if self._track_history:
            start = time.time()

        if args is None or not len(args):
            logging.debug(s)
            cur.execute(query)
        else:
            logging.debug(s, *args)
            cur.execute(query, args)

        if self._track_history:
            self.history.append(DBQuery(query, time.time() - start))

        rv = fetchfn(cur)
        cur.close()

        return rv


class DB:
    def __init__(self, opts=None, db_host=None, db_port=None, db_name=None, db_user=None, db_pass=None,
                 db_ssl=False, fatal_error_cb=None, autodiscovery=True):
        self.loc = DBLocation(opts=opts, db_host=db_host, db_port=db_port, db_name=db_name, db_user=db_user, db_pass=db_pass, db_ssl=db_ssl,
                              autodiscovery=autodiscovery)

        self._fatal_error_cb = fatal_error_cb

        self.vermajor_a = 0
        self.vermajor_b = 0
        self.verminor = 0

        self.conn_no = 0

    @staticmethod
    def add_options(option_parser):
        DBLocation.add_options(option_parser)

    def __str__(self):
        return str(self.loc)

    def fatal_error(self, msg):
        return self._fatal_error_cb(msg) if hasattr(self, "_fatal_error_cb") and self._fatal_error_cb else db_fatal_error(msg)

    def connect(self, autocommit=True, ro_mode=False, fatal_error_cb=None, reconnect_attempts=5, track_history=False):
        con = DBConnection(self, self.conn_no, autocommit=autocommit, ro_mode=ro_mode,
                           fatal_error_cb=fatal_error_cb if fatal_error_cb else self._fatal_error_cb,
                           reconnect_attempts=reconnect_attempts, track_history=track_history)
        self.conn_no += 1

        if not self.vermajor_a:
            ret = self.execute_fetchval(con, "show server_version").split('.')
            self.vermajor_a = int(ret[0])
            self.vermajor_b = int(ret[1])
            self.verminor = int(ret[2] if len(ret) == 3 else '0')
            logging.info("%s: connected to PostgreSQL version %d.%d.%d" %
                         (str(con), self.vermajor_a, self.vermajor_b, self.verminor))

        con.clear_history()
        return con

    @staticmethod
    def execute_fetch(con, query, fetchfn, *args):
        return con.execute_fetch(query, fetchfn, *args)

    @staticmethod
    def execute_fetchone(con, query, *args):
        return DB.execute_fetch(con, query, lambda cur: cur.fetchone(), *args)

    @staticmethod
    def execute_fetchall(con, query, *args):
        return DB.execute_fetch(con, query, lambda cur: cur.fetchall(), *args)

    @staticmethod
    def execute_fetchval(con, query, *args):
        ret = DB.execute_fetchone(con, query, *args)
        if ret is not None and len(ret) > 0:
            return ret[0]

    @staticmethod
    def execute(con, query, *args):
        return DB.execute_fetch(con, query, lambda cur: cur.rowcount, *args)


class DBLocation:
    def __init__(self, opts=None, db_host=None, db_port=None, db_name=None, db_user=None, db_pass=None, db_ssl=False, autodiscovery=False):
        self.db_host = db_host if db_host else (opts.db_host if opts else None)
        self.db_port = db_port if db_port else (opts.db_port if opts else DEF_DB_PORT)
        self.db_name = db_name if db_name else (opts.db_name if opts else None)
        self.db_user = db_user if db_user else (opts.db_user if opts else None)
        self.db_pass = db_pass if db_pass else (opts.db_pass if opts else DEF_DB_PASS)
        self.db_ssl = db_ssl if db_ssl else (opts.db_ssl if opts and hasattr(opts, 'db_ssl') else False)

        self.autodiscovered = False

        if autodiscovery:
            self.discover()

        if not self.db_name:
            self.fatal_error("couldn't recognize the DB name, please set the --db-name option\n")

    def __str__(self):
        return "%s@%s:%s/%s%s" % (self.db_user, self.db_host, self.db_port, self.db_name,
                                  "(SSL)" if self.db_ssl else "")

    @staticmethod
    def add_options(option_parser):
        g = optparse.OptionGroup(option_parser, "Database credentials")
        g.add_option("", "--db-host", type="string",
                     help="hostname/IP (or '/tmp/' for local socket connection)")
        g.add_option("", "--db-port", type=int, default=DEF_DB_PORT, help="database port (default %d)" % DEF_DB_PORT)
        g.add_option("", "--db-name", type="string", help="database name")
        g.add_option("", "--db-user", type="string", help="database username")
        g.add_option("", "--db-pass", type="string", default=DEF_DB_PASS, help="database password")
        g.add_option("", "--db-ssl", default=False, action="store_true", help="enable SSL connection")
        option_parser.add_option_group(g)

    def is_ok(self):
        if self.autodiscovered:
            return True
        return self.db_host and self.db_port and self.db_user and self.db_pass

    def _discover(self):
        logging.debug("DB credentials are incomplete (%s), Running autodiscovery..." % str(self))

        class ExcDiscovery(RuntimeError):
            pass

        def _raise(msg):
            raise ExcDiscovery(msg)

        def _search(db_names, db_hosts, db_users):
            for db_name in db_names:
                for db_host in db_hosts:
                    for db_user in db_users:
                        if not db_name or not db_host or not db_user:
                            continue

                        db = DB(db_name=db_name, db_host=db_host, db_user=db_user, autodiscovery=False,
                                fatal_error_cb=_raise)

                        logging.debug("Trying to connect to: %s ..." % str(db))

                        try:
                            c = db.connect(reconnect_attempts=1)
                        except ExcDiscovery as e:
                            continue

                        c.close()

                        self.db_host = db.loc.db_host
                        self.db_port = db.loc.db_port
                        self.db_name = db.loc.db_name
                        self.db_user = db.loc.db_user
                        self.db_pass = db.loc.db_pass
                        self.db_ssl = db.loc.db_ssl
                        self.autodiscovered = True
                        return True
            return False

        db_names = [self.db_name] + ['postgres']
        db_hosts = [self.db_host] + ['/tmp/', '127.0.0.1']
        db_users = [self.db_user] + ['postgres']
        if _search(db_names, db_hosts, db_users):
            return True

        return False

    def discover(self):
        if self.is_ok():
            return None
        ret = self._discover()
        if ret:
            logging.debug("DB '%s' has been autodiscovered!" % str(self))
        else:
            logging.debug("autodiscovery couldn't find any DB")
        return ret
