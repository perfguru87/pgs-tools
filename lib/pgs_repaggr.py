# (C) https://github.com/perfguru87/pgs-tools
# Apache-2.0 license

# The report.py reports aggregator

import pgs_report
import re
import sys
import os
import tempfile


class RAFetcher:
    def __init__(self, name=None, text=None, filename=None, line_parser=None,
                 section_begin_parser=None, section_end_parser=None):

        if not text and not filename:
            raise Exception("RAFetcher: neither file nor report text passed")

        if not line_parser:
            raise Exception("RAFetcher: the line_parser argument is mandatory")

        self.text = text
        if not self.text:
            f = open(filename, 'r')
            self.text = f.read()
            f.close()

        self.name = name if name else (os.path.split(filename)[-1].split(".")[0] if filename else None)

        self.data = {}

        section_found = False if section_begin_parser else True

        for line in self.text.splitlines():
            if not section_found:
                section_found = section_begin_parser(line)
            if section_found and section_end_parser and section_end_parser(line):
                section_found = False
            if not section_found:
                continue
            key, value = line_parser(line)
            if key:
                self.data[key] = (len(self.data), value)


class RAComparator:
    def __init__(self, sect_title, report_fetchers, table_title=None, rows_top_bottom_colors=False):
        self.sect_title = sect_title
        self.table_title = table_title if table_title else sect_title
        self.rows_top_bottom_colors = rows_top_bottom_colors
        self.report_fetchers = report_fetchers
        self.keys = {}

    def _init_keys(self):
        if len(self.keys):
            return

        for n in range(0, len(self.report_fetchers)):
            r = self.report_fetchers[n]
            r.name = r.name if r.name else "#%d" % n

            for key in sorted(r.data, key=lambda x: r.data[x][0]):
                if key not in self.keys:
                    self.keys[key] = len(self.keys)

    def _dump(self, format=pgs_report.FORMAT_TEXT, filestream=None):
        self._init_keys()

        r = pgs_report.Report()
        s = r.add_section(self.sect_title)
        t = s.add_table()

        t.add_header([pgs_report.RTableCol(self.table_title, left=True)] + [rf.name for rf in self.report_fetchers])

        for key in sorted(self.keys, key=lambda x: self.keys[x]):
            row = [key]
            for rf in self.report_fetchers:
                val = rf.data.get(key, None)
                row.append(val[1] if val else 0)
            t.add_row(row, top_bottom_colors=self.rows_top_bottom_colors)

        r.flush(format=format, filestream=filestream if filestream else sys.stdout)

    def dump(self, format="text", filestream=None):
        self._init_keys()
        if format in ("text", "html"):
            return self._dump(format=pgs_report.FORMAT_TEXT if format == "text" else pgs_report.FORMAT_HTML,
                              filestream=filestream)
        raise Exception("unsupported format: %s" % str(format))


##########################################################################################

_rep1 = """
====================
Section #2
--------------------

    Key     Val1   Val2
    -------------------
    aaa        1      2
    bbb        3      4
    ccc        7      6
"""

_rep2 = """
====================
Section #1
--------------------

    Key     Val1   Val2
    -------------------
    xxx      0.1   0.98
    yyy      0.2   0.99
    ddd      0.3    0.5

====================
Section #2
--------------------

    Key     Val1   Val2
    -------------------
    ccc        2     12
    aaa        4     13
    ddd        6     14

====================
Section #3
--------------------

    Key     Val1   Val2
    -------------------
    zzz       2     12

"""


def __example__():

    reLINE = re.compile(r"\s+(\w+)\s+(\d+)\s+\d+.*")

    def sbp(line):
        return line == "Section #2"

    def sep(line):
        return line.startswith("=" * 10)

    def lp(line):
        m = reLINE.match(line)
        if not m:
            return False, False
        g = m.groups()
        return g[0], g[1]

    reps = []
    reps.append(RAFetcher("r1", text=_rep1, line_parser=lp, section_begin_parser=sbp, section_end_parser=sep))

    fobj, fname = tempfile.mkstemp()
    os.write(fobj, _rep2.encode())
    reps.append(RAFetcher("r2", filename=fname, line_parser=lp, section_begin_parser=sbp, section_end_parser=sep))

    os.unlink(fname)

    rc = RAComparator("My data", reps)
    rc.dump(format="text")


def __coverage__():
    def lp(line):
        return 'a', 1

    try:
        r1 = RAFetcher("x", text=_rep1)
    except Exception as e:
        r1 = RAFetcher("x", text=_rep1, line_parser=lp)

    try:
        r2 = RAFetcher("x", line_parser=lp)
    except Exception as e:
        r2 = RAFetcher("x", text=_rep2, line_parser=lp)

    rc = RAComparator("y", [r1, r2])
    try:
        rc.dump(format='xxx')
    except Exception as e:
        pass


if __name__ == "__main__":
    __example__()
    __coverage__()
