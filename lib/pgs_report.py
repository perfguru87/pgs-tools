# (C) https://github.com/perfguru87/pg-tools
# Apache-2.0 license

# Features:
#
# RTable
# |- table features: autoalign table width, text/html
# |- columns features: min width, colspan, left alignment, wrapping, hidden, custom formatting
# |- table header can be initialized by list of strings, tuples or column objects
# '- rows/columns styles (colors)
#
# RSection
# `- auto header notes generation (depnding on internal sections content)
#
# TODO:
# `- safe flush

import sys
import os
import re
import copy
import datetime
import textwrap
import json
import decimal

try:
    from StringIO import StringIO  # for Python 2
except ImportError:
    from io import StringIO  # for Python 3

###################################################################################################
# Attributes to be used in caller: row/column styles and report options
###################################################################################################

BOLD = ('\033[1m', '\033[0m')
HEADER = ('', '')

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, GRAY = \
    [("\x1b[1;%dm" % (30 + c), "\x1b[0m") for c in range(9)]

BG_BLACK, BG_RED, BG_GREEN, BG_YELLOW, BG_BLUE, BG_MAGENTA, BG_CYAN, BG_WHITE = \
    [("\x1b[6;30;%dm" % (40 + c), "\x1b[0m") for c in range(8)]

# special hack for gray background, otherwise it is shown as black on black on some terminals
BG_GRAY = ("\x1b[6;38;38m", "\x1b[0m")

HTML_BG_COLORS = {
    'RED': '#f66f6f',
    'GREEN': '#9acd82',
    'YELLOW': '#ffeb79',
    'BLUE': '#2980b9',
    'GRAY': '#afafaf'
}

FORMAT_TEXT = 0
FORMAT_HTML = 1
FORMAT_JSON = 2

HTML_WIDTH = 180
TEXT_WIDTH = 120

TABLE_COL_SEP = "  "

###################################################################################################
# Internal constants
###################################################################################################

_ITERABLE = (list, tuple)
_SUPPORTED_ROW_FMTS = (str, list, tuple)
_FORMATS = [FORMAT_TEXT, FORMAT_HTML, FORMAT_JSON]
_DEFAULT_INDENT = "  "
_HTML_DASH = '&#8212;'
_VISIBLE_LINES = 50


###################################################################################################
# HTML stuff
###################################################################################################

HTML_HEADER = """
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/9.12.0/highlight.min.js"></script>
<style>
body { font-size: 12px; margin: 8px 8px 0px 8px; }
h1, h2 { font-size: 12px; font-family: verdana; padding: 2px 0px 2px 20px; z-index: 5px; }
h1 { background-color: #336; color: white; line-height: 16px; margin: 0px; }
h2 { background-color: #ccc; color: black; line-height: 12px; margin: 5px 0px; }
div.section { margin: 4px 0px 8px 0px; padding: 10px 15px; background-color: #eee; }
div.subsection { margin: 5px 5px; background-color: #eee; padding-top: 5px; }
div.sql_query_list { padding: 10px; background-color: #0f192a; }
div.sql_query_list pre { color: #d1edff; font-size: 9px; font-family: monospace, Courier; }
div.sql_query_list pre sql { color: #ffffff; }
div.sql_query_list .timing { color: #b43d3d; font-size: 9px; }
pre { font-size: 12px; padding: 0px; margin: 0px; }
pre pre { margin: 2px 0px 2px 0px; }
div.table_body { }
div.table_tail_container { padding: 0px; margin: 0px; }
div.table_tail_body { background-color: #d8d8d8; display: none; }
div.table_tail_toggle_show { color: #339; padding-left: 30px; border-top: 1px dashed #ccc; margin-top: 2px; }
div.table_tail_toggle_hide { color: #339; padding-left: 30px; background-color: #ccc; margin-top: 0px; display: none; }
div.table_tail_toggle_show:hover, div.table_tail_toggle_hide:hover { color: #339; cursor:pointer; }
.header_notes { font-size: 9px; float: right; display: block; font-weight: normal; padding-right: 10px; }
.header_notes span { margin-left: 5px; min-width: 10px; }
h1 .header_notes { color: #ccc; }
h2 .header_notes { color: #444; }
.header_notes_body { display: none; position: relative; z-index: 1; }
.section .header_notes_body { top: 0px; padding: 30px 0px 0px 0px; margin: 0px; position: relative; }
.section .header_notes_body:first-of-type { top: -15px; position: relative; margin: 0px; padding: 0px; }
#footer {
    padding: 5px; z-index: 10;
    font-family: arial; min-height: 20px;
    position: fixed; bottom: 0px; left: 0px; width: 100%;
    background: #ccc; box-shadow: 0px 0px 5px #aaa; border-top: 1px solid #888;
}
#footer div { padding: 0px 0px 0px 10px; color: #339; cursor: pointer; font-size: 12px; max-width: 250px; }
#footer div:hover { background-color: #aaa; color: #fff; }
#footer div.selected { background-color: #888; color: #fff; }
#footer table { table-layout: fixed; }
#footer table td { vertical-align: top; }
.hljs-comment { color: #ccc; }
.hljs-keyword { color: #f7e741; }
</style>

<script>
function toggle(el)
{
    var tail = el.parentNode.getElementsByClassName('table_tail_body');
    var tog_show = el.parentNode.getElementsByClassName('table_tail_toggle_show')[0];
    var tog_hide = el.parentNode.getElementsByClassName('table_tail_toggle_hide')[0];

    if (!tail)
        return;
    if (tail[0].style.display == "block") {
        tail[0].style.display = "none";
        tog_show.style.display = "block";
        tog_hide.style.display = "none";
    } else {
        tail[0].style.display = "block";
        tog_show.style.display = "none";
        tog_hide.style.display = "block";
    }
}

function toggle_notes(el)
{
    var notes = el.parentNode.parentNode.getElementsByClassName('header_notes_body');

    for (var i = 0; i < notes.length; i++) {
        if (notes[i].style.display == "block") {
            el.children[0].innerHTML = "&#9656;";
            notes[i].style.display = "none";
        } else {
            el.children[0].innerHTML = "&#9662";
            notes[i].style.display = "block";
        }
    }
}

function scroll() {
    var hs = document.getElementsByClassName("section-header")
    var candidate = null;
    var final_candidate = false;

    for (var i = 0; i < hs.length; i++) {
        var name = hs[i].getAttribute("name");
        var el = document.getElementById("link-to-" + name);
        if (!el)
            continue;
        var pos = hs[i].getBoundingClientRect();
        el.className = "";
        if (pos.top > 0 && pos.top < 50) {
            final_candidate = true
            candidate = el;
        }
        if (!final_candidate && pos.top < 100)
            candidate = el;
    }
    if (candidate)
        candidate.className = "selected";
}

window.onload = function () {
    var columns = 6;

    var toc = "<table style='width: 95%;'><tr><td>";

    var hs = document.getElementsByClassName("section-header")
    for (var i = 0; i < hs.length; i++) {
        if (i && !(i % Math.ceil(hs.length / columns)))
            toc += "</td><td>";
        name = hs[i].getAttribute("name");
        toc += "<div id='link-to-" + name + "' onclick=\\\"location.href='#" +
               name + "';\\\">" + hs[i].nextSibling.innerHTML.split("<")[0] + "</div>";
    }

    var footer = document.getElementById("footer");
    var body = document.getElementById("body");
    footer.innerHTML = toc;
    body.style.paddingBottom = footer.clientHeight;
    scroll();

    hljs.initHighlighting();
}

window.onscroll = function() { scroll(); }
</script>

<body>
<div id=container>
<div id='footer'></div>
<div id='body'>
"""

HTML_FOOTER = "</div></div></body></html>"

###################################################################################################
# Helpers
###################################################################################################

_html_escape_table = {
    "&": "&amp;",
    '"': "&quot;",
    "'": "&apos;",
    ">": "&gt;",
    "<": "&lt;",
    "\\": "&#92;"
}


def html_escape(text):
    """Produce entities within text."""
    return "".join(_html_escape_table.get(c, c) for c in text)


reSPLIT = re.compile(r'\w+|\W')
known_words = ["llc", "inc", "ltd", "limited", "co", "plc", "pllc", "the", "group", "ag", "bv"]


def obfuscate(name):
    try:
        ar = reSPLIT.findall(name)
        for n in range(0, len(ar)):
            if len(ar[n]) == 1:
                continue
            if ar[n].lower() in known_words:
                continue
            ar[n] = "".join(["X" if ch.isupper() else "x" for ch in ar[n]])
        return "".join(ar)
    except Exception as e:
        return "***"


def has_colors(stream):
    if not hasattr(stream, "isatty") or not stream.isatty():
        return False

    try:
        import curses
        curses.setupterm()
        return curses.tigetnum("colors") > 2
    except Exception as e:
        return False


def justify(val, width, left=False):
    if not width:
        return val
    if len(val) > width:
        return val[:width - 3] + "..."
    if len(val) < width:
        return val.ljust(width, ' ') if left else val.rjust(width, ' ')
    return val

###################################################################################################
# Report nodes
###################################################################################################


class RNode:
    def __init__(self):
        self.nodes = []
        self.parent = None
        self.width = None
        self.indent_txt = _DEFAULT_INDENT
        self.section_header_note = ""  # something that will be shown in section header as a note
        self._filestream_flushed = {FORMAT_TEXT: set(), FORMAT_HTML: set(), FORMAT_JSON: set()}
        self.id = 0
        self.next_id = 0

    def add_indent(self, text):
        indent = 0
        p = self.parent
        while p:
            p = p.parent
            indent += 1
        out = [self.indent_txt * (indent - 1) + s for s in text.split("\n")]
        return "\n".join(out)

    def render_html(self):
        out = ""
        for n in self.nodes:
            out += n.render_html()
        return out

    def render_text(self):
        out = ""
        for n in self.nodes:
            out += n.render_text()
        return self.add_indent(out) + "\n"

    def render_json(self):
        nodes = []
        for n in self.nodes:
            j = n.render_json()
            if j:
                nodes.append(j)
        return nodes

    def flush(self, format=FORMAT_TEXT, filestream=None):
        if format not in _FORMATS:
            raise RuntimeError("Unsupported format: %s" % (str(format)))

        if format == FORMAT_JSON:
            return ""

        if not self.parent:
            for n in self.nodes:
                n.flush(format=format, filestream=filestream)
            return ""
        if filestream in self._filestream_flushed[format]:
            return ""
        self._filestream_flushed[format].add(filestream)

        if format == FORMAT_TEXT:
            out = self.render_text()
        elif format == FORMAT_HTML:
            out = self.render_html()

        if filestream:
            filestream.write(out)
            filestream.flush()
            return
        return out

    def reset_flushed(self):
        self._filestream_flushed = {FORMAT_TEXT: {}, FORMAT_HTML: {}}

    def _get_parent_width(self):
        p = self.parent
        while p:
            if p.width:
                return p.width
            p = p.parent
        return TEXT_WIDTH

    def gen_id(self):
        p = self.parent
        if not p:
            p = self
        while p.parent:
            p = p.parent
        p.next_id += 1
        return p.next_id


class Report(RNode):
    def __init__(self, width=None):
        RNode.__init__(self)
        self.width = width
        if self.width is None:
            try:
                # FIXME: width must be initialized during the rendering, not in the constuctor
                #        because here we don't know will it be HTML or TEXT report
                _, self.width = os.popen('stty size', 'r').read().split()
                self.width = min(int(self.width) - 1, HTML_WIDTH)
            except Exception as e:
                self.width = TEXT_WIDTH
        self.add_node(RHeader())

    def add_node(self, node):
        node.parent = self
        node.id = self.gen_id()
        self.nodes.append(node)
        return self

    # useful alieases

    def add_section(self, *args, **kwargs):
        s = RSection(*args, **kwargs)
        self.add_node(s)
        return s

    def flush(self, format=FORMAT_TEXT, filestream=None):
        if format == FORMAT_JSON:
            footer_found = False
            for n in self.nodes:
                if isinstance(n, RFooter):
                    footer_found = True

            if footer_found is False:
                return ""

            out = json.dumps(self.render_json())
            if filestream:
                filestream.write(out)
                filestream.flush()
                return
            return out
        return RNode.flush(self, format=format, filestream=filestream)


class RHeader(RNode):
    def render_html(self):
        return HTML_HEADER

    def render_text(self):
        return ""

    def render_json(self):
        return {}


class RFooter(RNode):
    def render_html(self):
        return HTML_FOOTER

    def render_text(self):
        return ""

    def render_json(self):
        return {}


class RSection(RNode):
    def __init__(self, title, section_note_title=""):
        self._title = title
        self._section_note_title = section_note_title
        self._header_notes = []
        RNode.__init__(self)

    def add_node(self, node):
        if node.section_header_note:
            self._header_notes.append(node.section_header_note)
        node.parent = self
        node.id = self.gen_id()
        self.nodes.append(node)
        return self

    def render_html(self):
        out = self._title
        if self._header_notes:
            out += "<span class='header_notes' onclick='toggle_notes(this);'>%s%s<span>&#9656;</span></span>" % \
                   (self._section_note_title, " | ".join([n for n in self._header_notes]))

        if isinstance(self.parent, Report):
            out = "<div><a class='section-header' name='section%d'></a><h1>%s</h1>" % (self.id, out)
        else:
            out = "<div><h2>" + out + "</h2>"

        out += "<div class='section'>"

        first = True
        for n in self.nodes:
            if isinstance(n, (RTable, RText)):
                if first:
                    first = None
                else:
                    out += "<br>"
            out += n.render_html()

        out += "</div></div>"

        return out

    def render_text(self):
        out = [""]
        if isinstance(self.parent, RSection):
            out.append(self._title)
            out.append("=" * len(self._title))
        else:
            w = self._get_parent_width() - len(self.add_indent(""))
            out.append("=" * w)
            out.append(self._title)
            out.append("-" * w)
        out = self.add_indent("\n".join(out) + "\n")
        for n in self.nodes:
            out += n.render_text()
        if isinstance(self.parent, Report):
            out += "\n"
        return out

    def render_json(self):
        out = {}
        out['node_type'] = "section"
        out['title'] = self._title

        nodes = []
        for n in self.nodes:
            nodes.append(n.render_json())
        out['data'] = nodes
        return out

    # useful aliases

    def add_table(self, *args, **kwargs):
        t = RTable(*args, **kwargs)
        self.add_node(t)
        return t

    def add_section(self, *args, **kwargs):
        t = RSection(*args, **kwargs)
        self.add_node(t)
        return t

    def add_text(self, *args, **kwargs):
        t = RText(*args, **kwargs)
        self.add_node(t)
        return t


class RText(RNode):
    def __init__(self, text, raw_html=False):
        self._text = text
        self._raw_html = raw_html
        RNode.__init__(self)

    def render_html(self):
        return self._text if self._raw_html else ("<br><pre>%s</pre><br>" % html_escape(self._text))

    def render_text(self):
        return "\n" + self.add_indent(self._text) + "\n"

    def render_json(self):
        out = {}
        out['node_type'] = 'text'
        out['data'] = self._text
        return out


class RTableCol:
    def __init__(self, title=None, width=None, left=False, format=None, separator=TABLE_COL_SEP, style=None,
                 wrap=False, colspan=1, bottom=True, raw_html=False):
        """
        width     - None - auto; 0 - hidden; >0 - min width = width; <0 - min width = -width, left aligned
        left      - force left alignment
        format    - cell format string or formatter function: format_cb(val, width, is_left, is_html)
                    expected return values are (a) a string or (b) a tuple (string, width)
        separator - columns separator
        style     - column cells style
        wrap      - column cells are wrappable
        colspan   - similar to HTML colspan tag
        bottom    - bottom aligned
        raw_html  - column includes raw HTML so no escaping required
        """

        self.title = title
        self.hidden = width == 0
        width = 0 if width is None else int(width)
        self.width_user = width if width >= 0 else -width  # wanted by user
        self.width_max = self.width_user                   # max value width
        self.width = self.width_user                       # displayed width
        self.left = left or width < 0
        self.format = format  # string or callback to format the value
        self.separator = separator
        self.style = style if style else None
        self.wrap = wrap
        self.colspan = colspan
        self.colno = 0
        self.bottom = bottom
        self.raw_html = raw_html

    def adjust(self, len):
        self.width_max = max(self.width_max, len)
        if self.width_user:
            return
        self.width = self.width_max

    def __str__(self):
        return "%s|width=%d(max=%d)|left=%s|format=%s|separator=%s|style=%s|%s|colspan=%d|%s|raw_html=%s" % \
               (str(self.title).replace("\n", "\\n"), self.width, self.width_max, str(self.left)[0],
                str(self.format), self.separator, str(self.style),
                "wrap" if self.wrap else "nowrap", self.colspan,
                "bottom" if self.bottom else "top", str(self.raw_html)[0])


class RTableCell:
    def __init__(self, val, colspan=1, left=None, wrap=False, is_separator=False, bottom=False, style=None):
        self.val = val
        self.colspan = colspan
        self.left = left
        self.wrap = wrap
        self.is_separator = is_separator
        self.bottom = bottom
        self.style = style

    def is_left(self, col):
        return self.left if self.left is not None else col.left

    def _format_value(self, col, autoreplace, is_html):
        val = self.val
        if self.colspan == 0 or col.hidden or col.colspan == 0:
            return "", 0

        if val is None:
            return "-", 1

        if self.is_separator:
            return str(self.val)[0], 1

        if callable(col.format):
            try:
                ret = col.format(val, col.width, self.is_left(col), is_html)
                return (ret, len(ret)) if not isinstance(ret, tuple) else (ret[0], ret[1])
            except AttributeError:
                val = str(val)  # column title typically goes here
        else:
            try:
                if col.format:
                    val = col.format % val
            except TypeError:
                val = str(val)  # column title typically goes here

        val = str(val)

        if val in autoreplace:
            val = str(autoreplace[val])

        return val, max([len(v) for v in val.split('\n')])

    def get_text(self, col, autoreplace, is_html):
        val, _ = self._format_value(col, autoreplace, is_html)
        return val

    def get_width(self, col, autoreplace, is_html):
        _, width = self._format_value(col, autoreplace, is_html)
        return width

    def get_lines(self, col, width, autoreplace, is_html):
        if not width:
            return []

        text = self.get_text(col, autoreplace, is_html)
        if self.wrap or col.wrap:
            return textwrap.wrap(text.strip(), width)

        return text.split("\n")

    def _get_style(self, style, has_colors, is_html):
        if not style:
            return "", ""
        if is_html:
            if style == HEADER:
                return "<b>", "</b>"
            if style == BOLD:
                return "<b>", "</b>"
            for s in ('BLACK', 'RED', 'GREEN', 'YELLOW', 'BLUE', 'MAGENTA', 'CYAN', 'WHITE', 'GRAY'):
                if style == globals()[s]:
                    return ("<span style='color: %s;'>" % s, "</span>")
                elif style == globals()['BG_' + s]:
                    return ("<span style='background-color: %s;'>" % HTML_BG_COLORS.get(s, s), "</span>")
        else:
            if has_colors:
                return style[0], style[1]
        return "", ""

    def render(self, text, width, col, row, has_colors, is_html):
        out = text

        if self.is_separator:
            out = _HTML_DASH * width if is_html and text[0] == "-" else text[0] * width
        elif is_html and (row.raw_html or col.raw_html):
            pass
        else:
            out = justify(text, width, self.is_left(col))
            if is_html:
                if len(text) > len(out):
                    out = "<span title='%s'>%s</span>" % (html_escape(text), html_escape(out))
                else:
                    out = html_escape(out)

        pfx0, sfx0 = self._get_style(self.style, has_colors, is_html)
        pfx1, sfx1 = self._get_style(row.style, has_colors, is_html)
        pfx2, sfx2 = self._get_style(col.style if col else None, has_colors, is_html)
        return pfx0 + pfx1 + pfx2 + out + sfx2 + sfx1 + sfx0


class RTableRow:
    def __init__(self, values, style=None, raw_html=False, top_bottom_colors=None):
        self.raw_values = values
        self.style = style
        self.raw_html = raw_html
        self.top_bottom_colors = top_bottom_colors  # True - higher is green, False - lower is green
        self.cells = []

    def init_cells(self, table):
        self.cells = []

        colcnt = table.get_col_cnt()

        if isinstance(self.raw_values, (str, )):
            val = self.raw_values
            if len(val) == 1:
                for n in range(0, colcnt):
                    self.cells.append(RTableCell(val, is_separator=True))
                return

            self.cells.append(RTableCell(val, colspan=colcnt))
            for c in range(1, colcnt):
                self.cells.append(RTableCell(None, colspan=0))
            return

        for val in self.raw_values:
            if isinstance(val, RTableCell):
                self.cells.append(val)
                for _n in range(1, val.colspan):
                    self.cells.append(RTableCell(None, colspan=0))
            else:
                self.cells.append(RTableCell(val))

        if self.top_bottom_colors in (True, False):
            self.init_top_bottom_colors(self.top_bottom_colors)
        elif table._has_top_bottom_colors_row:
            self.cells.append(RTableCell('MAX'))

    def init_top_bottom_colors(self, higher_is_better):
        vals = []
        for c in self.cells:
            try:
                _v = float(c.val)
            except Exception as e:
                continue
            if _v:
                vals.append(_v)

        if not len(vals):
            self.cells.append(RTableCell(0, style=BG_GRAY))
            return

        vmin = min(vals)
        vmax = max(vals)
        vminf = float(min(vals))
        vmaxf = float(max(vals))

        for c in self.cells:
            try:
                v = float(str(c.val).strip())
            except Exception as e:
                continue
            if not v or v == 0.0:
                continue
            if v > (vmaxf * 0.95) or v > (vminf + (vmaxf - vminf) * 0.8):
                c.style = BG_GREEN if higher_is_better else BG_RED
            elif float(v) < (vminf + (vmaxf - vminf) * 0.2):
                c.style = BG_RED if higher_is_better else BG_GREEN
            else:
                c.style = BG_YELLOW

        self.cells.append(RTableCell(vmax, style=BG_GRAY))

    def get_cell_width(self, table, colno):
        col = table.get_col(colno)
        width = col.width
        for n in range(colno + 1, colno + self.cells[colno].colspan):
            col = table.get_col(n)
            if col.hidden:
                continue
            width += len(col.separator) + col.width
        if not width:
            return len(str(self.cells[colno].val))
        return width

    def __str__(self):
        out = ["%s|%d|left=%s|%s" % (str(c.val), c.colspan, str(c.left)[0],
                                     "wrap" if c.wrap else "nowrap") for c in self.cells if self.cells]
        return str(out)


class RTable(RNode):
    def __init__(self, header=None, rows=None, autoreplace=None, autowidth=True, col_sep=TABLE_COL_SEP,
                 visible_lines=_VISIBLE_LINES, left_aligned_cols=None):
        """
        autowidth=True means table width will be aligned to 100%, 75%, 50% or 25% of the report width
        """
        RNode.__init__(self)
        self._columns = []
        self._rows = []
        self._styles = []
        self._final_width = 0
        self._autoreplace = autoreplace if autoreplace else {"0": "-"}
        self._autowidth = autowidth
        self._col_sep = col_sep
        self._visible_lines = visible_lines
        self._has_colors = has_colors(sys.stdout)
        self._has_top_bottom_colors_row = False
        self._left_aligned_cols = left_aligned_cols

        if header:
            self.add_header(header)

        if rows:
            for r in rows:
                self.add_row(r)

    def get_col_cnt(self):
        return len(self._columns)

    def get_row_cnt(self):
        return len(self._rows)

    def get_col(self, colno):
        return self._columns[colno]

    def get_row(self, rowno):
        return self._rows[rowno]

    def _adjust_width(self, bias):
        width, starvation = self._get_width()
        to_distribute = bias - width

        c_starvation = []
        for n in range(0, len(self._columns)):
            c = self._columns[n]
            c_starvation.append((n, max(0, c.width_max - c.width), c.width_max + len(c.separator)))

        if starvation:
            # give space to those who can't wrap first
            for wrap in (True, False):
                for n, s, w in sorted(c_starvation, key=lambda x: x[1], reverse=True):
                    if self._columns[n].wrap == wrap:
                        continue
                    distributed = s if to_distribute >= starvation else int(to_distribute * s / float(starvation))
                    to_distribute -= distributed
                    self._columns[n].width += distributed

        to_distribute_left = to_distribute

        if self._columns[n].left:
            total = float(width)
        else:
            total = width - self._columns[n].width
            total = float(total) if total > 0 else float(width)

        for n, s, w in sorted(c_starvation, key=lambda x: x[2], reverse=True):
            if not n and not self._columns[n].left:
                continue  # don't increase width of right-aligned first column
            distributed = int(min(to_distribute_left, round(to_distribute * w / total, 0)))
            to_distribute_left -= distributed
            self._columns[n].width += distributed

        if to_distribute_left and len(self._columns):
            self._columns[0].width += to_distribute_left

    def _get_columns(self):
        return [c for c in self._columns if not c.hidden]

    def _get_width(self):
        width, starvation = 0, 0
        n = 0
        for c in self._get_columns():
            if n:
                width += len(c.separator)
            starvation += max(0, c.width_max - c.width)
            width += c.width
            n += 1
        return width, starvation

    def _init_table_width(self, is_html=False):
        if self._final_width:
            return

        if self._has_top_bottom_colors_row:
            self._add_column(RTableCol("MAX"))

        for n in range(0, len(self._columns)):
            if self._left_aligned_cols and n in self._left_aligned_cols:
                self._columns[n].left = True

        max_row_width = 0  # special case for table w/o header and only with raw strings as rows

        for row in self._rows:
            raw_values = row.raw_values

            row.init_cells(self)

            # only iterable values affect table width
            if not isinstance(raw_values, _ITERABLE):
                max_row_width = max(len(str(raw_values)), max_row_width)
                continue

            for n in range(0, len(self._columns)):
                c = self._columns[n]
                if row.cells[n].colspan == 1:
                    c.adjust(row.cells[n].get_width(c, self._autoreplace, is_html))

        if max_row_width and len(self._columns) == 1 and self._columns[0].width == 0:
            self._columns[0].adjust(max_row_width)
            self._columns[0].left = True

        if self._autowidth:
            curr_width, starvation = self._get_width()
            target_width = self._get_parent_width()
            indent = len(self.add_indent("")) + len(_DEFAULT_INDENT)

            for b in [0.25, 0.5, 0.75, 1]:
                bias = int(target_width * b)
                if curr_width + starvation + indent > bias and b != 1:
                    continue
                if curr_width and curr_width < bias:
                    self._adjust_width(bias - indent)
                    break

        self._final_width, _ = self._get_width()
        return self._final_width

    def _add_column(self, column):
        column.colno = len(self._columns)
        self._columns.append(column)

    def add_row(self, values, style=None, top_bottom_colors=None):
        """
        supported 'values' format:
        - RTableRow object:             RTableRow()
        - a string:                     "some table-wide string"
        - a single-byte sring:          '-' - will be used as table separator
        - list of cells:                ['cell1', 'cell2'...]
        - list of RTableCell() objects: [RTableCell(), RTableCell(), ...]
        """
        self._final_width = False

        if isinstance(values, RTableRow):
            row = values
            values = row.raw_values
        elif not isinstance(values, _SUPPORTED_ROW_FMTS):
            raise RuntimeError("Unsupported row type: %s, val: %s" % (str(type(values)), str(values)))
        else:
            row = RTableRow(values, style, top_bottom_colors=top_bottom_colors)
            if isinstance(values, (str, )):
                if not self._columns:
                    self._add_column(RTableCol())

        if isinstance(values, (list, tuple)):
            if not self._columns:
                for v in values:
                    span = v.colspan if isinstance(v, RTableCell) else 1
                    for n in range(0, span):
                        self._add_column(RTableCol(separator=self._col_sep))
            else:
                span = 0
                for v in values:
                    span += v.colspan if isinstance(v, RTableCell) else 1
                if span != len(self._columns):
                    raise RuntimeError("columns number mismatch. It must be %d, but get %d: %s" %
                                       (len(self._columns), span, str([str(v) for v in values])))

        if row.top_bottom_colors is not None:
            self._has_top_bottom_colors_row = True

        self._rows.append(row)
        return self

    def add_header(self, columns, style=HEADER):
        """
        supported 'columns' format:
        - list of titles:              ['col1', 'col2'...]
        - list of tuples:              [('col1', col_width), ...]
        - list of tuples:              [('title', col_width, col_format), ...]
        - list of RTableCol():         [RTableCol(), RTableCol(), ...]
        - list of list of RTableCol()  [[RTableCol(), RTableCol(), ...], [RTableCol(), ...] - mutliline header
        """
        if not isinstance(columns, list) or len(columns) == 0:
            raise RuntimeError("add_header() function accepts list of objects, while given: %s" % str(columns))

        if isinstance(columns[0], list):
            columns_rows = columns
        else:
            columns_rows = [columns]

        for columns_row in columns_rows:
            cols = []
            for c in columns_row:
                if isinstance(c, RTableCol):
                    cols.append(c)
                elif isinstance(c, _ITERABLE) and len(c) >= 2:
                    cols.append(RTableCol(str(c[0]), width=c[1], format=c[2] if len(c) > 2 else None,
                                          separator=self._col_sep))
                else:
                    cols.append(RTableCol(str(c), separator=self._col_sep))

            # create columns if missing
            if not self._columns:
                colno = 0
                for col in cols:
                    c = col
                    for s in range(0, c.colspan):
                        self._add_column(c)
                        c = copy.copy(c)
                        c.colspan = 0

            # merge attributes to existing columns
            colno = 0
            for col in cols:
                if colno >= len(self._columns):
                    raise RuntimeError("Columns count mismatch in the same table:\n"
                                       "prev columns (%d) %s\nadding (%d) %s" %
                                       (len(self._columns),
                                        ["%s,colspan=%d" % (c.title, c.colspan) for c in self._columns],
                                        len(cols),
                                        ["%s,colspan=%d" % (c.title, c.colspan) for c in cols]))
                self._columns[colno] = col
                colno += col.colspan

            # build final row
            row = [RTableCell(c.title, colspan=c.colspan, wrap=c.wrap, left=c.left, bottom=c.bottom) for c in cols]
            self.add_row(row, style=style)

        self.add_row("-")

    def _render_line(self, rowno, is_html):
        row = self._rows[rowno]
        lines = []

        cellsrows = []
        max_cellsrows = 0

        first = True
        for n in range(0, len(row.cells)):
            c = self._columns[n]

            if c.hidden or row.cells[n].colspan == 0:
                cellsrows.append([])
            else:
                cell_lines = row.cells[n].get_lines(c, c.width, self._autoreplace, is_html)
                cellsrows.append(cell_lines)

            if max_cellsrows < len(cellsrows[-1]):
                max_cellsrows = len(cellsrows[-1])

        # apply bottom valign if needed
        for n in range(0, len(row.cells)):
            if row.cells[n].colspan and row.cells[n].bottom:
                c = cellsrows[n]
                while len(c) < max_cellsrows:
                    c.insert(0, None)

        for r in range(0, max_cellsrows):
            line = ""
            first = True
            for n in range(0, len(row.cells)):
                if row.cells[n].colspan == 0 or self._columns[n].hidden:
                    continue

                width = row.get_cell_width(self, n)
                if not width:
                    continue

                if first:
                    first = False
                else:
                    line += self._columns[n].separator

                v = cellsrows[n][r] if r < len(cellsrows[n]) else None
                if v is None:
                    line += " " * width
                    continue

                line += row.cells[n].render(v, width, self._columns[n], row, self._has_colors, is_html)

            lines.append(line)

        return lines

    def _get_lines(self, is_html=False, start_row=0):
        self._init_table_width()

#       print "\nCOLUMNS:\n   " + "\n   ".join([str(c) for c in self._columns])
#       print "ROWS:\n   " + "\n   ".join([str(r) for r in self._rows])

        lines = []
        for rowno in range(start_row, len(self._rows)):
            lines += self._render_line(rowno, is_html)

        return lines

    def render_html(self, start_row=0):
        lines = self._get_lines(is_html=True, start_row=start_row)

        out = "<pre>" + "\n".join(lines[:self._visible_lines]) + "</pre>"
        if len(lines) > self._visible_lines:
            out += """<div class='table_tail_container'>
                      <div class='table_tail_toggle_show' onclick='toggle(this);'>&darr; View the rest %d lines...</div>
                      <div class='table_tail_toggle_hide' onclick='toggle(this);'>&uarr; Collapse the tail</div>
                      <div class='table_tail_body'><pre>%s</pre></div>
                      </div>
                   """ % (len(lines) - self._visible_lines, "\n".join(lines[self._visible_lines:]))
        return out

    def render_text(self, start_row=0):
        if start_row:
            return self.add_indent("\n".join(self._get_lines(start_row=start_row)))
        return "\n" + self.add_indent("\n".join(self._get_lines())) + "\n"

    def render_json(self):
        self._init_table_width()
        out = {}
        out['node_type'] = 'table'

        data = []

        for r in self._rows:
            row = []
            for n in range(0, len(self._columns)):
                v = r.cells[n].get_text(self._columns[n], self._autoreplace, False)
                row.append(v)
            data.append(row)
        out['data'] = data
        return out


class RSqlQueryList(RNode):
    def __init__(self, db_con):
        RNode.__init__(self)
        self._db_con = db_con
        self.section_header_note = []
        if len(self._db_con.history):
            self.section_header_note = "%.1f sec" % sum([q.dur_sec for q in self._db_con.history])
        self._content = self._db_con.history if self._db_con else ""
        self._db_con.clear_history()

    def render_html(self):
        if not self._content:
            return ""
        out = [(("--query took %3.1f sec\n" % q.dur_sec) + html_escape(q.format()) + "\n") for q in self._content]
        out = "<pre><code class='sql'>" + "\n".join(out) + "</code></pre>"
        t = RText(out, raw_html=True)
        return "<section class='header_notes_body'><div class='sql_query_list'>%s</div></section>" % t.render_html()

    def render_text(self):
        t = RTable()
        for q in self._content:
            t.add_row("---- query took %3.1f sec ----" % q.dur_sec)
            t.add_row(q.format())
            t.add_row("")
        return self.add_indent(t.render_text()) + "\n"

    def render_json(self):
        out = {}
        out['node_type'] = 'sql_query_list'
        out['data'] = [q.format() for q in self._content]
        return out


def __coverage__():
    # $ coverage run ./lib/pgs_report.py
    # $ coverage report -m

    class DBQuery:
        def __init__(self):
            self.query = "SELECT * FROM table"
            self.dur_sec = 1.0

        def format(self):
            return self.query

    class DBcon:
        def __init__(self):
            self.history = [DBQuery(), DBQuery()]

        def clear_history(self):
            pass

    stdout = sys.stdout

    assert justify("abc", 0) == "abc", "Justify failed"

    C = RTableCol
    r = Report()

    for autowidth in (False, True):
        s = RSection("Example #%d, rich table, autowidth=%s" % (3 if autowidth else 2, autowidth))

        sys.stdout = str
        t = RTable(autowidth=autowidth)
        sys.stdout = stdout

        cols = [["", "", "", "", C("aaaaaaaaaaaaaaaaaaaaa", raw_html=True), ""]]
        cols.append([("COLUMN #1", -20),
                     ("COLUMN #2", None, "%1.f"),
                     C("WRAPPABLE GREEN COLUMN", 15, " | ", style=GREEN, wrap=True),
                     "COLUMN\n## 4",
                     ("HIDDEN COLUMN", 0),
                     ""])

        t = RTable(autowidth=autowidth, header=cols, rows=[["Some bold string", 12.5, 'some text', 0, 1, ""]])

        t.add_row(["Some longer string", 12.7, 'maybe even more text', 21.0, 2, ""], style=("1", '2'))
        t.add_row(["Some very long string that doesn't fit into cell", 212.5,
                   'well, crazy length column', decimal.Decimal(32213.01), 3, ""], style=BOLD)
        t.add_row(["Some red string", 21.99, None, datetime.datetime.now(), 5, ""])
        t.add_row([0, 0.0, 0, 0, 0.0, 0], top_bottom_colors=True)
        try:
            t.add_row(["Some bad string", 1, 2, 3, 4, 5, 6, 7, 8])
        except Exception as e:
            pass

        try:
            t.add_row(float(1.2))
        except RuntimeError as e:
            pass

        s2 = s.add_section("Subsection")
        s3 = s2.add_section("Subsection")

        s.add_node(RSqlQueryList(DBcon()))
        s.add_node(t)

        s.add_table()

        t = RTable()
        try:
            t.add_header([[C("1"), C("2"), C("3-5", colspan=3)], [C("1-2", colspan=2), C("3"), C("4")]])
        except RuntimeError as e:
            pass

        t2 = RTable()
        try:
            t2.add_header([[C("1"), C("2"), C("3-4", colspan=2)], [C("1-3", colspan=3), C("4"), C("5")]])
        except RuntimeError as e:
            pass
        try:
            t2.add_header("xxxx")
        except RuntimeError as e:
            pass

        for n in range(0, 100):
            t.add_row(["A row", n, 1, 2, RTableCell(3, style=BG_RED)])

        str(t.get_col(0))
        str(t.get_row(0))

        s.add_node(t)
        s.add_text("some text")
        r.add_node(s)

    r.add_node(RFooter())

    r.render_text()
    r.render_html()
    r.render_json()

    for fmt in (FORMAT_TEXT, FORMAT_HTML, FORMAT_JSON, 123):
        out = StringIO()
        sys.stdout = out
        try:
            r.flush(format=fmt, filestream=sys.stdout)
            r.flush(format=fmt, filestream=sys.stdout)
            r.flush(format=fmt)
        except RuntimeError as e:
            pass

    r.reset_flushed()
    sys.stdout = stdout


def __example__():
    C = RTableCol

    r = Report()
    s = r.add_section("Example #1, header-less table")

    t = RTable(autowidth=False)
    t.add_header(["SIMPLE HEADER"])
    t.add_row(["Single cell table"])
    s.add_node(t)

    s.add_node(RTable().add_row(RTableRow(["a", "b", "c"])))

    t = RTable()
    t.add_header([[C("COL"), C("coLSpan=3", colspan=3, left=True)],
                  [C("#1"), C("COL#2"), C("COL#3", width=6), C("COL#4")]])

    t.add_row([123, 456, 7890, 21.2])
    t.add_row([91824, 150000, 0, 21000], top_bottom_colors=True)
    t.add_row([0, 12, None, None], top_bottom_colors=True)
    t.add_row([RTableCell('blue', style=BLUE), RTableCell('green', style=GREEN), None, None])

    s.add_node(t)
    s.add_node(RText("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut\n"
                     "labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco\n"
                     "laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in\n"
                     "voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat\n"
                     "cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."))

    s2 = RSection("A subsection")
    s.add_node(s2)

    s2.add_node(RTable().add_row(["Test", "row", "1"]).add_row(["Test", "row", 2]))

    s3 = RSection("One more subsection...")
    s2.add_node(s3)
    s3.add_node(RTable(autowidth=False).add_row(["a very long column", "cell value", "1000"]).add_row(["xxx", "yyy", 2]))

    s.add_node(s2)
    r.add_node(s)

    for autowidth in (False, True):
        s = RSection("Example #%d, rich table, autowidth=%s" % (3 if autowidth else 2, autowidth))

        t = RTable(autowidth=autowidth)

        cols = [["", "", "", "", "", C("COLUMN WITH COLSPAN=2", colspan=2)]]
        cols.append([("COLUMN #1", -20),
                     ("COLUMN #2", None, "%1.f"),
                     C("WRAPPABLE GREEN COLUMN", 15, " | ", style=GREEN, wrap=True),
                     "COLUMN\n## 4",
                     ("HIDDEN COLUMN", 0),
                     C("DATE", format=lambda a, b, c, d: a.strftime("%Y-%m-%d")),
                     C("TIME", format=lambda a, b, c, d: a.strftime("%H:%M"))
                     ])

        now = datetime.datetime.now()

        t = RTable(autowidth=autowidth, header=cols)

        t.add_row(["Some bold string", 12.5, 'some text', 0, 1, now, now], style=BOLD)
        t.add_row(["Some string with HTML-entities", 12.5, 'some \'<b>text', 0, 1, now, now])
        t.add_row(["String with obfuscation", 12.5, obfuscate('Some Text'), 5, 1000, now, now])
        t.add_row(["Some longer string", 12.7, 'maybe even more text', 21.0, 2, now, now])
        t.add_row("Some very long string across the table #1")
        t.add_row("Some very long string across the table #2")
        t.add_row(["Some very long string that doesn't fit into cell", 212.5,
                   'well, crazy length column', 32213.02, 3, now, now])
        t.add_row("-")
        t.add_row(["Some multi-string\n text with '\\n'", 312.0, '', 0.5, 4, now, now])
        t.add_row("-")
        t.add_row(["Some red string", 21.99, None, 0.0, 5, now, now], style=RED)

        s.add_node(t)
        r.add_node(s)

    r.add_node(RFooter())
    r.flush(format=FORMAT_TEXT, filestream=sys.stdout)


if __name__ == "__main__":
    __example__()
    __coverage__()
