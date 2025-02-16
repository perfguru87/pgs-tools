# pgs-tools

pgs-tools is a collection of tools for PostgreSQL system administration.

# Installation

## Using pip and virtual environment

1. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install the package:

```bash
pip install -r requirements.txt
```

3. Install the package:

```bash
pip install -e .
```


# Usage

## Common tools parameters

All tools accept the following parameters:

- `-v`, `--verbose`: Enable verbose mode.
- `-h`, `--help`: Show help message and exit.
- `--db-host=DB_HOST`: hostname/IP (or '/tmp/' for local socket connection)
- `--db-port=DB_PORT`: database port (default 5432)
- `--db-name=DB_NAME`: database name
- `--db-user=DB_USER`: database username
- `--db-pass=DB_PASS`: database password
- `--db-ssl`: enable SSL connection


## Tools

### pgs-bench

pgs-bench is a simple benchmarking tool that measures basic PostgreSQL operations performance. It helps establish baseline performance metrics for fundamental database operations.

Key features:
- Tests fundamental database operations:
  - Sequential SELECT performance
  - Sequential INSERT + COMMIT performance
- Reports operations per second
- Configurable test duration
- Performance threshold validation
- Automatic good/bad performance detection

Usage:
```bash
pgs-bench [options]
```

Options:
- `-v`, `--verbose`: Enable verbose mode
- `-t`, `--testtime=SECONDS`: Test duration in seconds (default: 5)

Example:
```bash
# Run default 5-second benchmark
pgs-bench

# Run 10-second benchmark with verbose output
pgs-bench -t 10 -v
```

Note: The tool creates temporary test tables during benchmarking and removes them afterward.


### pgs-vacuum

pgs-vacuum is a tool for detecting and cleaning bloated PostgreSQL tables. It helps identify tables that have excessive bloat (wasted space) and can perform VACUUM FULL operations to reclaim disk space.

Key features:
- Detects tables with bloat above specified thresholds
- Shows detailed bloat statistics including:
  - Bloat size in MB
  - Table size before/after potential vacuum
  - Bloat percentage
  - Row counts
  - Last vacuum/analyze timestamps
- Interactive mode to select which tables to vacuum
- Supports both PostgreSQL 12+ and older versions

Usage:
```bash
pgs-vacuum [options]
```

Options:
- `-v`, `--verbose`: Enable verbose mode
- `-y`, `--yes`: Automatically vacuum all problematic tables without prompting
- `-a`, `--vacuum`: Perform the vacuum operation (without this flag, only analysis is done)
- `-m`, `--bloat-mb=MB`: Table bloat threshold in megabytes (default: 100)
- `-p`, `--bloat-perc=PERCENT`: Table bloat threshold in percent (default: 50)
- `-t`, `--vacuum-table=TABLE`: Vacuum specific table and exit (can be specified multiple times)

Example:
```bash
# Analyze tables with >50% bloat and >100MB wasted space
pgs-vacuum

# Vacuum all problematic tables automatically
pgs-vacuum --vacuum --yes

# Vacuum specific tables
pgs-vacuum --vacuum-table mytable1 --vacuum-table mytable2
```

### pgs-warmupper

pgs-warmupper is a tool for monitoring and warming up PostgreSQL tables and indexes by reading them into the operating system's page cache. It helps improve query performance by ensuring frequently accessed database objects are cached in memory.

Key features:
- Monitors table and index usage in real-time
- Automatically detects and warms up frequently accessed objects
- Shows detailed statistics including:
  - Read activity since monitoring started
  - Object sizes on disk
  - Warmup time and read rates
- Supports both tables and their indexes
- Can warm up specific relations on demand
- Dry-run mode for testing

Usage:
```bash
pgs-warmupper [options]
```

Options:
- `-v`, `--verbose`: Enable verbose mode
- `-d`, `--delay=SECONDS`: Delay between database polls (default: 2)
- `-n`, `--count=N`: Exit after N iterations (default: run indefinitely)
- `-r`, `--relation=NAME`: Comma-separated list of tables/indexes to warmup and exit
- `--dry-run`: Skip actual file warmup (testing mode)
- `-t`, `--threshold=MB`: Data read threshold in MB to trigger warmup (default: 1)

Example:
```bash
# Monitor and auto-warmup with default settings
pgs-warmupper

# Warm up specific tables/indexes
pgs-warmupper -r "mytable,myindex"

# Monitor with 5-second intervals, exit after 10 iterations
pgs-warmupper -d 5 -n 10

# Use 10MB threshold for auto-warmup
pgs-warmupper -t 10
```

Note: The tool requires appropriate filesystem permissions to read database files directly. For optimal operation, run it on the same machine as the PostgreSQL server.

### pgs-top

pgs-top is an interactive monitoring tool that provides real-time statistics about PostgreSQL table activity. It's similar to the Unix 'top' command but specialized for PostgreSQL table operations.

Key features:
- Real-time monitoring of table activity
- Interactive interface with sortable columns
- Detailed statistics including:
  - Write operations (inserts/updates/deletes)
  - Index and sequential scan rates
  - Lock information
  - Row counts
- Multi-database support
- Customizable refresh rate
- Schema filtering capability

Usage:
```bash
pgs-top [options]
```

Options:
- `-v`, `--verbose`: Enable verbose mode
- `-d`, `--delay=SECONDS`: Delay between updates (default: 1 second)
- `-n`, `--count=N`: Exit after N iterations
- `-a`, `--abs`: Show absolute values instead of rates
- `-s`, `--sort=COLUMN`: Sort by column (default: Write)
- `-S`, `--schema=SCHEMA`: Monitor only specified schema

Available columns:
- `Table`: Table name
- `DB`: Database name
- `Write`: Total writes (inserts + updates + deletes) per second
- `Ins`: Insertions per second
- `Upd`: Updates per second
- `Del`: Deletions per second
- `UpdIdx`: Index updates per second
- `IdxScan`: Index scans per second
- `SeqScan`: Sequential scans per second
- `SeqRows`: Rows fetched by sequential scans per second
- `Locks`: Number of processes waiting for locks
- `Reltuples`: Approximate row count

Interactive keys:
- `Left/Right`: Change sort column
- `p`: Pause/resume updates
- `Space`: Force refresh
- `q`: Quit

Example:
```bash
# Basic monitoring with default settings
pgs-top

# Monitor specific schema with 5-second updates
pgs-top -S public -d 5

# Sort by sequential scans, show absolute values
pgs-top -s SeqScan -a
```

Note: The tool requires appropriate PostgreSQL permissions to access system statistics tables.

### pgs-stat

pgs-stat is a real-time PostgreSQL statistics monitoring tool that provides detailed metrics about database activity across multiple categories.

Key features:
- Real-time monitoring of database metrics
- Comprehensive statistics groups:
  - Database size
  - Write operations (inserts/updates/deletes)
  - Table scans (index and sequential)
  - Cache performance
  - Lock information
  - Transaction counts
  - Process states
  - I/O wait times
- Support for absolute and rate values
- Configurable update intervals
- Row count threshold filtering

Usage:
```bash
pgs-stat [options]
```

Options:
- `-v`, `--verbose`: Enable verbose mode
- `-d`, `--delay=SECONDS`: Delay between updates (default: 2)
- `-n`, `--count=N`: Exit after N iterations
- `-a`, `--abs`: Show absolute values instead of rates
- `-r`, `--scan-threshold=N`: Skip tables with fewer rows for scan stats (default: 5000)

Statistics groups:
- Database: Size in KB
- Write Ops: INS, UPD, DEL operations
- Scan: IDX (index), SEQ (sequential), SEQ% (sequential ratio)
- Cache: HIT and MISS rates
- Locks: Lock count and deadlocks
- Transactions: COMMIT and ROLLBACK counts
- Processes: Idle and active counts
- I/O: Read and write wait percentages

Example:
```bash
# Monitor with default settings
pgs-stat

# Show absolute values with 5-second updates
pgs-stat -a -d 5

# Monitor large tables only (>10000 rows)
pgs-stat -r 10000

# Exit after 100 iterations
pgs-stat -n 100
```

Note: Some statistics (like I/O wait times) are only available in PostgreSQL 9.2+.

### pgs-ps

pgs-ps is a PostgreSQL process monitoring tool that provides detailed information about database sessions and their activities.

Key features:
- Session statistics aggregation
- Detailed process information
- Transaction duration tracking
- Lock monitoring
- Configurable session filtering
- Query text display

Usage:
```bash
pgs-ps [options]
```

Options:
- `-t`, `--idle-in-transaction`: Show idle in transaction sessions
- `-i`, `--idle`: Show idle sessions
- `-W`, `--width=N`: Number of characters in query text (default: 37)

Output sections:
1. Session Statistics:
   - Database and username
   - Session state
   - Lock information
   - Session/transaction duration
   - Process counts

2. Session Details:
   - Process ID
   - Client information
   - Session start time
   - Query runtime
   - Current query text
   - Lock status

Example:
```bash
# Show active sessions only
pgs-ps

# Include idle-in-transaction sessions
pgs-ps -t

# Show all sessions including idle ones
pgs-ps -t -i

# Display longer query texts
pgs-ps -W 100
```

Note: Output format varies slightly between PostgreSQL versions due to changes in system catalogs.

### pgs-info

pgs-info is a comprehensive PostgreSQL database analysis tool that provides detailed information about database configuration, performance, and health metrics.

Key features:
- Database configuration analysis
- Detailed statistics reporting:
  - Database-wide statistics
  - Background writer performance
  - Schema and table sizes
  - Table and index bloat analysis
  - Missing and ineffective indexes
  - Transaction statistics
  - Replication status
- Support for PostgreSQL 9.0+
- Report comparison functionality
- Customizable output depth

Usage:
```bash
pgs-info [options] [REPORT_FILE1 [REPORT_FILE2 ...]]
```

Options:
- `-v`, `--verbose`: Enable verbose mode
- `-l`, `--lines=N`: Number of lines in output (default: 40)
- `-t`, `--tran-threshold=SECONDS`: Long transactions threshold (default: 120)
- `--min-tab-size=BYTES`: Minimum table size for analysis (default: 32KB)
- `-C`, `--compare`: Compare multiple report files
- `--html=FILE`: Output report in HTML format

Report sections:
- Database settings and configuration
- Database statistics and metrics
- Background writer statistics
- Schema and table sizes
- Table and index bloat analysis
- Missing and ineffective indexes
- Transaction statistics
- Vacuum statistics
- Replication information

Example:
```bash
# Generate basic database report
pgs-info

# Generate detailed report with more lines
pgs-info -l 100

# Compare two database reports
pgs-info -C report1.txt report2.txt

# Generate HTML report
pgs-info --html=report.html
```

Note: Some statistics require superuser privileges for full access to system information.