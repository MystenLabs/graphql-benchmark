# README

Scripts in Clojure for bulk-loading tables, uses the following
techniques to speed up loads:

- Use a thread pool to issue multiple transactions to the DB in
  parallel.
- Fill partitions before they get attached to the main table.
- Create tables that are going to be loaded into without any
  constraints or indices so that bulk-loading can focus on writing
  pages to the table, rather than updating indices (indices and
  constraints are added later).
- Disabling auto-vacuuming, as there will be no rows that can be
  cleaned up (reset before attaching partitions).
- Chunk up work in a way that leverages the source table indices and
  the partitioning of the destination table.

## Use

Install Clojure with homebrew:

``` shell
brew install clojure
```

For best results, open it in an interactive editing environment, like:

- CIDER for Emacs
- Conjure for (Neo)vim
- Calva for VSCode

So you can load the file and interact with it via a REPL from your
editor, otherwise you can load the script into a REPL at the command-
-line:

``` shell
bulk_load$ clj -i src/versions.clj -r
```

``` clojure
user=> (use 'versions)
user=> db ;; etc...
```

The database connection can be configured by setting various
environment variables before loading the scripts:

``` shell
DBNAME=defaultdb
DBHOST=localhost
DBPORT=5432
DBUSER=postgres
DBPASS=postrespw
```
