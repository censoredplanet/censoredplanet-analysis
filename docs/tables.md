# Bigquery Tables

## Base tables

These tables are directly derived from the raw Censored Planet data. They are
used as a base for further analysis tables.

- [Base Tables](base_tables.md)

## Derived Tables

These tables are created from the base tables. They drop some data and also do
some common pre-processing, making them faster, easier and less expensive to
use.

- [Merged Scans Table](merged_scans_table.md) This table merges the data from
  the base tables and drops some less useful fields.
- [Merged Reduced Scans Table](merged_reduced_scans_table.md) This table merges
  the data from the base tables, but uses a set of subtables to reduce the
  total amount of data read. It also includes an error analysis field.
