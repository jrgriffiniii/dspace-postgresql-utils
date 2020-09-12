# DSpace PostgreSQL Utilities
*Migration and Diagnostic Tools for DSpace*

## Requirements

- [Ruby](https://www.ruby-lang.org/en/downloads/releases/) release 2.5.8 (or later)
- [Bundler](https://rubygems.org/gems/bundler)

## Installation

```bash
bundle install
```

## Configuration

### Preparing the Databases

Retrieve a database export using the [pg_dump](https://www.postgresql.org/docs/12/app-pgdump.html) utility. Then please proceed with the following:

```bash
# The --username argument may also be your personal account, as this is the default superuser in macOS environments
createuser --host=localhost --port=5432 --username=postgres --createdb dspace

createdb dspace_staging --owner=dspace --username=dspace
psql --host=localhost --port=5432 dspace_staging dspace < dspace_staging_export.sql

createdb dspace_production --owner=dspace --username=dspace
psql --host=localhost --port=5432 dspace_production dspace < dspace_production_export.sql
```

It may also be necessary to use [pg_restore](https://www.postgresql.org/docs/12/app-pgrestore.html) in order to restore the database from the export:

```bash
pg_restore --host=localhost --port=5432 --username=dspace --dbname=dspace_production --format=custom --no-owner --no-privileges --verbose dspace_production_export.sql.c
```

Then please edit the database configuration in `config/databases.yml`:

```yaml
source_database:
  host: 'localhost'
  port: 5432
  name: 'dspace_staging'
  user: 'dspace'
destination_database:
  host: 'localhost'
  port: 5432
  name: 'dspace_production'
  user: 'dspace'
```

### The Command-Line Interface

This project uses [thor](http://whatisthor.com/) to provide a CLI. In order to list the available commands, please invoke the following:

```bash
bundle exec thor list
```

In order to invoke a task (e. g. an Item migration task), please invoke the following:

```bash
bundle exec thor dspace:migrate_items_by_metadata --metadata-field=pu.date.classyear --metadata-value=2020
```
