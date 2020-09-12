# DSpace PostgreSQL Utilities
*Migration and Diagnostic Tools for DSpace*

## Requirements

- [Ruby](https://www.ruby-lang.org/en/downloads/releases/) release 2.5.8 (or later)
- [Bundler](https://rubygems.org/gems/bundler)

## Installation

```bash
bundle install
```

## Usage

Please edit the database configuration in `config/databases.yml`:

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

### Command-Line Interface

This project uses [thor](http://whatisthor.com/) to provide a command-line interface. In order to list the available commands, please invoke the following:

```bash
bundle exec thor list
```

In order to invoke a task (e. g. a collection migration task), please invoke the following:

```bash
bundle exec thor dataspace:student_theses_migrate -y 2020
```
