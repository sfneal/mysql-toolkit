# MySQL-toolkit

[![GuardRails badge](https://badges.production.guardrails.io/mrstephenneal/mysql-toolkit.svg)](https://www.guardrails.io)

Development toolkit for building applications that interact with a MySQL database.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### PyPi installation

PyPi distribution

```
pip install mysql-toolkit
```

## Usage

MySQL-tookit aims to provide an easy to use MySQL dependency that allows developers to integrate MySQL database's with their Python applications.

The entire MySQL-toolkit module can be utilized through a single import.  To initialize a MySQL instance, simply provide a dictionary of MySQL database connection parameters with your call to MySQL within a context manager.  Wrap all method and property calls with a context manager in order to automate connecting a disconnecting to a database.

```python
from mysql.toolkit import MySQL

# Database connection parameters
config = {
    "database": "xxxnameofyourdatabasexxx",
    "host": "xxxhosturlxxx",
    "password": "xxxyourpasswordxxx",
    "port": xxxhostportxxx,
    "raise_on_warnings": true,
    "user": "xxxyourusernamexxx"
}

# Establish a connection and execute queries
with MySQL(config) as sql:
	# Select all rows from 'tablename'
	results = sql.select_all('tablename')
	
	# Update the row in 'anothertable' where the column 'id' equals 20421
	sql.update('anothertable', ['column1', 'column2'], ['value1', 2], ('id', 20421)
	
	# Retrieve a dictionary containing table, row_count key/values for every table in the database
	counts = sql.count_rows_all()

# Query will fail and raise an error because the database connection is only maintained inside with context
tables = sql.tables()  # Retrieve all tables in the database
```

## User API
The MySQL class's methods are broken down into three categories and inherited via sub-modules.

#### core
Method's that allow for programatic execution of basic SQL queries.

| Method | Description |
| --- | --- |
select | Query every row and only certain columns from a table
select_all | Query all rows and columns from a table
select\_all_join (coming soon) | Left join all rows and columns from two tables where a common value is shared
select_where | Query certain columns from a table where a particular value is found
insert | Insert a single row into a table
insert_many | Insert multiple rows into a table
update | Update the values of a particular row where a value is met
truncate | Empty a table by deleting all of its rows
drop | Drop a table from a database

#### results
Properties and methods that return metadata about a MySQL table(s).

| Method | Description |
| --- | --- |
tables | Retrieve a list of tables in the connected database
databases | Retrieve a list of databases that are accessible under the current connection
get\_primary_key | Retrieve the column which is the primary key for a table
get\_primary_key\_vals | Retrieve a list of primary key values in a table
get_schema | Retrieve the database schema for a particular table
count_rows | Get the number of rows in a particular table
count\_rows_all | Get the number of rows for every table in the database

#### advanced
Methods provide functionality that is not (easily) possible with standard MySQL queries

| Method | Description |
| --- | --- |
create_table (coming soon) | Generate and execute a create table query by parsing a 2D dataset
drop\_empty_tables | Drop all empty tables in a database
**insert_uniques** | Insert multiple rows into a table that do not already exist
update_many | Update the values of several rows
truncate_database | Drop all tables in a database
execute_script | Execute a sql file one command at a time.
script | Perform operations with a SQL script
compare_dbs | Compare the tables and row counts of two databases


## Built With

* [differentiate](https://github.com/mrstephenneal/differentiate) - Compare multiple data sets and retrieve the unique, non-repeated elements.
* [mysql-connector](https://dev.mysql.com/doc/connector-python/en/) - Self-container driver for communication with MySQL servers
* [looptools](https://github.com/mrstephenneal/looptools) - Logging output, timing processes and counting iterations
* [sqlparse](https://github.com/andialbrecht/sqlparse) - A non-validating SQL parser module for Python
* [tqdm](https://github.com/tqdm/tqdm) - A fast, extensible progress bar for Python

## Contributing

Please read [CONTRIBUTING.md](https://github.com/mrstephenneal/mysql-toolkit/contributing.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/mrstephenneal/mysql-toolkit).

## Authors

* **Stephen Neal** - *Initial work* - [mysql-toolkit](https://github.com/mrstephenneal/mysql-toolkit)


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
