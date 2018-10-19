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
The MySQL class's methods are broken down into several categories and inherited via sub-modules.  All methods (with a few exceptions) are inherited to the core MySQL class, exposing compiled methods through a single class.

### Manipulate
SQL commands that deal with the manipulation of data present in database.

| Class | Method | Description |
| --- | --- | --- |
Select | select_all | Query all rows and columns from a table
Select | select_distinct | Query distinct values from a table
Select | select | Query every row and only certain columns from a table
Select | select\_all_join (coming soon) | Left join all rows and columns from two tables where a common value is shared
Select | select_limit | Run a select query with an offset and limit parameter
Select | select_where | Query certain columns from a table where a particular value is found
Insert | insert_uniques | Insert multiple rows into a table that do not already exist
Insert | insert | Insert a single row into a table
Insert | insert_many | Insert multiple rows into a table
Update | update | Update the values of a particular row where a value is met
Update | update_many | Update the values of several rows
Delete | delete | Delete existing rows from a table


### Operations
SQL commands that deal with the definitions of data present in database.

| Class | Method | Description |
| --- | --- | --- |
Operations | backup_database | Create a backup of a database
Operations | create_table | Generate and execute a create table query by parsing a 2D dataset
Operations | execute_script | Wrapper method for SQLScript class
Operations | script | Wrapper method providing access to the SQLScript class's methods and properties
Clone | copy_database | Copy a database's content and structure
Compare | compare_dbs | Compare the tables and row counts of two databases
Compare | compare_schemas | Compare the structures of two databases
Compare | compare_data | Compare the data stored in two databases
Remove | truncate | Empty a table by deleting all of its rows
Remove | truncate_database | Drop all tables in a database
Remove | drop | Drop a table from a database
Remove | drop_empty_tables | Drop all empty tables in a database
Remove | truncate | Empty a table by deleting all of its rows


#### Structure
Properties and methods that return metadata about a MySQL table(s).

| Class | Method | Description |
| --- | --- | --- |
Structure | tables | Retrieve a list of tables in the connected database
Structure | databases | Retrieve a list of databases that are accessible under the current connection
Structure | get\_unique\_column | Determine if any of the columns in a table contain exclusively unique values
Structure | count\_rows\_duplicates | Get the number of rows that do not contain distinct values
Structure | count\_rows\_all | Get the number of rows for every table in the database
Structure | count_rows | Get the number of rows in a particular table
Structure | count\_rows\_all | Get the number of rows for every table in the database
Structure | count\_rows\_all\_distinct | Get the number of distinct rows for every table in the database
Structure | count\_rows\_distinct | Get the number distinct of rows in a particular table
Structure | get\_duplicate\_vals | Retrieve duplicate values in a column of a table
Alter | add_column | Add a column to an existing table
Alter | drop_column | Remove a column to an existing table
Alter | add_comment | Add a comment to an existing column in a table
PrimaryKey | get\_primary\_key\_vals | Retrieve a list of primary key values in a table
PrimaryKey | get\_primary\_key | Retrieve the column which is the primary key for a table
PrimaryKey | set\_primary\_key | Create a Primary Key constraint on a specific column when the table is already created
PrimaryKey | set\_primary\_keys\_all | Create primary keys for every table in the connected database
PrimaryKey | drop\_primary\_key | Drop a Primary Key constraint for a specific table
ForeignKey | set\_foreign\_key | Create a Foreign Key constraint on a column from a table
Definition | get\_table\_definition | Retrieve a CREATE TABLE statement for an existing table
Definition | get\_column\_definition\_all | Retrieve the column definition statement for a column from a table
Definition | get\_column\_definition | Retrieve the column definition statement for a column from a table
Schema | show_schema | Print schema information
Schema | get_columns | Retrieve a list of columns in a table
Schema | get_schema\_dict | Retrieve the database schema in key, value pairs for easier references and comparisons
Schema | get_schema | Retrieve the database schema for a particular table


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
