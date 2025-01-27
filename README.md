# pgweb-jdbc-driver

A proof-of-concept JDBC driver for executing queries via an HTTP proxy (pgweb).

## Overview
This driver is designed to work with GUI database clients such as DataGrip, IntelliJ IDEA, or any database client that supports custom JDBC drivers. It enables seamless interaction with PostgreSQL databases through an HTTP-based proxy.

## Features
- Supports essential functionality for database introspection (verified with DataGrip 2021.2.4)
- Executes basic SQL queries
- Limited support for prepared statements
- Partial support for returned types (`ResultSet::MetaData`)

## Usage
To use this driver, configure your database client to connect via the custom JDBC driver and specify the pgweb proxy endpoint. 

Provide connection string in the following format
`jdbc:pgweb:<pgweb base path with protocol>:postgres://<user>:<password>@<db-host>:5432/<dbname>`

Example: `jdbc:pgweb:http://localhost:8081:postgres://tenebras:password@localhost:5432/pgwebtest`

Debug level session log stored here: `/tmp/pgweb-jdbc.log`

## Limitations
As this is an experimental implementation, certain advanced JDBC features may not be fully supported yet. Contributions and feedback are welcome!

