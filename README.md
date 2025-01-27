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
To use this driver, configure your database client to connect via the custom JDBC driver and specify the pgweb proxy endpoint. Further details on setup and configuration will be added in future updates.

## Limitations
As this is an experimental implementation, certain advanced JDBC features may not be fully supported yet. Contributions and feedback are welcome!

