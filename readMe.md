
## Overview
ELECTONICA DATAWARE HOUSE:
The application connects to a MySQL database containing sales transaction data. It processes the data in batches, enriches each transaction with additional attributes from a master data table, and then loads the transformed data into a data warehouse. 
It uses Hybird Join By  handle a stream of sales transactions efficiently.

## Dependencies

IDE Such as Elicps Or Intelli-J
MySQL database server
MySQL Connector/J library

## Setup

# Database Setup:

Ensure that a MySQL database is set up with the necessary tables (transactions, master_data, time, product, customer, store, supplier, sales) as defined in the code.
USE Password and Username that Matches to you DataBase REPLACE username: "root" and password: "ahmii123"

# Java Environment:

Install Java Eclips or IntelliJ

# Dependencies:

Include the MySQL Connector/J library in your project. You can download it from the official MySQL website 

## Usage

Compile and Run:
Extract the File 
Create a New project, Import the Main.java file
Compile the Java code using a Java compiler(ELICPS or IntelliJ).
Run the compiled program.

## Execution:

The program connects to the MySQL database, reads sales transactions from the transactions table, and processes them in batches of 1000 with the Delay of 2 seconds.
Master data attributes are loaded and joined for each transaction.
The joined data is then shipped to the ELECTRONICA_DW data warehouse.

## Database Schema

# transactions: Represents the sales transactions.
# master_data: Contains master data attributes related to products, suppliers, stores.
# ELECTRONICA_DW: Data warehouse schema with tables for time, product, customer, store, supplier, and sales.