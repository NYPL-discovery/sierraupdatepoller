# SierraUpdatePoller

This package is intended to be used to retrieve Bib/Item IDs (depending on the values provided for environment variables) that have been updated by polling the Sierra API and then posting those IDs to a Kinesis stream for further processing.

## Installation

Configure environment variables for Redis, AWS access, and Sierra API access (see `System.getenv of EnvironmentConfig.java` for variables used).

## Requirements (check pom.xml for version information)

* Redis
* Spring Boot
* Apache Camel
* Apache Avro

## Features

* Polls the [Sierra Item API](https://ilsstaff.nypl.org/iii/sierra-api/swagger/index.html#!) at a fixed interval as specified in `HarvesterConstants`
* Posts Bib/Item IDs that have updated to Kinesis Streams specified in the environment using Avro encoding.


## Usage

TBD

## Tests

TBD