# SierraUpdatePoller

This package is intended to be used to retrieve Bib/Item IDs that have been updated by polling the Sierra API and then posting those IDs to a Kinesis stream for further processing.

## Installation

Configure environment variables for the following (also check src/main/java/...config/Environment.config file for anything that is not listed here):

```
resourceType = bibs/items (depending on which one has to be processed)

redisHost = <redis_host>

redisPort = <redis_port>

kinesisResourceUpdateStream = <kinesis_stream_for_bib/item_updates>

kinesisResourceRetrievalRequestStream = <kinesis_stream_for_bib/item_retrieval>

sierraApi = <base_api>/bibs or <base_api>/items

accessTokenUri = <base_api>/token

clientId = <clientId_to_access_sierra_api>

clientSecret = <clientSecret_to_access_sierra_api>

grantType = <grant_type_for_sierra>

pollDelay = <how_frequent_should_sierra_calls_be_made>
```

The following amazon credentials need to be provided too. These are not used by EnvironmentConfig.java file, however, using Amazon credentials chaining process it would need this if it doesn't see one set:
```
AWS_ACCESS_KEY_ID = <amazon_key_id>
AWS_SECRET_ACCESS_KEY = <amazon_secret_key>
```

## Requirements (check pom.xml for version information)

* Redis
* Spring Boot
* Apache Camel
* Apache Avro

## Features

* Polls the [Sierra Item API](https://ilsstaff.nypl.org/iii/sierra-api/swagger/index.html#!) at a fixed interval as specified in environment variable for pollDelay
* Posts Bib/Item IDs that have updated to Kinesis Streams specified in the environment using Avro encoding.

## Checkstyle

* Use google checkstyle for code formatting
Before code checkin, please make sure code follows the google checkstyle format


## Usage
On IDE:
  STS - Choose Run As - Run as a spring boot application
  As a jar file - mvn clean package will create jar file and start the app. To just create the jar file - mvn clean package -DskipTests
