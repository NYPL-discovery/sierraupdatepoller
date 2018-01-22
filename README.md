# SierraUpdatePoller

[![Coverage Status](https://coveralls.io/repos/github/NYPL-discovery/sierraupdatepoller/badge.svg?branch=master)](https://coveralls.io/github/NYPL-discovery/sierraupdatepoller?branch=master)

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

## Description

On the redis cache the values will be stored as follows:

```
127.0.0.1:6379> HGETALL bibs
1) "bibsUpdatePoller:beginTimeDelta"
2) "2017-04-14T15:33:30Z"
3) "bibsUpdatePoller:lastUpdatedOffset"
4) "41"
5) "bibsUpdatePoller:endTimeDelta"
6) "2017-04-14T15:34:22Z"
7) "isComplete"
8) "true"
```

* When the app starts for the first time

1) There is nothing the redis cache now. The app will query the sierra api with startTime - "YYYY-MM-DDT00:00:00" (year, month, date fields based on your current system date), the endTime - current system date time in the format "YYYY-MM-DDTHH:MM:SSZ", limit (based on your env variable, max allowed is 2000) of bib/item ids to be fetched from sierra api, starting offset - 0. 

2) Get the results and post to kinesis then update the redis cache as you see above, except isComplete will be set to false. Then iterates until all the bib/item ids updated within the time period are taken and posted to kinesis

3) Once there is nothing left to fetch and post within the time period, isComplete will be set to true

* When the app runs after the idle time

1) It will check redis cache and see if isComplete is true (to ensure previous iteration worked and everything was fetched and sent to kinesis)

2) If isComplete is false, it will use the same startTime and endTime and start with the offset in the redis cache. If isComplete is true, the startTime will be the endTime of redis cache and the new endTime will be your system's current date time and the iteration happens (fetch - post to kinesis - change is complete to true)

The advantage of this is that if the app fails in between, when it starts back again, it knows where to start from looking at the isCompleted flag.

## Usage

On IDE:
   * STS - Choose Run As - Run as a spring boot application
   * As a jar file:

    * `mvn clean package` will create jar file and start the app.

    * `mvn clean package -DskipTests` to just create the jar file

# Deployment

### Git Strategy

`master` is stable but bleeding edge. Cut feature branches off of `master`.
Send PRs to be merged into `master`.

-`master` ==branches out to==> `feature-branch` ==gets merged into==> `master`.

### AWS Deployment Strategy
For each of `bib` and `item` resourceType, we deploy a separate instance of Elastic Beanstalk. Subsequent deployments can be differentiated by specifying the environment name during `eb deploy`.

There is a separate branch for the delete pollers from the update pollers, so in total, we need to deploy 4 separate Elastic Beanstalk instance:
1. Bib Updates
2. Bib Deletes
3. Item Updates
4. Item Deletes

### AWS Elastic Beanstalk
1. `.ebextensions` directory needed at application's root directory
2. `.ebextensions/environmentvariables.config` to store environment variables. For environment variables that needs to be hidden,
3. `Procfile` to start Spring Boot app after deployment.
4. `eb init -i --profile <<your AWS profile>>` specify application, region, and AWS account of deployment
5. Initial creation of instance on Beanstalk:

Please use the instance profile of _cloudwatchable-beanstalk_.
Which has all the permissions needed for a traditional or Docker-flavored Beanstalk
machine that wants to log to CloudWatch.

```bash
eb create <<environment name>> --instance_type <<size of instance>> \
    --instance_profile cloudwatchable-beanstalk \
    --envvars FOO="bar",MYVAR="myval" \
    --cname <<cname prefix (XXX.us-east-1.elasticbeanstalk.com)>> \
    --vpc.id <<ask for custom vpc_id>> \
    --vpc.ec2subnets <<privateSubnetId1,privateSubnetId2>> \
    --vpc.elbsubnets <<publicSubnetId1,publicSubnetId2>> \
    --vpc.elbpublic \
    --profile <<your AWS profile>>
```

6. Subsequent deployment
`eb deploy <<environment name>> --profile <<your AWS profile>>`