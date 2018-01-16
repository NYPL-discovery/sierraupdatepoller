# Troubleshooting on Local instance

## Prerequisites
The assumption is troubleshooting a **local** instance for initial code testing.  This means we are assuming 
- there is a local IDE 
- there is a localhost version of Redis Server 
- a sandbox version of Kinesis is being used
- etc.

## Not connected to Kinesis
The error message would be similar to the following
```
com.amazonaws.SdkClientException: Unable to load AWS credentials from any provider in the chain
```
The solution is to provide values for environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` within the IDE. This is not specific to any AWS user, but the keys has to match the specified AWS profile in order to have a valid log in.

## Changing Timestamps Manually
The purpose of changing the timestamp manually is to test whether the SierraPollerUpdater is working properly.

**By default**, the most current date and time are recorded in the hashset stored within Redis.

We log into redis and look for the keys identified by the environment variable `resourceType`. Here we are demonstrating how to manipulate updating and retrieving Bibs.

### Search for Key on Redis
```
$ redis-cli

127.0.0.1:6379> KEYS *
...
16) "bibs"
...
```
### Viewing contents of Hash Set
```
127.0.0.1:6379> HGETALL bibs
1) "bibsUpdatePoller:isComplete"
2) "true"
3) "bibsUpdatePoller:lastUpdatedOffset"
4) "0"
5) "bibsUpdatePoller:beginTimeDelta"
6) "2018-01-12T20:34:16Z"
7) "bibsUpdatePoller:endTimeDelta"
8) "2018-01-12T20:44:10Z"
```
### Setting a value for a hash key
```
127.0.0.1:6379> HSET bibs bibsUpdatePoller:beginTimeDelta 2017-07-01T00:00:00  
```
Confirm that the hash key is modified
```
127.0.0.1:6379> HGETALL bibs
...
5) "bibsUpdatePoller:beginTimeDelta"
6) "2017-07-01T00:00:00"
...
```
### Run on local IDE
After changing `beginTimeDelta` and `endTimeDelta`, re-run SierraItemUpdatePoller to confirm results.