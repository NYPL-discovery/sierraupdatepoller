package org.nypl.harvester.sierra.utils;

public class HarvesterConstants {

  public static final String POLL_DELAY = "30s";
  public static final String REDIS_KEY_LAST_UPDATED_TIME = "ItemUpdatePoller:lastUpdatedTime";
  public static final String SIERRA_API_UPDATED_DATE = "updatedDate";
  public static final String SIERRA_API_OFFSET = "offset";
  public static final String SIERRA_API_LIMIT = "limit";
  public static final String SIERRA_API_FIELDS_PARAMETER = "fields";
  public static final String SIERRA_API_FIELDS_VALUE = "id";
  public static final String SIERRA_API_RESPONSE_ENTRIES = "entries";
  public static final String SIERRA_API_RESPONSE_TOTAL = "total";
  public static final String SIERRA_API_RESPONSE_ID = "id";
  public static final String SIERRA_API_RESPONSE_BODY = "body";
  public static final String SIERRA_API_RESPONSE_HTTP_CODE = "sierraResponseCode";
  public static final String KINESIS_PARTITION_KEY = "CamelAwsKinesisPartitionKey";
  public static final String KINESIS_SEQUENCE_NUMBER = "CamelAwsKinesisSequenceNumber";
  public static final String APP_ITEMS_LIST = "items";
}
