package org.nypl.harvester.sierra.utils;

import org.nypl.harvester.sierra.config.EnvironmentConfig;

public class HarvesterConstants {

  public static final String REDIS_KEY_LAST_UPDATED_TIME =
      getResource() + "UpdatePoller:lastUpdatedTime";
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
  public static final String SIERRA_API_HEADER_KEY_AUTHORIZATION = "Authorization";
  public static final String SIERRA_API_HEADER_AUTHORIZATION_VAL_BEARER = "bearer";
  public static final String KINESIS_PARTITION_KEY = "CamelAwsKinesisPartitionKey";
  public static final String KINESIS_SEQUENCE_NUMBER = "CamelAwsKinesisSequenceNumber";
  public static final String APP_RESOURCES_LIST = getResource();
  public static final String BIBS = "bibs";
  public static final String ITEMS = "items";

  public static String getResource() {
    if (EnvironmentConfig.isBib)
      return BIBS;
    else
      return ITEMS;
  }


}
