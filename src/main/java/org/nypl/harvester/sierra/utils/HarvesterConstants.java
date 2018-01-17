package org.nypl.harvester.sierra.utils;

import org.nypl.harvester.sierra.config.EnvironmentConfig;

public final class HarvesterConstants {

  public static final String REDIS_KEY_START_TIME_DELTA =
      EnvironmentConfig.resourceType + "DeletionsPoller:beginTimeDelta";
  public static final String REDIS_KEY_END_TIME_DELTA =
      EnvironmentConfig.resourceType + "DeletionsPoller:endTimeDelta";
  public static final String REDIS_KEY_LAST_UPDATED_OFFSET =
      EnvironmentConfig.resourceType + "DeletionsPoller:lastUpdatedOffset";
  public static final String REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE =
      EnvironmentConfig.resourceType + "DeletionsPoller:isComplete";
  public static final String SIERRA_API_UPDATED_DATE = "deletedDate";
  public static final String SIERRA_API_OFFSET = "offset";
  public static final String SIERRA_API_LIMIT = "limit";
  public static final String SIERRA_API_FIELDS_PARAMETER = "fields";
  public static final String SIERRA_API_FIELDS_VALUE = "id";
  public static final String SIERRA_API_RESPONSE_ENTRIES = "entries";
  public static final String SIERRA_API_RESPONSE_TOTAL = "total";
  public static final String SIERRA_API_RESPONSE_ID = "id";
  public static final String SIERRA_API_RESPONSE_BODY = "body";
  public static final String SIERRA_API_FIELD_TIME_FORMAT = "yyyy-MM-dd";
  public static final String SIERRA_API_RESPONSE_HTTP_CODE = "sierraResponseCode";
  public static final String SIERRA_API_HEADER_KEY_AUTHORIZATION = "Authorization";
  public static final String SIERRA_API_HEADER_AUTHORIZATION_VAL_BEARER = "bearer";
  public static final String KINESIS_PARTITION_KEY = "CamelAwsKinesisPartitionKey";
  public static final String KINESIS_SEQUENCE_NUMBER = "CamelAwsKinesisSequenceNumber";
  public static final String BIBS = "bibs";
  public static final String ITEMS = "items";
  public static final String APP_OPTIONAL_CACHE_RESOURCE = "optionalCacheResource";
  public static final String APP_RESOURCE_TYPE = "resourceType";
  public static final String IS_PROCESSED = "isProcessed";
  public static final int KINESIS_PUT_RECORDS_MAX_SIZE = 499;

}
