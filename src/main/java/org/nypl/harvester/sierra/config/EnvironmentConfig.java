package org.nypl.harvester.sierra.config;

public final class EnvironmentConfig {

  public static final Integer redisPort =
      Integer.parseInt(System.getenv(EnvironmentVariableNames.REDIS_PORT));

  public static final String resourceType =
      System.getenv(EnvironmentVariableNames.RESOURCE_TYPE).trim().toLowerCase();

  public static final String redisHost = System.getenv(EnvironmentVariableNames.REDIS_HOST);

  public static final String kinesisUpdateStream =
      System.getenv(EnvironmentVariableNames.KINESIS_RESOURCE_UPDATE_STREAM);

  public static final String kinesisResourceRetrievalRequestStream =
      System.getenv(EnvironmentVariableNames.KINESIS_RESOURCE_RETRIEVAL_STREAM);

  public static final String sierraApi = System.getenv(EnvironmentVariableNames.SIERRA_API);

  public static final String accessTokenUri =
      System.getenv(EnvironmentVariableNames.ACCESS_TOKEN_URI);

  public static final String clientId = System.getenv(EnvironmentVariableNames.CLIENT_ID);

  public static final String clientSecret = System.getenv(EnvironmentVariableNames.CLIENT_SECRET);

  public static final String grantType = System.getenv(EnvironmentVariableNames.GRANT_TYPE);

  public static final String pollDelay = System.getenv(EnvironmentVariableNames.POLL_DELAY);


}
