package org.nypl.harvester.sierra.config;

public class EnvironmentConfig {

  public static Boolean isBib = Boolean.parseBoolean(System.getenv("processBibs").toLowerCase());

  public static String redisHost = System.getenv("redisHost");

  public static Integer redisPort = Integer.parseInt(System.getenv("redisPort"));

  public static String kinesisUpdateStream = System.getenv("kinesisResourceUpdateStream");

  public static String kinesisResourceRetrievalRequestStream =
      System.getenv("kinesisResourceRetrievalRequestStream");

  public static String sierraApi = System.getenv("sierraApi");

  public static String accessTokenUri = System.getenv("accessTokenUri");

  public static String clientId = System.getenv("clientId");

  public static String clientSecret = System.getenv("clientSecret");

  public static String grantType = System.getenv("grantType");

  public static String pollDelay = System.getenv("pollDelay");
}
