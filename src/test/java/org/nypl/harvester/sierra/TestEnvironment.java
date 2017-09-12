package org.nypl.harvester.sierra;

import org.apache.camel.test.junit4.ExchangeTestSupport;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.nypl.harvester.sierra.config.EnvironmentVariableNames;

public class TestEnvironment extends ExchangeTestSupport {

  @Rule
  public final EnvironmentVariables envVariables = new EnvironmentVariables();

  public TestEnvironment() {
    envVariables.set(EnvironmentVariableNames.RESOURCE_TYPE, "mockResourceType");
    envVariables.set(EnvironmentVariableNames.ACCESS_TOKEN_URI, "mockToeknUri");
    envVariables.set(EnvironmentVariableNames.CLIENT_ID, "mockClientId");
    envVariables.set(EnvironmentVariableNames.CLIENT_SECRET, "mockClientSecret");
    envVariables.set(EnvironmentVariableNames.GRANT_TYPE, "mockGrantType");
    envVariables.set(EnvironmentVariableNames.KINESIS_RESOURCE_RETRIEVAL_STREAM,
        "mockKinesisStream");
    envVariables.set(EnvironmentVariableNames.KINESIS_RESOURCE_UPDATE_STREAM, "mockKinesisStream");
    envVariables.set(EnvironmentVariableNames.POLL_DELAY, "mockPollDelay");
    envVariables.set(EnvironmentVariableNames.REDIS_HOST, "mockRedisHost");
    envVariables.set(EnvironmentVariableNames.REDIS_PORT, "12345");
    envVariables.set(EnvironmentVariableNames.SIERRA_API, "mockSierraApi");
  }
}
