package org.nypl.harvester.sierra.processor;

import static org.mockito.Mockito.doReturn;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.mockito.Mockito;
import org.nypl.harvester.sierra.TestEnvironment;
import org.nypl.harvester.sierra.cache.CacheResource;
import org.nypl.harvester.sierra.config.EnvironmentVariableNames;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;

public class CacheResourceMonitorTest extends TestEnvironment {

  @Test
  public void testEmptyCacheResource() throws SierraHarvesterException {
    CacheResourceMonitor cacheResourceMonitor = new CacheResourceMonitor(null, "something");
    CacheResourceMonitor spyCacheResourceMonitor = Mockito.spy(cacheResourceMonitor);
    doReturn(Optional.ofNullable(null)).when(spyCacheResourceMonitor).getPropertiesFromProcessor();

    Assert.assertEquals(Optional.empty(), spyCacheResourceMonitor.getCacheResource());

    doReturn(Optional.ofNullable(new HashMap<>())).when(spyCacheResourceMonitor)
        .getPropertiesFromProcessor();
    Assert.assertEquals(Optional.empty(), spyCacheResourceMonitor.getCacheResource());
  }


  @Test
  public void testPartialCacheResource() throws SierraHarvesterException {
    CacheResourceMonitor cacheResourceMonitor = new CacheResourceMonitor(null, "something");
    CacheResourceMonitor spyCacheResourceMonitor = Mockito.spy(cacheResourceMonitor);
    Map<String, String> hashValsForCacheResource = new HashMap<>();
    hashValsForCacheResource.put(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET, "12345");
    hashValsForCacheResource.put(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE,
        "mockCompletionStatus");

    doReturn(Optional.of(hashValsForCacheResource)).when(spyCacheResourceMonitor)
        .getPropertiesFromProcessor();

    Assert.assertEquals(Optional.empty(), spyCacheResourceMonitor.getCacheResource());
  }

  @Test
  public void testCompleteCacheResource() throws SierraHarvesterException {
    String mockOffset = "12345";
    String mockCompletionStatus = "true";
    String mockEndTime = "mockEndTime";
    String mockStartTime = "mockStartTime";

    CacheResourceMonitor cacheResourceMonitor = new CacheResourceMonitor(null, "something");
    CacheResourceMonitor spyCacheResourceMonitor = Mockito.spy(cacheResourceMonitor);

    Map<String, String> hashValsForCacheResource = new HashMap<>();
    hashValsForCacheResource.put(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET, mockOffset);
    hashValsForCacheResource.put(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE,
        mockCompletionStatus);
    hashValsForCacheResource.put(HarvesterConstants.REDIS_KEY_END_TIME_DELTA, mockEndTime);
    hashValsForCacheResource.put(HarvesterConstants.REDIS_KEY_START_TIME_DELTA, mockStartTime);

    doReturn(Optional.of(hashValsForCacheResource)).when(spyCacheResourceMonitor)
        .getPropertiesFromProcessor();
    CacheResource actualCacheResource = spyCacheResourceMonitor.getCacheResource().get();
    Assert.assertEquals(CacheResource.class, actualCacheResource.getClass());

    Assert.assertTrue(mockOffset.equalsIgnoreCase(actualCacheResource.getOffset().toString()));
    Assert.assertEquals(Boolean.parseBoolean(mockCompletionStatus),
        actualCacheResource.getIsDone());
    Assert.assertEquals(mockEndTime, actualCacheResource.getEndTime());
    Assert.assertEquals(mockStartTime, actualCacheResource.getStartTime());
  }

}
