package org.nypl.harvester.sierra.processor;

import static org.mockito.Mockito.doReturn;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nypl.harvester.sierra.cache.CacheResource;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;

public class ResourceIdProcessorTest {

  @Test
  public void testGetStartTime() throws SierraHarvesterException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    CacheResource cacheResource = new CacheResource();
    cacheResource.setEndTime("lastEndTimeNewStartTime");
    Assert.assertEquals("lastEndTimeNewStartTime",
        spyResourceIdProcessor.getStartTime(Optional.of(cacheResource), "mockResourceType"));

    Assert.assertTrue(spyResourceIdProcessor.getStartTime(Optional.empty(), "mockResourceType")
        .endsWith("T00:00:00Z"));
  }

  @Test
  public void testGetSierraAPIQueryParamsForEmptyCacheResource() throws SierraHarvesterException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> queryParams =
        spyResourceIdProcessor.getSierraAPIQueryParams(Optional.empty(), "mockResourceType");
    Assert.assertEquals(0, queryParams.get("offset"));
    Assert.assertEquals(500, queryParams.get("limit"));
    Assert.assertEquals(0, queryParams.get("total"));
    Assert.assertTrue(queryParams.get("startTime").toString().endsWith("T00:00:00Z"));
  }

  @Test
  public void testGetSierraAPIQueryParamsForCompletedCacheResource()
      throws SierraHarvesterException {
    String mockEndTime = "mockEndTime";
    CacheResource cacheResource = new CacheResource();
    cacheResource.setIsDone(true);
    cacheResource.setEndTime(mockEndTime);
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> queryParams = spyResourceIdProcessor
        .getSierraAPIQueryParams(Optional.of(cacheResource), "mockResourceType");
    Assert.assertEquals(0, queryParams.get("offset"));
    Assert.assertEquals(500, queryParams.get("limit"));
    Assert.assertEquals(0, queryParams.get("total"));
    Assert.assertTrue(queryParams.get("startTime").toString().endsWith(mockEndTime)); // old end
                                                                                      // time will
                                                                                      // be new
                                                                                      // start time
  }

  @Test
  public void testGetSierraAPIQueryParamsForInCompletedCacheResource()
      throws SierraHarvesterException {
    String mockEndTime = "mockEndTime";
    String mockStartTime = "mockStartTime";
    int mockOffset = 1098765;
    CacheResource cacheResource = new CacheResource();
    cacheResource.setIsDone(false);
    cacheResource.setEndTime(mockEndTime);
    cacheResource.setStartTime(mockStartTime);
    cacheResource.setOffset(mockOffset);
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> queryParams = spyResourceIdProcessor
        .getSierraAPIQueryParams(Optional.of(cacheResource), "mockResourceType");
    Assert.assertEquals(mockOffset, queryParams.get("offset"));
    Assert.assertEquals(500, queryParams.get("limit"));
    Assert.assertEquals(0, queryParams.get("total"));
    Assert.assertEquals(mockStartTime, queryParams.get("startTime"));
    Assert.assertEquals(mockEndTime, queryParams.get("endTime"));
  }

  @Test
  public void testProcessResourcesAndUpdateCacheForBadResponseFromSierra()
      throws SierraHarvesterException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> mockQueryParams = new HashMap<>();
    mockQueryParams.put("startTime", "startTime");
    mockQueryParams.put("endTime", "endTime");
    mockQueryParams.put("offset", 0);
    mockQueryParams.put("limit", 0);
    mockQueryParams.put("total", 0);
    doReturn(mockQueryParams).when(spyResourceIdProcessor).getSierraAPIQueryParams(Optional.empty(),
        "mockResourceType");
    Map<String, Object> mockResponseFromSierra = new HashMap<>();
    mockResponseFromSierra.put(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE, 400);
    doReturn(mockResponseFromSierra).when(spyResourceIdProcessor).getResultsFromSierra("startTime",
        "endTime", 0, 0, "mockResourceType");
    Assert.assertFalse(spyResourceIdProcessor.processResourcesAndUpdateCache(Optional.empty(),
        "mockResourceType"));
  }

  @Test
  public void testProcessResourcesAndUpdateCacheForGoodResponseFromSierra()
      throws SierraHarvesterException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> mockQueryParams = new HashMap<>();
    mockQueryParams.put("startTime", "startTime");
    mockQueryParams.put("endTime", "endTime");
    mockQueryParams.put("offset", 0);
    mockQueryParams.put("limit", 0);
    mockQueryParams.put("total", 0);
    doReturn(mockQueryParams).when(spyResourceIdProcessor).getSierraAPIQueryParams(Optional.empty(),
        "mockResourceType");
    Map<String, Object> mockResponseFromSierra = new HashMap<>();
    mockResponseFromSierra.put(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE, 200);
    mockResponseFromSierra.put(HarvesterConstants.SIERRA_API_RESPONSE_BODY,
        "{\"body\" :  { \"mockApiResponseBody\" : \"mockApiResponseBody\"}}");
    doReturn(mockResponseFromSierra).when(spyResourceIdProcessor).getResultsFromSierra("startTime",
        "endTime", 0, 0, "mockResourceType");
    doReturn(true).when(spyResourceIdProcessor).processResourcesAndUpdateCache(Optional.empty(),
        "mockResourceType");
    Assert.assertTrue(spyResourceIdProcessor.processResourcesAndUpdateCache(Optional.empty(),
        "mockResourceType"));
  }

  @Test
  public void testHasThereBeenAnyUpdatesInSierra() throws SierraHarvesterException {
    Map<String, Object> mockApiResponse = new HashMap<>();
    mockApiResponse.put(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL, 100);
    boolean isThereAnyUpdate = new ResourceIdProcessor(null, null, null, null, null)
        .hasThereBeenAnyUpdatesInSierra(mockApiResponse, "mockResourceType");
    Assert.assertTrue(isThereAnyUpdate);
  }

}
