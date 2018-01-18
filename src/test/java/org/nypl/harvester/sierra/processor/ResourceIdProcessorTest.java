package org.nypl.harvester.sierra.processor;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.activation.DataHandler;

import org.apache.camel.Attachment;
import org.apache.camel.Exchange;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.Message;
import org.apache.camel.http.common.HttpOperationFailedException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.nypl.harvester.sierra.TestEnvironment;
import org.nypl.harvester.sierra.cache.CacheResource;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.springframework.http.HttpMethod;
import org.springframework.retry.support.RetryTemplate;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ResourceIdProcessorTest extends TestEnvironment {

  @Test
  public void testGetStartTime() throws SierraHarvesterException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    CacheResource cacheResource = new CacheResource();
    cacheResource.setEndTime("lastEndTimeNewStartTime");
    Assert.assertEquals("lastEndTimeNewStartTime",
        spyResourceIdProcessor.getStartTime(Optional.of(cacheResource), "mockResourceType"));

    // Assert.assertTrue(spyResourceIdProcessor.getStartTime(Optional.empty(), "mockResourceType")
    //    .endsWith("T00:00:00Z"));
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
    // Assert.assertTrue(queryParams.get("startTime").toString().endsWith("T00:00:00Z"));
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
  public void testProcessResourcesAndUpdateCacheForGoodResponseFromSierraWhenUpdatesAvailable()
      throws SierraHarvesterException, JsonParseException, JsonMappingException, IOException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> mockQueryParams = new HashMap<>();
    mockQueryParams.put("startTime", "startTime");
    mockQueryParams.put("endTime", "endTime");
    mockQueryParams.put("offset", 0);
    mockQueryParams.put("limit", 0);
    mockQueryParams.put("total", 0);
    doReturn(mockQueryParams).when(spyResourceIdProcessor).getSierraAPIQueryParams(null,
        "mockResourceType");
    Map<String, Object> mockResponseFromSierra = new HashMap<>();
    mockResponseFromSierra.put(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE, 200);
    doReturn(mockResponseFromSierra).when(spyResourceIdProcessor).getResultsFromSierra("startTime",
        "endTime", 0, 0, "mockResourceType");
    doReturn(true).when(spyResourceIdProcessor).processApiResponseWhenUpdatesAreAvailable(
        mockResponseFromSierra, 0, 0, 0, "startTime", "endTime", "mockResourceType");
    Assert.assertTrue(
        spyResourceIdProcessor.processResourcesAndUpdateCache(null, "mockResourceType"));
  }

  @Test
  public void testProcessResourcesAndUpdateCacheForGoodResponseFromSierraWhenUpdatesNotAvailable()
      throws SierraHarvesterException, JsonParseException, JsonMappingException, IOException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> mockQueryParams = new HashMap<>();
    mockQueryParams.put("startTime", "startTime");
    mockQueryParams.put("endTime", "endTime");
    mockQueryParams.put("offset", 0);
    mockQueryParams.put("limit", 0);
    mockQueryParams.put("total", 0);
    doReturn(mockQueryParams).when(spyResourceIdProcessor).getSierraAPIQueryParams(null,
        "mockResourceType");
    Map<String, Object> mockResponseFromSierra = new HashMap<>();
    mockResponseFromSierra.put(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE, 200);
    doReturn(mockResponseFromSierra).when(spyResourceIdProcessor).getResultsFromSierra("startTime",
        "endTime", 0, 0, "mockResourceType");
    doReturn(false).when(spyResourceIdProcessor).processApiResponseWhenUpdatesAreAvailable(
        mockResponseFromSierra, 0, 0, 0, "startTime", "endTime", "mockResourceType");
    doReturn(false).when(spyResourceIdProcessor).postResourcesIfAnyAndUpdateCache(Optional.empty(),
        0, "startTime", "endTime", "mockResourceType");
    Assert.assertFalse(
        spyResourceIdProcessor.processResourcesAndUpdateCache(null, "mockResourceType"));
  }

  @Test
  public void testHasThereBeenAnyUpdatesInSierra() throws SierraHarvesterException {
    Map<String, Object> mockApiResponse = new HashMap<>();
    mockApiResponse.put(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL, 100);
    boolean expectingUpdates = new ResourceIdProcessor(null, null, null, null, null)
        .hasThereBeenAnyUpdatesInSierra(mockApiResponse, "mockResourceType");
    Assert.assertTrue(expectingUpdates);
    mockApiResponse = new HashMap<>();
    boolean expectingNoUpdates = new ResourceIdProcessor(null, null, null, null, null)
        .hasThereBeenAnyUpdatesInSierra(mockApiResponse, "mockResourceType");
    Assert.assertFalse(expectingNoUpdates);
  }

  @Test
  public void testProcessApiResponseForResourceUpdates()
      throws SierraHarvesterException, JsonParseException, JsonMappingException, IOException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, Object> response = new HashMap<>();
    response.put(HarvesterConstants.SIERRA_API_RESPONSE_BODY, "{\"body\" : \"mockResponseBody\"}");
    Assert.assertFalse(spyResourceIdProcessor.processApiResponseWhenUpdatesAreAvailable(response, 0,
        0, 0, null, null, null));
    response.put(HarvesterConstants.SIERRA_API_RESPONSE_BODY, "{\"total\" : 10}");
    Map<String, Object> apiResponseBody = new HashMap<>();
    apiResponseBody.put("total", 10);
    doReturn(true).when(spyResourceIdProcessor).handleSierraUpdates(10, apiResponseBody, response,
        0, 0, null, null, null);
    Assert.assertTrue(spyResourceIdProcessor.processApiResponseWhenUpdatesAreAvailable(response, 10,
        0, 0, null, null, null));
  }

  @Test
  public void testIfAnyMoreResourcesToUpdateFromSierra() throws SierraHarvesterException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    Assert.assertTrue(
        resourceIdProcessor.noMoreResourcesLeftToGetfromSierra(2, 10, "mockResourceType"));
    Assert.assertFalse(
        resourceIdProcessor.noMoreResourcesLeftToGetfromSierra(20, 10, "mockResourceType"));
  }

  @Test
  public void testGetValuesToUpdateCache() {
    Map<String, String> testValuesToUpdateCache =
        new ResourceIdProcessor(null, null, null, null, null).getValuesToUpdateCache(10,
            "mockStartTime", "mockEndTime");
    Assert.assertEquals(Integer.parseInt(
        testValuesToUpdateCache.get(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET)), 10);
    Assert.assertTrue(testValuesToUpdateCache.get(HarvesterConstants.REDIS_KEY_START_TIME_DELTA)
        .equals("mockStartTime"));
    Assert.assertTrue(testValuesToUpdateCache.get(HarvesterConstants.REDIS_KEY_END_TIME_DELTA)
        .equals("mockEndTime"));
    Assert.assertEquals(
        Boolean.parseBoolean(
            testValuesToUpdateCache.get(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE)),
        false);
  }

  @Test(expected = SierraHarvesterException.class)
  public void testPostResourcesIfAnyAndUpdateCache() throws SierraHarvesterException {
    ResourceIdProcessor resourceIdProcessor = new ResourceIdProcessor(null, null, null, null, null);
    ResourceIdProcessor spyResourceIdProcessor = Mockito.spy(resourceIdProcessor);
    Map<String, String> mockCacheVals = new HashMap<>();
    mockCacheVals.put(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE,
        Boolean.toString(false));
    mockCacheVals.put(HarvesterConstants.REDIS_KEY_END_TIME_DELTA, "mockEndTime");
    mockCacheVals.put(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET, Integer.toString(10));
    mockCacheVals.put(HarvesterConstants.REDIS_KEY_START_TIME_DELTA, "mockStartTime");
    doReturn(mockCacheVals).when(spyResourceIdProcessor).getValuesToUpdateCache(10, "mockStartTime",
        "mockEndTime");
    doNothing().when(spyResourceIdProcessor).updateCache("mockresourcetype", mockCacheVals);
    Assert.assertTrue(spyResourceIdProcessor.postResourcesIfAnyAndUpdateCache(Optional.empty(), 10,
        "mockStartTime", "mockEndTime", "mockresourcetype"));
    Resource resource1 = new Resource();
    Resource resource2 = new Resource();
    List<Resource> resources = new ArrayList<>();
    resources.add(resource1);
    resources.add(resource2);
    Optional<List<Resource>> optionalResources = Optional.of(resources);
    Integer[] mockCountResourcesPosted = new Integer[2];
    mockCountResourcesPosted[0] = 100;
    mockCountResourcesPosted[1] = 100;
    doReturn(mockCountResourcesPosted).when(spyResourceIdProcessor).postResourcesToStream(resources,
        "mockresourcetype");
    doReturn(mockCacheVals).when(spyResourceIdProcessor).getValuesToUpdateCache(10, "mockStartTime",
        "mockEndTime");
    doNothing().when(spyResourceIdProcessor).updateCache("mockresourcetype", mockCacheVals);
    Assert.assertTrue(spyResourceIdProcessor.postResourcesIfAnyAndUpdateCache(optionalResources, 10,
        "mockStartTime", "mockEndTime", "mockresourcetype"));
    mockCountResourcesPosted[0] = 100;
    mockCountResourcesPosted[1] = 60;
    doReturn(mockCountResourcesPosted).when(spyResourceIdProcessor).postResourcesToStream(resources,
        "mockresourcetype");
    doReturn(mockCacheVals).when(spyResourceIdProcessor).getValuesToUpdateCache(10, "mockStartTime",
        "mockEndTime");
    doNothing().when(spyResourceIdProcessor).updateCache("mockresourcetype", mockCacheVals);
    spyResourceIdProcessor.postResourcesIfAnyAndUpdateCache(optionalResources, 10, "mockStartTime",
        "mockEndTime", "mockresourcetype");
  }

  @Test
  public void testGetResponseFromExchange() throws Exception {
    setUp();
    HttpOperationFailedException httpOperationFailedException =
        new HttpOperationFailedException("", 200, null, null, null, "mockResponseBody");
    exchange.setException(httpOperationFailedException);
    Map<String, Object> response = new ResourceIdProcessor(null, null, null, null, null)
        .getResponseFromExchange(exchange, "mockResourceType");
    Assert.assertEquals(response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE), 200);
    Assert.assertEquals(response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY),
        "mockResponseBody");
    httpOperationFailedException = null;
    exchange.setException(httpOperationFailedException);
    Message in = exchange.getIn();
    in.setHeader(Exchange.HTTP_RESPONSE_CODE, 400);
    in.setBody("mockBodyResponse");
    exchange.setOut(in);
    response = new ResourceIdProcessor(null, null, null, null, null)
        .getResponseFromExchange(exchange, "mockResourceType");
    Assert.assertEquals(response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE), 400);
    Assert.assertEquals(response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY),
        "mockBodyResponse");
  }

  @Test
  public void testAddResourcesFromAPIResponse() throws SierraHarvesterException {
    Map<String, Object> apiResponse = new HashMap<>();
    Map<String, Object> entry = new HashMap<>();
    entry.put("id", "12345678");
    List<Map<String, Object>> entries = new ArrayList<>();
    entries.add(entry);
    apiResponse.put("entries", entries);
    List<Resource> resources = new ResourceIdProcessor(null, null, null, null, null)
        .addResourcesFromAPIResponse(apiResponse, "mockResourceType");
    Resource resource = resources.get(0);
    Assert.assertTrue(resource.getId().equals("12345678"));
  }

}
