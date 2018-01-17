package org.nypl.harvester.sierra.processor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.http.common.HttpOperationFailedException;
import org.nypl.harvester.sierra.cache.CacheProcessor;
import org.nypl.harvester.sierra.cache.CacheResource;
import org.nypl.harvester.sierra.config.BaseConfig;
import org.nypl.harvester.sierra.config.EnvironmentConfig;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.nypl.harvester.sierra.resourceid.poster.ResourcePoster;
import org.nypl.harvester.sierra.resourceid.poster.StreamPoster;
import org.nypl.harvester.sierra.routebuilder.RouteBuilderIdPoller;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

public class ResourceIdProcessor implements Processor {

  private static Logger logger = LoggerFactory.getLogger(ResourceIdProcessor.class);

  private ProducerTemplate template;

  private String token;

  private RetryTemplate retryTemplate;

  private Map<String, StreamDataModel> streamNameAndDataModel;

  private BaseConfig baseConfig;

  private static final String START_TIME = "startTime";
  private static final String END_TIME = "endTime";
  private static final String OFFSET = "offset";
  private static final String LIMIT = "limit";
  private static final String TOTAL = "total";

  public ResourceIdProcessor(String token, ProducerTemplate producerTemplate,
      RetryTemplate retryTemplate, Map<String, StreamDataModel> streamNameAndDataModel,
      BaseConfig baseConfig) {
    this.token = token;
    this.template = producerTemplate;
    this.retryTemplate = retryTemplate;
    this.streamNameAndDataModel = streamNameAndDataModel;
    this.baseConfig = baseConfig;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    String resourceType = null;
    try {
      Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);
      resourceType = (String) exchangeContents.get(HarvesterConstants.APP_RESOURCE_TYPE);
      Optional<CacheResource> cacheResource = (Optional<CacheResource>) exchangeContents
          .get(HarvesterConstants.APP_OPTIONAL_CACHE_RESOURCE);

      Boolean status = processResourcesAndUpdateCache(cacheResource, resourceType);

      Map<String, Object> resourceTypeAndProcessedStatus = new HashMap<>();
      resourceTypeAndProcessedStatus.put(HarvesterConstants.APP_RESOURCE_TYPE, resourceType);
      resourceTypeAndProcessedStatus.put(HarvesterConstants.IS_PROCESSED, status);

      exchange.getIn().setBody(resourceTypeAndProcessedStatus);
    } catch (NullPointerException npe) {
      logger.error("Hit null pointer exception while getting resource ids that got updated - ",
          npe);

      throw new SierraHarvesterException("Null pointer exception occurred - " + npe.getMessage(),
          resourceType);
    }
  }

  public boolean processResourcesAndUpdateCache(Optional<CacheResource> optionalCacheResource,
      String resourceType) throws SierraHarvesterException {
    try {
      Map<String, Object> sierraApiQueryParams =
          getSierraAPIQueryParams(optionalCacheResource, resourceType);

      String startTime = (String) sierraApiQueryParams.get(START_TIME);
      String endTime = (String) sierraApiQueryParams.get(END_TIME);
      int offset = (int) sierraApiQueryParams.get(OFFSET);
      int limit = (int) sierraApiQueryParams.get(LIMIT);
      int total = (int) sierraApiQueryParams.get(TOTAL);

      Map<String, Object> response =
          getResultsFromSierra(startTime, endTime, offset, limit, resourceType);

      Integer responseCode =
          (Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE);

      if (responseCode == 200) {
        if (processApiResponseWhenUpdatesAreAvailable(response, total, limit, offset, startTime,
            endTime, resourceType))
          return true;
        else { // when there are no updates total will not be there, but we want to update start
               // and end time of cache
          return postResourcesIfAnyAndUpdateCache(Optional.empty(), 0, startTime, endTime,
              resourceType);
        }
      } else if (responseCode >= 400) {
        logger.error("API_ERROR: Hit error with response code- " + responseCode);
      }
      return false;
    } catch (JsonParseException jsonParseException) {
      logger.error(resourceType
          + " : Hit a json parse exception while parsing json response from resources " + "api - ",
          jsonParseException);

      throw new SierraHarvesterException("JsonParseException while parsing resources "
          + "response - " + jsonParseException.getMessage(), resourceType);
    } catch (JsonMappingException jsonMappingException) {
      logger.error("Hit a json mapping exception for api response ", jsonMappingException);

      throw new SierraHarvesterException(
          "JsonMappingException while for api response " + "- " + jsonMappingException.getMessage(),
          resourceType);
    } catch (IOException ioe) {
      logger.error(resourceType + " : Hit an IOException - ", ioe);

      throw new SierraHarvesterException(
          "IOException while for api response " + "- " + ioe.getMessage(), resourceType);
    }
  }

  public Map<String, Object> getSierraAPIQueryParams(Optional<CacheResource> optionalCacheResource,
      String resourceType) throws SierraHarvesterException {
    try {
      String startTime = getStartTime(optionalCacheResource, resourceType);

      int offset = 0;
      int limit = 500;
      int total = 0;

      String endTime = getCurrentTimeInZuluTimeFormat();

      if (optionalCacheResource.isPresent() && !optionalCacheResource.get().getIsDone()) {
        offset = optionalCacheResource.get().getOffset();
        startTime = optionalCacheResource.get().getStartTime();
        endTime = optionalCacheResource.get().getEndTime();
      }

      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put(START_TIME, startTime);
      queryParams.put(END_TIME, endTime);
      queryParams.put(TOTAL, total);
      queryParams.put(LIMIT, limit);
      queryParams.put(OFFSET, offset);
      return queryParams;
    } catch (Exception e) {
      logger.error("Error - ", e);
      throw new SierraHarvesterException(
          "Unable to get query params for sierra api - " + e.getMessage(), resourceType);
    }
  }

  public String getStartTime(Optional<CacheResource> optionalCacheResource, String resourceType)
      throws SierraHarvesterException {
    try {
      if (optionalCacheResource.isPresent()) {
        CacheResource cacheResource = optionalCacheResource.get();
        return cacheResource.getEndTime();
      } else {
        Date currentDate = new Date();

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));

        logger.info("Last updated time: " + dateFormat.format(currentDate)); // .concat("T00:00:00Z"));

        return dateFormat.format(currentDate); // .concat("T00:00:00Z");
      }
    } catch (Exception e) {
      logger.error("Error - ", e);
      throw new SierraHarvesterException(
          "Unable to get start time from cache resource - " + e.getMessage(), resourceType);
    }
  }

  public boolean processApiResponseWhenUpdatesAreAvailable(Map<String, Object> response, int total,
      int limit, int offset, String startTime, String endTime, String resourceType)
      throws SierraHarvesterException, JsonParseException, JsonMappingException, IOException {
    try {
      Map<String, Object> apiResponseBody = new ObjectMapper()
          .readValue((String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY), Map.class);

      if (hasThereBeenAnyUpdatesInSierra(apiResponseBody, resourceType)) {
        return handleSierraUpdates(total, apiResponseBody, response, limit, offset, startTime,
            endTime, resourceType);
      } else {
        return false;
      }
    } catch (Exception e) {
      logger.error("Error - ", e);
      throw new SierraHarvesterException(
          "Error occurred while processing api response - " + e.getMessage(), resourceType);
    }
  }

  public boolean hasThereBeenAnyUpdatesInSierra(Map<String, Object> apiResponse,
      String resourceType) throws SierraHarvesterException {
    try {
      Optional<Integer> optionalTotal = Optional
          .ofNullable((Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL));
      if (optionalTotal.isPresent()) {
        return true;
      } else
        return false;
    } catch (Exception e) {
      logger.error("Error - ", e);
      throw new SierraHarvesterException(
          "Error occurred while checking api response to see if there has been any updates - "
              + e.getMessage(),
          resourceType);
    }
  }

  public boolean handleSierraUpdates(int total, Map<String, Object> apiResponseBody,
      Map<String, Object> response, int limit, int offset, String startTime, String endTime,
      String resourceType) throws SierraHarvesterException {
    try {
      total = (Integer) apiResponseBody.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);
      List<Resource> resources = addResourcesFromAPIResponse(apiResponseBody, resourceType);

      if (noMoreResourcesLeftToGetfromSierra(total, limit, resourceType)) {
        offset = total;
        return postResourcesIfAnyAndUpdateCache(Optional.of(resources), offset, startTime, endTime,
            resourceType);
      } else {
        postResourcesIfAnyAndUpdateCache(Optional.of(resources), offset, startTime, endTime,
            resourceType);
        getAllResourcesForTimeRange(response, total, limit, offset, startTime, endTime,
            resourceType);
        return true;
      }
    } catch (Exception e) {
      logger.error("Error - ", e);
      throw new SierraHarvesterException(
          "Unable to process sierra updates that have been received - " + e.getMessage(),
          resourceType);
    }
  }

  public boolean noMoreResourcesLeftToGetfromSierra(int total, int limit, String resourceType)
      throws SierraHarvesterException {
    try {
      if (total < limit)
        return true;
      else
        return false;
    } catch (Exception e) {
      logger.error("Error - ", e);
      throw new SierraHarvesterException(
          "Error while validating sierra response for more resources - " + e.getMessage(),
          resourceType);
    }
  }


  private void getAllResourcesForTimeRange(Map<String, Object> response, int total, int limit,
      int offset, String startTime, String endTime, String resourceType)
      throws SierraHarvesterException {
    try {
      while (total == limit) {
        offset += limit;

        response = getResultsFromSierra(startTime, endTime, offset, limit, resourceType);

        Integer responseCode =
            (Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE);

        if (responseCode == 200) {
          List<Resource> resources = new ArrayList<>();
          Map<String, Object> apiResponse = new ObjectMapper().readValue(
              (String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY), Map.class);

          total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);

          resources = addResourcesFromAPIResponse(apiResponse, resourceType);

          postResourcesIfAnyAndUpdateCache(Optional.of(resources), offset, startTime, endTime,
              resourceType);
        } else if (responseCode >= 400) {
          logger.error("API_ERROR: Hit error with response code- " + responseCode);
        }
      }
    } catch (JsonParseException jsonParseException) {
      logger.error(resourceType
          + " : Hit a json parse exception while parsing json response from resources " + "api - ",
          jsonParseException);

      throw new SierraHarvesterException("JsonParseException while parsing resources "
          + "response - " + jsonParseException.getMessage(), resourceType);
    } catch (JsonMappingException jsonMappingException) {
      logger.error(resourceType + " : Hit a json mapping exception for resources api response ",
          jsonMappingException);

      throw new SierraHarvesterException("JsonMappingException for resources api response " + "- "
          + jsonMappingException.getMessage(), resourceType);
    } catch (IOException ioe) {
      logger.error(resourceType + " : Hit an IOException - ", ioe);

      throw new SierraHarvesterException(
          "IOException for resources api response " + "- " + ioe.getMessage(), resourceType);
    }
  }

  public List<Resource> addResourcesFromAPIResponse(Map<String, Object> apiResponse,
      String resourceType) throws SierraHarvesterException {
    try {
      List<Resource> resources = new ArrayList<>();
      List<Map<String, Object>> entries = (List<Map<String, Object>>) apiResponse
          .get(HarvesterConstants.SIERRA_API_RESPONSE_ENTRIES);

      for (Map<String, Object> entry : entries) {
        Resource resource = new Resource();
        resource.setId((String) entry.get(HarvesterConstants.SIERRA_API_RESPONSE_ID));
        resources.add(resource);
      }

      return resources;
    } catch (Exception e) {
      logger.error("Error occurred while processing resources from sierra api response - ", e);
      throw new SierraHarvesterException(
          "Error while processing sierra resources from sierra response - " + e.getMessage(),
          resourceType);
    }
  }

  public Map<String, Object> getResultsFromSierra(String startDdate, String endDate, int offset,
      int limit, String resourceType) throws SierraHarvesterException {
    try {
      Exchange apiResponse =
          getExchangeWithAPIResponse(startDdate, endDate, offset, limit, resourceType);

      Map<String, Object> response = getResponseFromExchange(apiResponse, resourceType);

      if ((Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE) == 401) {
        token = new RouteBuilderIdPoller().generateNewTokenProperties().getTokenValue();

        logger.info("Token expired. Got a new token - " + token);

        apiResponse = getExchangeWithAPIResponse(startDdate, endDate, offset, limit, resourceType);

        response = getResponseFromExchange(apiResponse, resourceType);
      }

      return response;
    } catch (Exception e) {
      logger.error("Error occurred while getting results from sierra - ", e);
      throw new SierraHarvesterException(
          "Error while getting results from sierra - " + e.getMessage(), resourceType);
    }

  }

  public Exchange getExchangeWithAPIResponse(String startDdate, String endDate, int offset,
      int limit, String resourceType) throws SierraHarvesterException {
    try {
      String apiToCall =
          EnvironmentConfig.sierraApi + "?" + HarvesterConstants.SIERRA_API_UPDATED_DATE + "=["
              + startDdate + "," + endDate + "]&" + HarvesterConstants.SIERRA_API_OFFSET + "="
              + offset + "&" + HarvesterConstants.SIERRA_API_LIMIT + "=" + limit + "&"
              + HarvesterConstants.SIERRA_API_FIELDS_PARAMETER + "="
              + HarvesterConstants.SIERRA_API_FIELDS_VALUE;

      logger.info("Calling api - " + apiToCall);

      Exchange templateResultExchange =
          retryTemplate.execute(new RetryCallback<Exchange, SierraHarvesterException>() {

            @Override
            public Exchange doWithRetry(RetryContext context) throws SierraHarvesterException {
              try {
                return template.request(apiToCall, new Processor() {
                  @Override
                  public void process(Exchange httpHeaderExchange) throws Exception {
                    httpHeaderExchange.getIn().setHeader(Exchange.HTTP_METHOD, HttpMethod.GET);
                    httpHeaderExchange.getIn().setHeader(
                        HarvesterConstants.SIERRA_API_HEADER_KEY_AUTHORIZATION,
                        HarvesterConstants.SIERRA_API_HEADER_AUTHORIZATION_VAL_BEARER + " "
                            + token);
                  }
                });
              } catch (Exception e) {
                logger.error("API_ERROR: Error occurred while calling sierra api - ", e);
                throw new SierraHarvesterException(
                    "Error occurred while calling sierra api - " + e.getMessage(), resourceType);
              }
            }

          });

      return templateResultExchange;
    } catch (Exception e) {
      logger.error("Error occurred while calling sierra api - ", e);
      throw new SierraHarvesterException("Error while calling sierra api - " + e.getMessage(),
          resourceType);
    }

  }

  public Map<String, Object> getResponseFromExchange(Exchange exchange, String resourceType)
      throws SierraHarvesterException {
    try {
      Message out = exchange.getOut();

      HttpOperationFailedException httpOperationFailedException =
          exchange.getException(HttpOperationFailedException.class);

      Integer responseCode = null;
      String apiResponse = null;

      if (httpOperationFailedException != null) {
        responseCode = httpOperationFailedException.getStatusCode();
        apiResponse = httpOperationFailedException.getResponseBody();
      } else {
        responseCode = out.getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        apiResponse = out.getBody(String.class);
      }

      logger.info("Sierra api response code - " + responseCode);
      logger.info("Sierra api response body - " + apiResponse);

      Map<String, Object> response = new HashMap<>();

      response.put(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE, responseCode);
      response.put(HarvesterConstants.SIERRA_API_RESPONSE_BODY, apiResponse);

      return response;
    } catch (Exception e) {
      logger.error("Error occurred while processing camel exchange to get sierra response - ", e);
      throw new SierraHarvesterException(
          "Error while trying to get sierra response - " + e.getMessage(), resourceType);
    }
  }

  public boolean postResourcesIfAnyAndUpdateCache(Optional<List<Resource>> optionalResources,
      int offset, String startTimeDelta, String endTimeDelta, String resourceType)
      throws SierraHarvesterException {
    if (optionalResources.isPresent()) {
      List<Resource> resources = optionalResources.get();
      Set<Integer> resourcesPosted = new HashSet<>();
      Integer[] countOfResourcesPosted = postResourcesToStream(resources, resourceType);
      for (Integer count : countOfResourcesPosted)
        resourcesPosted.add(count);
      if (resourcesPosted.size() > 1)
        throw new SierraHarvesterException("Count of resources sent to streams are not same",
            resourceType);
    }
    Map<String, String> valuesForCacheUpdate =
        getValuesToUpdateCache(offset, startTimeDelta, endTimeDelta);
    updateCache(resourceType, valuesForCacheUpdate);
    return true;
  }

  public Integer[] postResourcesToStream(List<Resource> resources, String resourceType)
      throws SierraHarvesterException {
    Integer[] countOfResourcesPostedInEachStream = new Integer[streamNameAndDataModel.size()];
    int count = 0;
    for (Entry<String, StreamDataModel> entry : streamNameAndDataModel.entrySet()) {
      ResourcePoster poster = new StreamPoster(entry.getKey(), entry.getValue(), baseConfig);
      countOfResourcesPostedInEachStream[count] =
          poster.postResources(template, resources, resourceType);
      count += 1;
    }
    return countOfResourcesPostedInEachStream;
  }

  public Map<String, String> getValuesToUpdateCache(int offset, String startTimeDelta,
      String endTimeDelta) {
    Map<String, String> cacheUpdateStatus = new HashMap<>();
    cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET,
        Integer.toString(offset));
    cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE,
        Boolean.toString(false));
    cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_START_TIME_DELTA, startTimeDelta);
    cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_END_TIME_DELTA, endTimeDelta);
    return cacheUpdateStatus;
  }

  public void updateCache(String resourceType, Map<String, String> cacheUpdateStatus) {
    new CacheProcessor().setHashAllValsInCache(resourceType, cacheUpdateStatus);
  }

  private String getCurrentTimeInZuluTimeFormat() {
    DateFormat dateFormat = new SimpleDateFormat(HarvesterConstants.SIERRA_API_FIELD_TIME_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));

    logger.info("Current time: " + dateFormat.format(new Date()));

    return dateFormat.format(new Date());
  }

}
