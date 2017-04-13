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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.http.common.HttpOperationFailedException;
import org.nypl.harvester.sierra.cache.CacheProcessor;
import org.nypl.harvester.sierra.cache.CacheResource;
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

  public ResourceIdProcessor(String token, ProducerTemplate producerTemplate,
      RetryTemplate retryTemplate, Map<String, StreamDataModel> streamNameAndDataModel) {
    this.token = token;
    this.template = producerTemplate;
    this.retryTemplate = retryTemplate;
    this.streamNameAndDataModel = streamNameAndDataModel;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    String resourceType = null;
    try {
      Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);
      resourceType = (String) exchangeContents.get(HarvesterConstants.APP_RESOURCE_TYPE);
      Optional<CacheResource> cacheResource = (Optional<CacheResource>) exchangeContents
          .get(HarvesterConstants.APP_OPTIONAL_CACHE_RESOURCE);

      processResourcesAndUpdateCache(cacheResource, resourceType);

      exchange.getIn().setBody(resourceType);
    } catch (NullPointerException npe) {
      logger.error("Hit null pointer exception while getting resource ids that got updated - ",
          npe);

      throw new SierraHarvesterException("Null pointer exception occurred - " + npe.getMessage(),
          resourceType);
    }
  }

  private void processResourcesAndUpdateCache(Optional<CacheResource> optionalCacheResource,
      String resourceType) throws SierraHarvesterException {
    List<Resource> resources = new ArrayList<>();
    CacheResource cacheResource = null;
    String startTime;
    try {
      if (optionalCacheResource.isPresent()) {
        cacheResource = optionalCacheResource.get();
        startTime = validateLastUpdatedTime(cacheResource.getEndTime());
      } else {
        startTime = validateLastUpdatedTime(null);
      }
      int offset = 0;
      int limit = 500;
      int total = 0;

      String endTime = getCurrentTimeInZuluTimeFormat();

      if (optionalCacheResource.isPresent() && !cacheResource.getIsDone()) {
        offset = cacheResource.getOffset();
        startTime = cacheResource.getStartTime();
        endTime = cacheResource.getEndTime();
      }

      Map<String, Object> response =
          getResultsFromSierra(startTime, endTime, offset, limit, resourceType);

      Integer responseCode =
          (Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE);

      if (responseCode == 200) {
        Map<String, Object> apiResponse = new ObjectMapper().readValue(
            (String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY), Map.class);

        Optional<Integer> optionalTotal = Optional
            .ofNullable((Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL));

        if (optionalTotal.isPresent()) {
          total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);
          resources = addResourcesFromAPIResponse(apiResponse, resources, resourceType);

          if (total < limit) {
            postResourcesAndUpdateCache(resources, offset, startTime, endTime, resourceType);
          } else { // total will always be less than or equal to the limit
            postResourcesAndUpdateCache(resources, offset, startTime, endTime, resourceType);
            getAllResourcesForTimeRange(response, total, limit, offset, startTime, endTime,
                resourceType);
          }
        }
      }
    } catch (JsonParseException jsonParseException) {
      logger.error(cacheResource.getResourceType()
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
      logger.error(cacheResource.getResourceType() + " : Hit an IOException - ", ioe);

      throw new SierraHarvesterException(
          "IOException while for api response " + "- " + ioe.getMessage(), resourceType);
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

          resources = addResourcesFromAPIResponse(apiResponse, resources, resourceType);

          postResourcesAndUpdateCache(resources, offset, startTime, endTime, resourceType);
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

  private List<Resource> addResourcesFromAPIResponse(Map<String, Object> apiResponse,
      List<Resource> resources, String resourceType) throws SierraHarvesterException {
    try {
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

  private Map<String, Object> getResultsFromSierra(String startDdate, String endDate, int offset,
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

  private Exchange getExchangeWithAPIResponse(String startDdate, String endDate, int offset,
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
                logger.error("Error occurred while calling sierra api - ", e);
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

  private Map<String, Object> getResponseFromExchange(Exchange exchange, String resourceType)
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

  private void postResourcesAndUpdateCache(List<Resource> resources, int offset,
      String startTimeDelta, String endTimeDelta, String resourceType)
      throws SierraHarvesterException {
    for (Entry<String, StreamDataModel> entry : streamNameAndDataModel.entrySet()) {
      ResourcePoster poster = new StreamPoster(entry.getKey(), entry.getValue(), retryTemplate);
      poster.postResources(template, resources, resourceType);
      Map<String, String> cacheUpdateStatus = new HashMap<>();
      cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET,
          Integer.toString(offset));
      cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE,
          Boolean.toString(false));
      cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_START_TIME_DELTA, startTimeDelta);
      cacheUpdateStatus.put(HarvesterConstants.REDIS_KEY_END_TIME_DELTA, endTimeDelta);
      new CacheProcessor().setHashAllValsInCache(resourceType, cacheUpdateStatus);
    }
  }

  private String validateLastUpdatedTime(String lastUpdatedTime) {
    if (lastUpdatedTime == null) {
      Date currentDate = new Date();

      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));

      logger.info("Last updated time: " + dateFormat.format(currentDate).concat("T00:00:00Z"));

      return dateFormat.format(currentDate).concat("T00:00:00Z");
    } else {
      return lastUpdatedTime;
    }
  }

  private String getCurrentTimeInZuluTimeFormat() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));

    logger.info("Current time: " + dateFormat.format(new Date()));

    return dateFormat.format(new Date());
  }

}
