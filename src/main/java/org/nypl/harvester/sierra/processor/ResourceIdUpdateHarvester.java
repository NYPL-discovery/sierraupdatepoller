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
import java.util.Optional;
import java.util.TimeZone;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.http.common.HttpOperationFailedException;
import org.nypl.harvester.sierra.config.EnvironmentConfig;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.routebuilder.RouteBuilderIdPoller;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

public class ResourceIdUpdateHarvester implements Processor {

  private static Logger logger = LoggerFactory.getLogger(ResourceIdUpdateHarvester.class);

  private ProducerTemplate template;

  private String token;

  private String newUpdatedTime;

  private RetryTemplate retryTemplate;

  public ResourceIdUpdateHarvester(String token, ProducerTemplate producerTemplate,
      RetryTemplate retryTemplate) {
    this.token = token;
    this.template = producerTemplate;
    this.retryTemplate = retryTemplate;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    try {
      String lastUpdatedDateTime = (String) exchange.getIn().getBody();
      lastUpdatedDateTime = validateLastUpdatedTime(lastUpdatedDateTime);

      Map<String, Object> exchangeContents = new HashMap<>();

      exchangeContents.put(HarvesterConstants.APP_RESOURCES_LIST,
          iterateToGetResourceIds(lastUpdatedDateTime));

      exchangeContents.put(HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME, newUpdatedTime);

      exchange.getIn().setBody(exchangeContents);
    } catch (NullPointerException npe) {
      logger.error(
          HarvesterConstants.getResource()
              + " : Hit null pointer exception while getting resource ids that got updated - ",
          npe);

      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : Null pointer exception occurred - " + npe.getMessage());
    }
  }

  private List<Resource> iterateToGetResourceIds(String startTime) throws SierraHarvesterException {
    List<Resource> resources = new ArrayList<>();

    try {
      int offset = 0;
      int limit = 500;
      int total = 0;

      newUpdatedTime = getCurrentTimeInZuluTimeFormat();

      Map<String, Object> response = getResultsFromSierra(startTime, newUpdatedTime, offset, limit);

      Integer responseCode =
          (Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE);

      if (responseCode == 200) {
        Map<String, Object> apiResponse = new ObjectMapper().readValue(
            (String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY), Map.class);

        Optional<Integer> optionalTotal = Optional
            .ofNullable((Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL));

        if (optionalTotal.isPresent()) {
          total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);
          resources = addResourcesFromAPIResponse(apiResponse, resources);

          if (total < limit) {
            return resources;
          } else {
            return getAllResourcesForTimeRange(response, total, resources, limit, offset, startTime,
                newUpdatedTime);
          }
        }
      }

      return resources;
    } catch (JsonParseException jsonParseException) {
      logger.error(HarvesterConstants.getResource()
          + " : Hit a json parse exception while parsing json response from resources " + "api - ",
          jsonParseException);

      throw new SierraHarvesterException(
          HarvesterConstants.getResource() + " : JsonParseException while parsing resources "
              + "response - " + jsonParseException.getMessage());
    } catch (JsonMappingException jsonMappingException) {
      logger.error(
          HarvesterConstants.getResource() + " : Hit a json mapping exception for api response ",
          jsonMappingException);

      throw new SierraHarvesterException(
          HarvesterConstants.getResource() + " : JsonMappingException while for api response "
              + "- " + jsonMappingException.getMessage());
    } catch (IOException ioe) {
      logger.error(HarvesterConstants.getResource() + " : Hit an IOException - ", ioe);

      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : IOException while for api response " + "- " + ioe.getMessage());
    }
  }

  private List<Resource> getAllResourcesForTimeRange(Map<String, Object> response, int total,
      List<Resource> resources, int limit, int offset, String startTime, String endTime)
      throws SierraHarvesterException {
    try {
      while (total == limit) {
        offset += limit;

        response = getResultsFromSierra(startTime, endTime, offset, limit);

        Integer responseCode =
            (Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE);

        if (responseCode == 200) {
          Map<String, Object> apiResponse = new ObjectMapper().readValue(
              (String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY), Map.class);

          total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);

          resources = addResourcesFromAPIResponse(apiResponse, resources);

          if (total < limit) {
            return resources;
          }
        } else {
          return resources;
        }
      }

      return resources;
    } catch (JsonParseException jsonParseException) {
      logger.error(HarvesterConstants.getResource()
          + " : Hit a json parse exception while parsing json response from resources " + "api - ",
          jsonParseException);

      throw new SierraHarvesterException(
          HarvesterConstants.getResource() + ": JsonParseException while parsing resources "
              + "response - " + jsonParseException.getMessage());
    } catch (JsonMappingException jsonMappingException) {
      logger.error(
          HarvesterConstants.getResource()
              + " : Hit a json mapping exception for resources api response ",
          jsonMappingException);

      throw new SierraHarvesterException(
          HarvesterConstants.getResource() + " : JsonMappingException for resources api response "
              + "- " + jsonMappingException.getMessage());
    } catch (IOException ioe) {
      logger.error(HarvesterConstants.getResource() + " : Hit an IOException - ", ioe);

      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : IOException for resources api response " + "- " + ioe.getMessage());
    }
  }

  private List<Resource> addResourcesFromAPIResponse(Map<String, Object> apiResponse,
      List<Resource> resources) throws SierraHarvesterException {
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
      logger.error(HarvesterConstants.getResource()
          + " : Error occurred while processing resources from sierra api response - ", e);
      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : Error while processing sierra resources from sierra response - " + e.getMessage());
    }
  }

  private Map<String, Object> getResultsFromSierra(String startDdate, String endDate, int offset,
      int limit) throws SierraHarvesterException {
    try {
      Exchange apiResponse = getExchangeWithAPIResponse(startDdate, endDate, offset, limit);

      Map<String, Object> response = getResponseFromExchange(apiResponse);

      if ((Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE) == 401) {
        token = new RouteBuilderIdPoller().generateNewTokenProperties().getTokenValue();

        logger.info(
            HarvesterConstants.getResource() + " : Token expired. Got a new token - " + token);

        apiResponse = getExchangeWithAPIResponse(startDdate, endDate, offset, limit);

        response = getResponseFromExchange(apiResponse);
      }

      return response;
    } catch (Exception e) {
      logger.error(HarvesterConstants.getResource()
          + " : Error occurred while getting results from sierra - ", e);
      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : Error while getting results from sierra - " + e.getMessage());
    }

  }

  private Exchange getExchangeWithAPIResponse(String startDdate, String endDate, int offset,
      int limit) throws SierraHarvesterException {
    try {
      String apiToCall =
          EnvironmentConfig.sierraApi + "?" + HarvesterConstants.SIERRA_API_UPDATED_DATE + "=["
              + startDdate + "," + endDate + "]&" + HarvesterConstants.SIERRA_API_OFFSET + "="
              + offset + "&" + HarvesterConstants.SIERRA_API_LIMIT + "=" + limit + "&"
              + HarvesterConstants.SIERRA_API_FIELDS_PARAMETER + "="
              + HarvesterConstants.SIERRA_API_FIELDS_VALUE;

      logger.info(HarvesterConstants.getResource() + " : Calling api - " + apiToCall);

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
                logger.error(HarvesterConstants.getResource()
                    + " : Error occurred while calling sierra api - ", e);
                throw new SierraHarvesterException(HarvesterConstants.getResource()
                    + " : Error occurred while calling sierra api - " + e.getMessage());
              }
            }

          });

      return templateResultExchange;
    } catch (Exception e) {
      logger.error(
          HarvesterConstants.getResource() + " : Error occurred while calling sierra api - ", e);
      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : Error while calling sierra api - " + e.getMessage());
    }

  }

  private Map<String, Object> getResponseFromExchange(Exchange exchange)
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

      logger
          .info(HarvesterConstants.getResource() + " : Sierra api response code - " + responseCode);
      logger
          .info(HarvesterConstants.getResource() + " : Sierra api response body - " + apiResponse);

      Map<String, Object> response = new HashMap<>();

      response.put(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE, responseCode);
      response.put(HarvesterConstants.SIERRA_API_RESPONSE_BODY, apiResponse);

      return response;
    } catch (Exception e) {
      logger.error(HarvesterConstants.getResource()
          + " : Error occurred while processing camel exchange to get sierra response - ", e);
      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : Error while trying to get sierra response - " + e.getMessage());
    }
  }

  private String validateLastUpdatedTime(String lastUpdatedTime) {
    if (lastUpdatedTime == null) {
      Date currentDate = new Date();

      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));

      logger.info(HarvesterConstants.getResource() + " : Last updated time: "
          + dateFormat.format(currentDate).concat("T00:00:00Z"));

      return dateFormat.format(currentDate).concat("T00:00:00Z");
    } else {
      return lastUpdatedTime;
    }
  }

  private String getCurrentTimeInZuluTimeFormat() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));

    logger.info(
        HarvesterConstants.getResource() + " : Current time: " + dateFormat.format(new Date()));

    return dateFormat.format(new Date());
  }

}
