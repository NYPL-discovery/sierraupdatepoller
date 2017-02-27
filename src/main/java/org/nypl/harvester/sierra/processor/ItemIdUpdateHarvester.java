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
import java.util.TimeZone;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.http.common.HttpOperationFailedException;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.routebuilder.RouteBuilderIdPoller;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;

public class ItemIdUpdateHarvester implements Processor {

  private static Logger logger = LoggerFactory.getLogger(ItemIdUpdateHarvester.class);

  private ProducerTemplate template;

  private String token;

  private String newUpdatedTime;

  public ItemIdUpdateHarvester(String token, ProducerTemplate producerTemplate) {
		this.token = token;
    this.template = producerTemplate;
  }

  @Override
  public void process(Exchange exchange) throws Exception {
    try {
      String lastUpdatedDateTime = (String) exchange.getIn().getBody();
      lastUpdatedDateTime = validateLastUpdatedTime(lastUpdatedDateTime);

      Map<String, Object> exchangeContents = new HashMap<>();

      exchangeContents.put(
          HarvesterConstants.APP_ITEMS_LIST,
          iterateToGetItemIds(lastUpdatedDateTime)
      );

      exchangeContents.put(
          HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME,
          newUpdatedTime
      );

      exchange.getIn().setBody(exchangeContents);
    } catch (NullPointerException npe) {
      logger.error("Hit nullpointer exception while getting item ids that got updated - ", npe);

      throw new SierraHarvesterException("Nullpointer exception occurred - " + npe.getMessage());
    }
  }

  private List<Item> iterateToGetItemIds(String startTime)
      throws SierraHarvesterException {
    List<Item> items = new ArrayList<>();
    try {
      int offset = 0;
      int limit = 500;
      int total = 0;

      newUpdatedTime = getCurrentTimeInZuluTimeFormat();

      Map<String, Object> response = getResultsFromSierra(
          startTime,
          newUpdatedTime,
          offset,
          limit
      );

      Integer responseCode = (Integer) response.get(
          HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE
      );

      if (responseCode == 200) {
        Map<String, Object> apiResponse = new ObjectMapper().readValue(
            (String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY),
            Map.class
        );

        total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);
        items = addItemsFromAPIResponse(apiResponse, items);

        if (total < limit) {
          return items;
        } else {
          return getAllItemsForTimeRange(
              response,
              total,
              items,
              limit,
              offset,
              startTime,
              newUpdatedTime
          );
        }
      }

      return items;
    } catch (JsonParseException jsonParseException) {
      logger.error(
          "Hit a json parse exception while parsing json response from items " + "api - ",
          jsonParseException
      );

      throw new SierraHarvesterException(
          "JsonParseException while parsing items " + "response - " +
              jsonParseException.getMessage()
      );
    } catch (JsonMappingException jsonMappingException) {
      logger.error(
          "Hit a json mapping exception for items api response ",
          jsonMappingException
      );

      throw new SierraHarvesterException(
          "JsonMappingException while for items api response " + "- " +
              jsonMappingException.getMessage()
      );
    } catch (IOException ioe) {
      logger.error("Hit an IOException - ", ioe);

      throw new SierraHarvesterException(
          "IOException while for items api response " + "- " + ioe.getMessage()
      );
    }
  }

  private List<Item> getAllItemsForTimeRange(Map<String, Object> response, int total,
      List<Item> items, int limit, int offset, String startTime, String endTime)
      throws SierraHarvesterException {
    try {
      while (total == limit) {
        offset += limit;

        response = getResultsFromSierra(
            startTime,
            endTime,
            offset,
            limit
        );

        Integer responseCode = (Integer) response.get(
            HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE
        );

				if (responseCode == 200) {
					Map<String, Object> apiResponse = new ObjectMapper().readValue(
							(String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY),
              Map.class
          );

					total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);

					items = addItemsFromAPIResponse(apiResponse, items);

					if (total < limit) {
						return items;
					}
				} else {
					return items;
				}
      }

      return items;
    } catch (JsonParseException jsonParseException) {
      logger.error(
          "Hit a json parse exception while parsing json response from items " + "api - ",
          jsonParseException
      );

      throw new SierraHarvesterException(
          "JsonParseException while parsing items " + "response - " + jsonParseException.getMessage()
      );
    } catch (JsonMappingException jsonMappingException) {
      logger.error(
          "Hit a json mapping exception for items api response ",
          jsonMappingException
      );

      throw new SierraHarvesterException(
          "JsonMappingException while for items api response " + "- " + jsonMappingException.getMessage()
      );
    } catch (IOException ioe) {
      logger.error("Hit an IOException - ", ioe);

      throw new SierraHarvesterException(
          "IOException while for items api response " + "- " + ioe.getMessage()
      );
    }
  }

  private List<Item> addItemsFromAPIResponse(Map<String, Object> apiResponse, List<Item> items) {
    List<Map<String, Object>> entries = (List<Map<String, Object>>) apiResponse.get(
        HarvesterConstants.SIERRA_API_RESPONSE_ENTRIES
    );

    for (Map<String, Object> entry : entries) {
      Item item = new Item();
      item.setId((String) entry.get(HarvesterConstants.SIERRA_API_RESPONSE_ID));
      items.add(item);
    }

    return items;
  }

  private Map<String, Object> getResultsFromSierra(String startDdate, String endDate,
      int offset, int limit) {
    Exchange apiResponse = getExchangeWithAPIResponse(startDdate, endDate, offset, limit);

    Map<String, Object> response = getResponseFromExchange(apiResponse);

    if ((Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE) == 401) {
      token = new RouteBuilderIdPoller().generateNewTokenProperties().getTokenValue();

      logger.info("Token expired. Got a new token - " + token);

      apiResponse = getExchangeWithAPIResponse(startDdate, endDate, offset, limit);

      response = getResponseFromExchange(apiResponse);
    }

    return response;
  }

  private Exchange getExchangeWithAPIResponse(String startDdate, String endDate,
      int offset, int limit) {
    String itemApiToCall = System.getenv("sierraItemApi") + "?" +
        HarvesterConstants.SIERRA_API_UPDATED_DATE + "=[" + startDdate + "," + endDate + "]&" +
        HarvesterConstants.SIERRA_API_OFFSET + "=" + offset + "&" +
        HarvesterConstants.SIERRA_API_LIMIT + "=" + limit + "&" +
        HarvesterConstants.SIERRA_API_FIELDS_PARAMETER + "=" + HarvesterConstants.SIERRA_API_FIELDS_VALUE;

    logger.info("Calling api - " + itemApiToCall);

    Exchange templateResultExchange = template.send(itemApiToCall,
        new Processor() {
          @Override
          public void process(Exchange httpHeaderExchange) throws Exception {
            httpHeaderExchange.getIn().setHeader(Exchange.HTTP_METHOD, HttpMethod.GET);
            httpHeaderExchange.getIn().setHeader("Authorization", "bearer " + token);
          }
        }
        );

    return templateResultExchange;
  }

  private Map<String, Object> getResponseFromExchange(Exchange exchange) {
    Message out = exchange.getOut();

    HttpOperationFailedException httpOperationFailedException = exchange.getException(
        HttpOperationFailedException.class
    );

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
