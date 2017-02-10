package org.nypl.harvester.sierra.processor;

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
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ItemIdUpdateHarvester implements Processor{
	
	private ProducerTemplate template;
	
	private static Logger logger = LoggerFactory.getLogger(ItemIdUpdateHarvester.class);
	
	private String token;
	
	public ItemIdUpdateHarvester(String token, ProducerTemplate producerTemplate) {
		this.token = token;
		this.template = producerTemplate;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		try{
			String lastUpdatedDateTime = (String) exchange.getIn().getBody();
			lastUpdatedDateTime = validateLastUpdatedTime(lastUpdatedDateTime);
			exchange.getIn().setBody(iterateToGetItemIds(lastUpdatedDateTime));
		}catch(NullPointerException npe){
			logger.error("Hit nullpointer exception while getting item ids that got updated - ", npe);
			throw new SierraHarvesterException("Nullpointer exception occurred - " + npe.getMessage());
		}
	}
	
	private List<Item> iterateToGetItemIds(String startTime) 
			throws SierraHarvesterException{
		List<Item> items = new ArrayList<>();
		int offset = 0;
		int limit = 500;
		int total = 0;
		String endTime = getCurrentTimeInZuluTimeFormat();
		Map<String, Object> response = getResultsFromSierra(startTime, 
				endTime, offset, limit);
		Optional<Integer> optionalResponseCode = Optional.ofNullable( 
				(Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE));
		if(optionalResponseCode.isPresent() && 
				 (Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE) != 404){
			return getAllItemsForTimeRange(response, total, items, limit, offset, 
					startTime, endTime);
		}
		return items;
	}
	
	private List<Item> getAllItemsForTimeRange(Map<String, Object> response, int total, 
			List<Item> items, int limit, int offset, String startTime, String endTime) 
					throws SierraHarvesterException{
		try{
			Map<String, Object> apiResponse = new ObjectMapper().readValue(
					(String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY), Map.class);
				total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);
				items = addItemsFromAPIResponse(apiResponse, items);
				if(total < limit){
					return items;
				}else{
					while(total == limit){
						offset += limit;
						response = getResultsFromSierra(startTime, 
								endTime, offset, limit);
						Optional<Integer> optionalResponseCode = Optional.ofNullable(
								(Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE));
						if(optionalResponseCode.isPresent() && 
								(Integer) response.get(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE) 
								!= 404){
							apiResponse = new ObjectMapper().readValue(
								(String) response.get(HarvesterConstants.SIERRA_API_RESPONSE_BODY), Map.class);
							total = (Integer) apiResponse.get(HarvesterConstants.SIERRA_API_RESPONSE_TOTAL);
							items = addItemsFromAPIResponse(apiResponse, items);
							if(total < limit){
								return items;
							}
						}else if(!optionalResponseCode.isPresent())
							return items;
					}
				}
				return items;
		}catch(JsonParseException jsonParseException){
			logger.error("Hit a json parse exception while parsing json response from items "
					+ "api - ", jsonParseException);
			throw new SierraHarvesterException("JsonParseException while parsing items "
					+ "response - " + jsonParseException.getMessage());
		}catch(JsonMappingException jsonMappingException){
			logger.error("Hit a json mapping exception for items api response ", 
					jsonMappingException);
			throw new SierraHarvesterException("JsonMappingException while for items api response "
					+ "- " + jsonMappingException.getMessage());
		}catch(IOException ioe){
			logger.error("Hit an IOException - ", ioe);
			throw new SierraHarvesterException("IOException while for items api response "
					+ "- " + ioe.getMessage());
		}
	}
	
	private List<Item> addItemsFromAPIResponse(Map<String, Object> apiResponse, List<Item> items){
		List<Map<String, Object>> entries = (List<Map<String, Object>>) apiResponse.
				get(HarvesterConstants.SIERRA_API_RESPONSE_ENTRIES);
		for(Map<String, Object> entry : entries){
			Item item = new Item();
			item.setId((String) entry.get(HarvesterConstants.SIERRA_API_RESPONSE_ID));
			items.add(item);
		}
		return items;
	}
	
	private Map<String, Object> getResultsFromSierra(String startDdate, String endDate, 
			int offset, int limit){
		String itemApiToCall = System.getenv("sierraItemAPI") + "?" + 
				HarvesterConstants.SIERRA_API_UPDATED_DATE + "=[" + 
				startDdate + "," + endDate + "]&" + 
				HarvesterConstants.SIERRA_API_OFFSET + "=" + offset + "&" + 
				HarvesterConstants.SIERRA_API_LIMIT + "=" + limit;
		logger.info("Calling api - " + itemApiToCall);
		Exchange templateResultExchange = template.send(itemApiToCall, 
				new Processor() {
					
					@Override
					public void process(Exchange httpHeaderExchange) throws Exception {
						httpHeaderExchange.getIn().setHeader(Exchange.HTTP_METHOD, HttpMethod.GET);
						httpHeaderExchange.getIn().setHeader("Authorization", "bearer " + token);
					}
				});
		Message out = templateResultExchange.getOut();
		Integer responseCode = null;
		String apiResponse = null;
		responseCode = (Integer) out.getHeader(Exchange.HTTP_RESPONSE_CODE);
		logger.info("Sierra api response code - " + responseCode);
		apiResponse = out.getBody(String.class);
		logger.info("Sierra api response body - " + apiResponse);
		Map<String, Object> response = new HashMap<>();
		response.put(HarvesterConstants.SIERRA_API_RESPONSE_HTTP_CODE, responseCode);
		response.put(HarvesterConstants.SIERRA_API_RESPONSE_BODY, apiResponse);
		return response;
	}
	
	private String validateLastUpdatedTime(String lastUpdatedTime) {
		if (lastUpdatedTime == null){
			Date currentDate = new Date();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));
			System.out.println(dateFormat.format(currentDate).concat("T00:00:00Z"));
			return dateFormat.format(currentDate).concat("T00:00:00Z");
		}else{
			return lastUpdatedTime;
		}
	}
	
	private String getCurrentTimeInZuluTimeFormat(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		dateFormat.setTimeZone(TimeZone.getTimeZone("Zulu"));
		System.out.println(dateFormat.format(new Date()));
		return dateFormat.format(new Date());
	}

}
