package org.nypl.harvester.sierra.routebuilder;

import java.util.Date;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.nypl.harvester.sierra.api.utils.OAuth2Client;
import org.nypl.harvester.sierra.api.utils.TokenProperties;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.processor.CacheItemIdMonitor;
import org.nypl.harvester.sierra.processor.ItemIdUpdateHarvester;
import org.nypl.harvester.sierra.processor.StreamProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RouteBuilderIdPoller extends RouteBuilder{
	
	@Autowired
	private TokenProperties tokenProperties;
	
	@Autowired
	private ProducerTemplate template;
	
	private static Logger logger = LoggerFactory.getLogger(RouteBuilderIdPoller.class);
	
	@Override
	public void configure() throws Exception {
		from("scheduler:sierrapoller?delay=30s&useFixedDelay=true")
		//check redis to see if there is updatedDate key
		.process(new CacheItemIdMonitor(template))
		//send the updatedDatekey value to id harvester
		// id harvester has to validate and then query for that until time now
		.process(new ItemIdUpdateHarvester(getToken(), template))
		// send ids to kinesis
		.process(new StreamProcessor(template));
		// update Kinesis with last checked time
	}
	
	public String getToken() throws SierraHarvesterException {
		try{
			Date currentDate = new Date();
			currentDate.setMinutes(currentDate.getMinutes() + 5);
			if(tokenProperties.getTokenExpiration() == null || 
					!currentDate.before(tokenProperties.getTokenExpiration())){
				logger.info("Requesting new nypl token");
				tokenProperties = generateNewTokenProperties();
				return tokenProperties.getTokenValue();
			}
			logger.info("Token expires - " + tokenProperties.getTokenExpiration());
			logger.info(tokenProperties.getTokenValue());
			return tokenProperties.getTokenValue();
		}catch(Exception e){
			logger.error("Exception caught - ", e);
			throw new SierraHarvesterException("Exception occurred while getting token");
		}
	}
	
	public TokenProperties generateNewTokenProperties(){
		return new OAuth2Client(
				System.getenv("accessTokenUri"),
				System.getenv("clientId"), 
				System.getenv("clientSecret"), 
				System.getenv("grantType"))
				.createAndGetTokenAccessProperties();
	}

}
