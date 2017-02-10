package org.nypl.harvester.sierra.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheItemIdMonitor implements Processor{
	
	private ProducerTemplate template;
	
	private static Logger logger = LoggerFactory.getLogger(CacheItemIdMonitor.class);
	
	public CacheItemIdMonitor(ProducerTemplate template) {
		this.template = template;
	}

	@Override
	public void process(Exchange exchange) throws SierraHarvesterException {
		try{
			System.out.println(exchange.getExchangeId());
			Exchange templateResultExchange = template.send("spring-redis://localhost:6379?command=GET"
					+ "&redisTemplate=#redisTemplate", 
					new Processor() {
						
						@Override
						public void process(Exchange redisMessageExchange) throws Exception {
							
							redisMessageExchange.getIn().setHeader("CamelRedis.Key", 
									HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME);
						}
					});
			String value = templateResultExchange.getIn().getBody(String.class);
			exchange.getIn().setBody(value);
			System.out.println(value);
		}catch(Exception exception){
			logger.error("Hit an issue with checking redis - ", exception);
			throw new SierraHarvesterException(exception.getMessage());
		}
	}

}
