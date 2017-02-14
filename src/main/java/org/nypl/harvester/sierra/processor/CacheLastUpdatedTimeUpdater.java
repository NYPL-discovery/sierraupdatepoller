package org.nypl.harvester.sierra.processor;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.utils.HarvesterConstants;

public class CacheLastUpdatedTimeUpdater implements Processor{
	
	private ProducerTemplate template;
	
	public CacheLastUpdatedTimeUpdater(ProducerTemplate template) {
		this.template = template;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);
		String timeToUpdateInCache = (String) exchangeContents.get(
				HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME);
		updateCache(timeToUpdateInCache);
	}
	
	private void updateCache(String timeToUpdateInCache){
		Exchange templateResultExchange = template.send("spring-redis://" + 
				System.getenv("redisHost") + ":" + System.getenv("redisPort") + "?command=SET"
							+ "&redisTemplate=#redisTemplate", 
							new Processor() {
								
								@Override
								public void process(Exchange redisMessageExchange) throws Exception {
									
									redisMessageExchange.getIn().setHeader("CamelRedis.Key", 
											HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME);
									redisMessageExchange.getIn().setHeader("CamelRedis.Value", 
											timeToUpdateInCache);
								}
							});
	}
					
}
