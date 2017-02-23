package org.nypl.harvester.sierra.processor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamPoster implements Processor{
	
	private ProducerTemplate template;
	
	private static Logger logger = LoggerFactory.getLogger(StreamPoster.class);
	
	public StreamPoster(ProducerTemplate template) {
		this.template = template;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);
		List<Item> items = (List<Item>) exchangeContents.get(HarvesterConstants.APP_ITEMS_LIST);
		sendToKinesis(items);
		logger.info("Sent " + items.size() + " items to kinesis");
	}
	
	private void sendToKinesis(List<Item> items) throws SierraHarvesterException{
		try{
			for(Item item : items){
				String content = new ObjectMapper().writeValueAsString(item);
				ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());
				Exchange kinesisResponse = template.send("aws-kinesis://" + 
						System.getenv("kinesisStream")
						+ "?amazonKinesisClient=#getAmazonKinesisClient", new Processor() {
							
							@Override
							public void process(Exchange kinesisRequest) throws Exception {
								kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_PARTITION_KEY,
										UUID.randomUUID());
								kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_SEQUENCE_NUMBER,
										System.currentTimeMillis());
								kinesisRequest.getIn().setBody(byteBuffer);
							}
						});
			}
		}catch(JsonGenerationException jsonGenerationException){
			logger.error("Hit json generation exception while converting item to json - ", 
					jsonGenerationException);
			throw new SierraHarvesterException("Hit an exception while processing item");
		}catch(JsonMappingException jsonMappingException){
			logger.error("Hit json mapping exception while converting item to json - ", 
					jsonMappingException);
			throw new SierraHarvesterException("Hit an exception while processing item");
		}catch(IOException ioException){
			logger.error("Hit IOException while converting item to json - ", 
					ioException);
			throw new SierraHarvesterException("Hit an exception while processing item");
		}
	}

}
