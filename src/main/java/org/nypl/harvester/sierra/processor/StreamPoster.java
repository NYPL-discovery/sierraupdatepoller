package org.nypl.harvester.sierra.processor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.codehaus.jackson.map.ObjectMapper;
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
		sendToKinesis(new ObjectMapper().writeValueAsString(items));
		logger.info("Sent " + items.size() + " items to kinesis");
	}
	
	private void sendToKinesis(String content){
		ByteBuffer byteBuffer = ByteBuffer.wrap(content.getBytes());
		Exchange kinesisResponse = template.send("aws-kinesis://" + 
				System.getenv("kinesisStream")
				+ "?amazonKinesisClient=#getAmazonKinesisClient", new Processor() {
					
					@Override
					public void process(Exchange kinesisRequest) throws Exception {
						kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_PARTITION_KEY, 
								Integer.parseInt(System.getenv("KinesisPartitionKey")));
						kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_SEQUENCE_NUMBER,
								System.currentTimeMillis());
						kinesisRequest.getIn().setBody(byteBuffer);
					}
				});
	}

}
