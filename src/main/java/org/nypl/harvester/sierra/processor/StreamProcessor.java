package org.nypl.harvester.sierra.processor;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.codehaus.jackson.map.ObjectMapper;
import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.utils.HarvesterConstants;

public class StreamProcessor implements Processor{
	
	private ProducerTemplate template;
	
	public StreamProcessor(ProducerTemplate template) {
		this.template = template;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		List<Item> items = exchange.getIn().getBody(List.class);
		sendToKinesis(new ObjectMapper().writeValueAsString(items));
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
