package org.nypl.harvester.sierra.processor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.api.utils.AvroSerializer;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamPoster implements Processor {

  private static Logger logger = LoggerFactory.getLogger(StreamPoster.class);

  private ProducerTemplate template;

  public StreamPoster(ProducerTemplate template) {
    this.template = template;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);
    List<Item> items = (List<Item>) exchangeContents.get(HarvesterConstants.APP_ITEMS_LIST);

    sendToKinesis(items);

    logger.info("Sent " + items.size() + " items to kinesis");
  }

  private void sendToKinesis(List<Item> items) throws SierraHarvesterException {
    Schema schema = AvroSerializer.getSchema(items.get(0));

    for (Item item : items) {
      Exchange kinesisResponse = template.send(
          "aws-kinesis://" +
              System.getenv("kinesisStream") +
              "?amazonKinesisClient=#getAmazonKinesisClient",
          new Processor() {
            @Override
            public void process(Exchange kinesisRequest) {
              try {
                kinesisRequest.getIn().setHeader(
                    HarvesterConstants.KINESIS_PARTITION_KEY,
                    UUID.randomUUID().toString()
                );
                kinesisRequest.getIn().setHeader(
                    HarvesterConstants.KINESIS_SEQUENCE_NUMBER,
                    System.currentTimeMillis()
                );

                kinesisRequest.getIn().setBody(
                    AvroSerializer.encode(schema, item)
                );
              } catch (Exception exception) {
                logger.error("Exception thrown encoding data", exception);
              }
            }
          }
        );
    }
  }

}
