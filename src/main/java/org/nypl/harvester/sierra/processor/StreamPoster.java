package org.nypl.harvester.sierra.processor;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.api.utils.AvroSerializer;
import org.nypl.harvester.sierra.api.utils.StreamDataTranslator;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamPoster implements Processor {

  private static Logger logger = LoggerFactory.getLogger(StreamPoster.class);

  private ProducerTemplate template;

  private String streamName;

  private StreamDataModel streamDataModel;

  public StreamPoster(ProducerTemplate template, String streamName, StreamDataModel streamData) {
    this.template = template;
    this.streamName = streamName;
    this.streamDataModel = streamData;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);
    List<Item> items = (List<Item>) exchangeContents.get(HarvesterConstants.APP_ITEMS_LIST);

    sendToKinesis(items);
  }

  private void sendToKinesis(List<Item> items) throws SierraHarvesterException {
    Schema schema = AvroSerializer.getSchema(this.getStreamDataModel());

    for (Item item : items) {
      Exchange exchange = template.send(
          "aws-kinesis://" + getStreamName() + "?amazonKinesisClient=#getAmazonKinesisClient",
          new Processor() {
            @Override
            public void process(Exchange kinesisRequest) throws SierraHarvesterException {
              try {
                kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_PARTITION_KEY,
                    UUID.randomUUID().toString());
                kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_SEQUENCE_NUMBER,
                    System.currentTimeMillis());

                kinesisRequest.getIn().setBody(AvroSerializer.encode(schema,
                    StreamDataTranslator.translate(getStreamDataModel(), item)));
              } catch (Exception exception) {
                logger.error("Exception thrown encoding data", exception);
                throw new SierraHarvesterException("Error occurred while posting to stream");
              }
            }
          });

      if (exchange.isFailed()) {
        logger.error("Error processing ProducerTemplate", exchange.getException());

        throw new SierraHarvesterException(
            "Error processing ProducerTemplate: " + exchange.getException().getMessage());
      }
    }

    logger.info("Sent " + items.size() + " items to Kinesis stream: " + getStreamName());
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public StreamDataModel getStreamDataModel() {
    return streamDataModel;
  }

  public void setStreamDataModel(StreamDataModel streamDataModel) {
    this.streamDataModel = streamDataModel;
  }
}
