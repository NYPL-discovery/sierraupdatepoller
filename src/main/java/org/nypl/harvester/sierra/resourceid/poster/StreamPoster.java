package org.nypl.harvester.sierra.resourceid.poster;

import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.api.utils.AvroSerializer;
import org.nypl.harvester.sierra.api.utils.StreamDataTranslator;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

public class StreamPoster implements ResourcePoster {

  private static Logger logger = LoggerFactory.getLogger(StreamPoster.class);

  private String streamName;

  private StreamDataModel streamDataModel;

  private RetryTemplate retryTemplate;

  public StreamPoster(String streamName, StreamDataModel streamData, RetryTemplate retryTemplate) {
    this.streamName = streamName;
    this.streamDataModel = streamData;
    this.retryTemplate = retryTemplate;
  }

  @Override
  public void postResources(ProducerTemplate template, List<Resource> resources,
      String resourceType) throws SierraHarvesterException {
    try {
      Schema schema = AvroSerializer.getSchema(this.getStreamDataModel());

      for (Resource resource : resources) {
        retryTemplate.execute(new RetryCallback<Exchange, SierraHarvesterException>() {

          @Override
          public Exchange doWithRetry(RetryContext context) throws SierraHarvesterException {
            Exchange exchange = template.request(
                "aws-kinesis://" + getStreamName() + "?amazonKinesisClient=#getAmazonKinesisClient",
                new Processor() {
                  @Override
                  public void process(Exchange kinesisRequest) throws SierraHarvesterException {
                    try {
                      kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_PARTITION_KEY,
                          UUID.randomUUID().toString());
                      kinesisRequest.getIn().setHeader(HarvesterConstants.KINESIS_SEQUENCE_NUMBER,
                          System.currentTimeMillis());

                      kinesisRequest.getIn()
                          .setBody(AvroSerializer.encode(schema, StreamDataTranslator.translate(
                              getStreamDataModel(), resource, resourceType), resourceType));
                    } catch (Exception exception) {
                      logger.error(resourceType + " : Exception thrown encoding data", exception);
                      throw new SierraHarvesterException("Error occurred while posting to stream",
                          resourceType);
                    }
                  }
                });

            if (exchange.isFailed()) {
              logger.error(resourceType + " : Error processing ProducerTemplate",
                  exchange.getException());

              throw new SierraHarvesterException(
                  "Error sending resources to kinesis: " + exchange.getException().getMessage(),
                  resourceType);
            }
            return exchange;
          }
        });

      }

      logger.info(resourceType + " : Sent " + resources.size() + " resources to Kinesis stream: "
          + getStreamName());
    } catch (Exception e) {
      logger.error(resourceType + " : Error occurred while sending resources to kinesis - ", e);
      throw new SierraHarvesterException(
          "Error occurred while sending resources to kinesis - " + e.getMessage(), resourceType);
    }
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
