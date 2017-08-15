package org.nypl.harvester.sierra.routebuilder;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.nypl.harvester.sierra.api.utils.OAuth2Client;
import org.nypl.harvester.sierra.api.utils.TokenProperties;
import org.nypl.harvester.sierra.config.BaseConfig;
import org.nypl.harvester.sierra.config.EnvironmentConfig;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.nypl.harvester.sierra.model.streamdatamodel.SierraResourceRetrievalRequest;
import org.nypl.harvester.sierra.model.streamdatamodel.SierraResourceUpdate;
import org.nypl.harvester.sierra.processor.CacheResourceMonitor;
import org.nypl.harvester.sierra.processor.CompleteCacheUpdate;
import org.nypl.harvester.sierra.processor.ResourceIdProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Component
public class RouteBuilderIdPoller extends RouteBuilder {

  private static Logger logger = LoggerFactory.getLogger(RouteBuilderIdPoller.class);

  @Autowired
  private TokenProperties tokenProperties;

  @Autowired
  private RetryTemplate retryTemplate;

  @Autowired
  private ProducerTemplate template;

  @Autowired
  private BaseConfig baseConfig;

  @Override
  public void configure() throws Exception {

    Map<String, StreamDataModel> streamNameAndDataModel = new HashMap<>();
    streamNameAndDataModel.put(EnvironmentConfig.kinesisResourceRetrievalRequestStream,
        new SierraResourceRetrievalRequest());
    streamNameAndDataModel.put(EnvironmentConfig.kinesisUpdateStream, new SierraResourceUpdate());

    onException(SierraHarvesterException.class).process(new Processor() {

      @Override
      public void process(Exchange exchange) throws Exception {
        logger.error(EnvironmentConfig.resourceType
            + " : FATAL ERROR OCCURRED - Component: sierraupdatepoller");
      }
    });

    from("scheduler:sierrapoller?delay=" + EnvironmentConfig.pollDelay + "&useFixedDelay=true")
        // check redis for resource that has offset, lastupdatedinfo etc
        .process(new CacheResourceMonitor(retryTemplate, EnvironmentConfig.resourceType))
        // send the resource for processing (getting full bib/item data from sierra and posting to
        // kinesis)
        .process(new ResourceIdProcessor(getToken(), template, retryTemplate,
            streamNameAndDataModel, baseConfig))
        // update redis as this iteration is complete
        .process(new CompleteCacheUpdate(retryTemplate));
  }

  public String getToken() throws SierraHarvesterException {
    try {
      Date currentDate = new Date();
      currentDate.setMinutes(currentDate.getMinutes() + 5);

      if (tokenProperties.getTokenExpiration() == null
          || !currentDate.before(tokenProperties.getTokenExpiration())) {
        logger.info(EnvironmentConfig.resourceType + " : Requesting new token");

        tokenProperties = generateNewTokenProperties();
        return tokenProperties.getTokenValue();
      }

      logger.info(EnvironmentConfig.resourceType + " : Token expires - "
          + tokenProperties.getTokenExpiration());
      logger.info(tokenProperties.getTokenValue());

      return tokenProperties.getTokenValue();
    } catch (Exception e) {
      logger.error(EnvironmentConfig.resourceType + " : Exception caught - ", e);

      throw new SierraHarvesterException("Exception occurred while getting token",
          EnvironmentConfig.resourceType);
    }
  }

  public TokenProperties generateNewTokenProperties() throws SierraHarvesterException {
    try {
      return new OAuth2Client(EnvironmentConfig.accessTokenUri, EnvironmentConfig.clientId,
          EnvironmentConfig.clientSecret, EnvironmentConfig.grantType)
              .createAndGetTokenAccessProperties();
    } catch (Exception e) {
      logger.error(EnvironmentConfig.resourceType
          + " : Error occurred while retrieving sierra token properties - ", e);
      throw new SierraHarvesterException(
          "Error occurred while retrieving sierra token properties - " + e.getMessage(),
          EnvironmentConfig.resourceType);
    }
  }
}
