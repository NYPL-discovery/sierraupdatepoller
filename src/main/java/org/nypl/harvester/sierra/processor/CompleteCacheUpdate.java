package org.nypl.harvester.sierra.processor;

import java.util.Map;
import java.util.Optional;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.nypl.harvester.sierra.cache.CacheProcessor;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

public class CompleteCacheUpdate implements Processor {

  private RetryTemplate retryTemplate;

  private static Logger logger = LoggerFactory.getLogger(CompleteCacheUpdate.class);

  public CompleteCacheUpdate(RetryTemplate retryTemplate) {
    this.retryTemplate = retryTemplate;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    String resourceWhoseCacheHasToBeUpdated = null;
    try {
      Map<String, Object> resourceTypeAndResourceProcessedStatus =
          exchange.getIn().getBody(Map.class);

      final String resource =
          (String) resourceTypeAndResourceProcessedStatus.get(HarvesterConstants.APP_RESOURCE_TYPE);
      Optional<Boolean> resourceProcessingStatus = Optional.ofNullable(
          (Boolean) resourceTypeAndResourceProcessedStatus.get(HarvesterConstants.IS_PROCESSED));
      
      if(resourceProcessingStatus.isPresent()){
        Boolean status = resourceProcessingStatus.get();
        retryTemplate.execute(new RetryCallback<Boolean, SierraHarvesterException>() {

          @Override
          public Boolean doWithRetry(RetryContext context) throws SierraHarvesterException {
            return updateCache(resource, status);
          }

        });
      }
    } catch (Exception e) {
      logger.error(
          resourceWhoseCacheHasToBeUpdated
              + " : Error occurred while updating redis with final changes that processing is complete - ",
          e);
      throw new SierraHarvesterException(
          "Error occurred while updating redis with final changes that processing is complete - "
              + e.getMessage(),
          resourceWhoseCacheHasToBeUpdated);
    }
  }

  private Boolean updateCache(String resource, Boolean resourceProcessingStatus)
      throws SierraHarvesterException {
    try {
      if(resourceProcessingStatus){
        new CacheProcessor().setHashSingleValInCache(resource,
            HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE, Boolean.toString(true));
        logger.info("Completed updating cache");
        return true;
      }else
        return false;
    } catch (Exception e) {
      logger.error(
          resource + " : Error occurred while getting last updated time from redis server - ", e);
      throw new SierraHarvesterException(
          "Error occurred while getting last updated time from redis server", resource);
    }
  }

}
