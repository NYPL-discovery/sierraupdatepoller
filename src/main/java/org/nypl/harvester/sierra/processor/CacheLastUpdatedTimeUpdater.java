package org.nypl.harvester.sierra.processor;

import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

import redis.clients.jedis.Jedis;

public class CacheLastUpdatedTimeUpdater implements Processor {
  
  private RetryTemplate retryTemplate;

  private static Logger logger = LoggerFactory.getLogger(CacheLastUpdatedTimeUpdater.class);
  
  public CacheLastUpdatedTimeUpdater(RetryTemplate retryTemplate) {
    this.retryTemplate = retryTemplate;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);

    String timeToUpdateInCache =
        (String) exchangeContents.get(HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME);
    retryTemplate.execute(new RetryCallback<Boolean, SierraHarvesterException>() {

      @Override
      public Boolean doWithRetry(RetryContext context) throws SierraHarvesterException {
        return updateCache(timeToUpdateInCache);
      }
      
    });
    
  }

  private Boolean updateCache(String timeToUpdateInCache) throws SierraHarvesterException {
    Jedis jedis = null;
    try {
      jedis = new Jedis(System.getenv("redisHost"), Integer.parseInt(System.getenv("redisPort")));
      jedis.set(HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME, timeToUpdateInCache);
      return true;
    } catch (Exception e) {
      logger.error("Error occurred while getting last updated time from redis server - ", e);
      throw new SierraHarvesterException(
          "Error occurred while getting last updated time from redis server");
    } finally {
      jedis.close();
    }
  }

}
