package org.nypl.harvester.sierra.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

import redis.clients.jedis.Jedis;

public class CacheItemIdMonitor implements Processor {

  private static Logger logger = LoggerFactory.getLogger(CacheItemIdMonitor.class);
  private RetryTemplate retryTemplate;

  public CacheItemIdMonitor(RetryTemplate retryTemplate) {
    this.retryTemplate = retryTemplate;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    try {
      String value = retryTemplate.execute(new RetryCallback<String, Throwable>() {

        @Override
        public String doWithRetry(RetryContext retryContext) throws Throwable {
          return getCacheStoreValue();
        }

      });
      exchange.getIn().setBody(value);
      logger.debug("Cached last updated date" + value);
    } catch (Exception exception) {
      logger.error("Hit an issue with checking redis - ", exception);
      throw new SierraHarvesterException(exception.getMessage());
    } catch (Throwable e) {
      logger.error("Hit an issue with checking redis - ", e);
      throw new SierraHarvesterException(e.getMessage());
    }
  }
  
  public String getCacheStoreValue() throws SierraHarvesterException{
    Jedis jedis = null;
    try{
      jedis = new Jedis(System.getenv("redisHost"), Integer.parseInt(System.getenv("redisPort")));
      return jedis.get(HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME);
    }catch(Exception e){
      logger.error("Error occurred while getting last updated time from redis server - ", e);
      throw new SierraHarvesterException("Error occurred while getting last updated time from redis server");
    }finally{
      jedis.close();
    }
    
  }

}
