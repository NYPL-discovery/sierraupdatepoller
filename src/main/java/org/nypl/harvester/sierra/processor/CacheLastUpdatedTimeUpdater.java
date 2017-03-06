package org.nypl.harvester.sierra.processor;

import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class CacheLastUpdatedTimeUpdater implements Processor {

  private static Logger logger = LoggerFactory.getLogger(CacheLastUpdatedTimeUpdater.class);

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    Map<String, Object> exchangeContents = exchange.getIn().getBody(Map.class);

    String timeToUpdateInCache =
        (String) exchangeContents.get(HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME);

    updateCache(timeToUpdateInCache);
  }

  private void updateCache(String timeToUpdateInCache) throws SierraHarvesterException {
    Jedis jedis = null;
    try {
      jedis = new Jedis(System.getenv("redisHost"), Integer.parseInt(System.getenv("redisPort")));
      jedis.set(HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME, timeToUpdateInCache);
    } catch (Exception e) {
      logger.error("Error occurred while getting last updated time from redis server - ", e);
      throw new SierraHarvesterException(
          "Error occurred while getting last updated time from redis server");
    } finally {
      jedis.close();
    }
  }

}
