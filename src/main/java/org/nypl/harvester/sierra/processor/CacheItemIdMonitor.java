package org.nypl.harvester.sierra.processor;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheItemIdMonitor implements Processor {

  private static Logger logger = LoggerFactory.getLogger(CacheItemIdMonitor.class);
  private ProducerTemplate template;

  public CacheItemIdMonitor(ProducerTemplate template) {
    this.template = template;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    try {
      Exchange templateResultExchange = template.send(
          "spring-redis://" +
              System.getenv("redisHost") + ":" + System.getenv("redisPort") + "?command=GET"
              + "&redisTemplate=#redisTemplate",
          new Processor() {
            @Override
            public void process(Exchange redisMessageExchange) throws Exception {
              redisMessageExchange.getIn().setHeader("CamelRedis.Key",
                  HarvesterConstants.REDIS_KEY_LAST_UPDATED_TIME);
            }
          });

      String value = templateResultExchange.getIn().getBody(String.class);
      exchange.getIn().setBody(value);

      logger.debug("Cached last updated date" + value);
    } catch (Exception exception) {
      logger.error("Hit an issue with checking redis - ", exception);

      throw new SierraHarvesterException(exception.getMessage());
    }
  }

}
