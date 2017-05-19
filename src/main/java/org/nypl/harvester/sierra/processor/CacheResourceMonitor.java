package org.nypl.harvester.sierra.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.nypl.harvester.sierra.cache.CacheProcessor;
import org.nypl.harvester.sierra.cache.CacheResource;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

public class CacheResourceMonitor implements Processor {

  private static Logger logger = LoggerFactory.getLogger(CacheResourceMonitor.class);
  private RetryTemplate retryTemplate;
  private String resourceType;

  public CacheResourceMonitor(RetryTemplate retryTemplate, String resourceType) {
    this.retryTemplate = retryTemplate;
    this.resourceType = resourceType;
  }

  @Override
  public void process(Exchange exchange) throws SierraHarvesterException {
    try {
      Optional<CacheResource> cacheResource = retryTemplate
          .execute(new RetryCallback<Optional<CacheResource>, SierraHarvesterException>() {

            @Override
            public Optional<CacheResource> doWithRetry(RetryContext retryContext)
                throws SierraHarvesterException {
              return getCacheResource();
            }

          });
      Map<String, Object> exchangeProperties = new HashMap<>();
      exchangeProperties.put(HarvesterConstants.APP_OPTIONAL_CACHE_RESOURCE, cacheResource);
      exchangeProperties.put(HarvesterConstants.APP_RESOURCE_TYPE, resourceType);
      exchange.getIn().setBody(exchangeProperties);
    } catch (Exception exception) {
      logger.error(resourceType + " : Hit an issue with checking redis - ", exception);
      throw new SierraHarvesterException(exception.getMessage(), resourceType);
    }
  }

  public Optional<CacheResource> getCacheResource() throws SierraHarvesterException {
    try {
      Optional<Map<String, String>> optionalCacheResource =
          Optional.ofNullable(new CacheProcessor().getHashAllValsInCache(resourceType));
      if (optionalCacheResource.isPresent() && optionalCacheResource.get().size() > 0) {
        Map<String, String> cacheProperties = optionalCacheResource.get();
        if (Optional.ofNullable(cacheProperties.get(HarvesterConstants.REDIS_KEY_END_TIME_DELTA))
            .isPresent()
            && Optional
                .ofNullable(cacheProperties.get(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET))
                .isPresent()
            && Optional
                .ofNullable(cacheProperties.get(HarvesterConstants.REDIS_KEY_START_TIME_DELTA))
                .isPresent()
            && Optional
                .ofNullable(
                    cacheProperties.get(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE))
                .isPresent()) {
          CacheResource cacheResource = new CacheResource();
          cacheResource
              .setEndTime(cacheProperties.get(HarvesterConstants.REDIS_KEY_END_TIME_DELTA));
          cacheResource.setIsDone(Boolean.parseBoolean(
              cacheProperties.get(HarvesterConstants.REDIS_KEY_APP_RESOURCE_UPDATE_COMPLETE)));
          cacheResource.setOffset(Integer
              .parseInt(cacheProperties.get(HarvesterConstants.REDIS_KEY_LAST_UPDATED_OFFSET)));
          cacheResource.setResourceType(resourceType);
          cacheResource
              .setStartTime(cacheProperties.get(HarvesterConstants.REDIS_KEY_START_TIME_DELTA));
          return Optional.of(cacheResource);
        } else
          return Optional.ofNullable(null);
      } else
        return Optional.ofNullable(null);
    } catch (Exception e) {
      logger.error(
          resourceType + " : Error occurred while getting cached resource from redis server - ", e);
      throw new SierraHarvesterException(
          "Error occurred while getting cached resource from redis server", resourceType);
    }
  }

}
