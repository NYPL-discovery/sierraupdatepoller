package org.nypl.harvester.sierra.config;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class BaseConfig {

  private static Logger logger = LoggerFactory.getLogger(BaseConfig.class);

  @Bean
  JedisConnectionFactory jedisConnectionFactory() {
    return new JedisConnectionFactory();
  }

  @Bean
  RedisTemplate<String, Object> redisTemplate() {
    final RedisTemplate<String, Object> template = new RedisTemplate<String, Object>();
    template.setConnectionFactory(jedisConnectionFactory());
    template.setKeySerializer(new StringRedisSerializer());
    template.setHashValueSerializer(new GenericToStringSerializer<Object>(Object.class));
    template.setValueSerializer(new GenericToStringSerializer<Object>(Object.class));

    return template;
  }

  @Bean
  public AmazonKinesisClient getAmazonKinesisClient() {
    AWSCredentials awsCredentials = new BasicAWSCredentials(
        System.getenv("awsAccessKey"),
        System.getenv("awsSecretKey")
    );

    AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(awsCredentials);
    ListStreamsResult streamResults = amazonKinesisClient.listStreams();
    boolean foundStream = false;

    for (String streamName : streamResults.getStreamNames()) {
      if (streamName.equals(System.getenv("kinesisStream"))) {
        foundStream = true;
        break;
      }
    }

    if (!foundStream) {
      amazonKinesisClient.createStream(System.getenv("kinesisStream"), 2);
    }

    logger.info("Configured Kinesis Client");
    return amazonKinesisClient;
  }

}
