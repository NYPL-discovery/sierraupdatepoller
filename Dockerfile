FROM openjdk:8-jdk-alpine
VOLUME /tmp
COPY target/sierraupdatepoller-0.0.1.jar /usr/src/myapp/sierraupdatepoller-0.0.1.jar
WORKDIR /usr/src/myapp

ENV clientId clientId
ENV AWS_ACCESS_KEY_ID AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY AWS_SECRET_ACCESS_KEY
ENV accessTokenUri accessTokenUri
ENV accessTokenUriProd accessTokenUriProd
ENV clientSecret clientSecret
ENV grantType client_credentials
ENV kinesisResourceRetrievalRequestStream SierraBibRetrievalRequest
ENV kinesisResourceUpdateStream SierraBibUpdate
ENV pollDelay 30s
ENV redisHost localhost
ENV redisPort 6379
ENV resourceType bibs
ENV sierraApi sierraApi
ENV sierraApiProd sierraApiProd

CMD ["java", "-jar", "sierraupdatepoller-0.0.1.jar"]
