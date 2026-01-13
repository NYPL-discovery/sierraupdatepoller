FROM openjdk:8
VOLUME /tmp

# Install maven
RUN apt-get update
RUN apt-get install -y maven


#COPY target/sierraupdatepoller-0.0.1.jar /usr/src/myapp/sierraupdatepoller-0.0.1.jar

ADD pom.xml /usr/myapp/pom.xml
ADD src usr/myapp/src

WORKDIR /usr/myapp

RUN ["mvn", "clean", "package"]


ENV clientId 4H1Q48F1vkjf8DHNmgAejojVJ2yP
ENV AWS_ACCESS_KEY_ID VALUE
ENV AWS_SECRET_ACCESS_KEY VALUE
ENV accessTokenUri VALUE
ENV accessTokenUriProd VALUE
ENV clientSecret VALUE-service
ENV grantType VALUE
ENV kinesisResourceRetrievalRequestStream VALUE
ENV kinesisResourceUpdateStream VALUE
ENV pollDelay 30s
ENV redisHost cache
ENV redisPort 6379
ENV resourceType bibs
ENV sierraApi VALUE
ENV sierraApiProd VALUE

WORKDIR /usr/myapp/target

#CMD ["java", "-jar", "sierraupdatepoller-0.0.1.jar"]
