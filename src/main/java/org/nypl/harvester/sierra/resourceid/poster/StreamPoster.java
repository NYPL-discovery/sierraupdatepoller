package org.nypl.harvester.sierra.resourceid.poster;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.camel.ProducerTemplate;
import org.nypl.harvester.sierra.api.utils.AvroSerializer;
import org.nypl.harvester.sierra.api.utils.StreamDataTranslator;
import org.nypl.harvester.sierra.config.BaseConfig;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.collect.Lists;

public class StreamPoster implements ResourcePoster {

  private static Logger logger = LoggerFactory.getLogger(StreamPoster.class);

  private String streamName;

  private StreamDataModel streamDataModel;

  private BaseConfig baseConfig;

  public StreamPoster(String streamName, StreamDataModel streamData, BaseConfig baseConfig) {
    this.streamName = streamName;
    this.streamDataModel = streamData;
    this.baseConfig = baseConfig;
  }

  @Override
  public Integer postResources(ProducerTemplate template, List<Resource> resources,
      String resourceType) throws SierraHarvesterException {
    try {
      Schema schema = AvroSerializer.getSchema(this.getStreamDataModel());

      List<byte[]> avroRecords = new ArrayList<>();

      for (Resource resource : resources) {
        byte[] avroRecord = AvroSerializer.encode(schema,
            StreamDataTranslator.translate(getStreamDataModel(), resource, resourceType),
            resourceType);
        avroRecords.add(avroRecord);
      }

      Integer numOfResourcesSent = processRecordsForStream(avroRecords, resourceType);

      logger.info(resourceType + " : Sent " + resources.size() + " resources to Kinesis stream: "
          + getStreamName());
      return numOfResourcesSent;
    } catch (Exception e) {
      logger.error(resourceType + " : Error occurred while sending resources to kinesis - ", e);
      throw new SierraHarvesterException(
          "Error occurred while sending resources to kinesis - " + e.getMessage(), resourceType);
    }
  }

  public Integer processRecordsForStream(List<byte[]> avroRecords, String resourceType)
      throws SierraHarvesterException {
    try {
      List<List<byte[]>> listOfSplitRecords =
          Lists.partition(avroRecords, HarvesterConstants.KINESIS_PUT_RECORDS_MAX_SIZE);
      for (List<byte[]> splitAvroRecords : listOfSplitRecords) {
        sendToKinesis(splitAvroRecords, resourceType);
      }
      return listOfSplitRecords.size();
    } catch (Exception e) {
      logger.error("Error occurred while sending items to kinesis - ", e);
      throw new SierraHarvesterException(
          "Error occurred while sending records to kinesis - " + e.getMessage(), resourceType);
    }
  }

  public boolean sendToKinesis(List<byte[]> avroRecords, String resourceType)
      throws SierraHarvesterException {
    try {
      PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
      putRecordsRequest.setStreamName(streamName);
      List<PutRecordsRequestEntry> listPutRecordsRequestEntry = new ArrayList<>();
      for (byte[] avroItem : avroRecords) {
        PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
        putRecordsRequestEntry.setData(ByteBuffer.wrap(avroItem));
        putRecordsRequestEntry.setPartitionKey(Long.toString(System.currentTimeMillis()));
        listPutRecordsRequestEntry.add(putRecordsRequestEntry);
      }
      putRecordsRequest.setRecords(listPutRecordsRequestEntry);
      PutRecordsResult putRecordsResult =
          baseConfig.getAmazonKinesisClient().putRecords(putRecordsRequest);
      while (putRecordsResult.getFailedRecordCount() > 0) {
        final List<PutRecordsRequestEntry> failedRecordsList = new ArrayList<>();
        final List<PutRecordsResultEntry> listPutRecordsResultEntry = putRecordsResult.getRecords();
        for (int i = 0; i < listPutRecordsResultEntry.size(); i++) {
          final PutRecordsRequestEntry putRecordsRequestEntry = listPutRecordsRequestEntry.get(i);
          final PutRecordsResultEntry putRecordsResultEntry = listPutRecordsResultEntry.get(i);
          if (putRecordsResultEntry.getErrorCode() != null) {
            failedRecordsList.add(putRecordsRequestEntry);
          }
          listPutRecordsRequestEntry = failedRecordsList;
          putRecordsRequest.setRecords(listPutRecordsRequestEntry);
          putRecordsResult = baseConfig.getAmazonKinesisClient().putRecords(putRecordsRequest);
        }
      }
      return true;
    } catch (Exception e) {
      logger.error("Error occurred while sending items to kinesis - ", e);
      throw new SierraHarvesterException(
          "Error occurred while sending items to kinesis - " + e.getMessage(), resourceType);
    }
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public StreamDataModel getStreamDataModel() {
    return streamDataModel;
  }

  public void setStreamDataModel(StreamDataModel streamDataModel) {
    this.streamDataModel = streamDataModel;
  }

}
