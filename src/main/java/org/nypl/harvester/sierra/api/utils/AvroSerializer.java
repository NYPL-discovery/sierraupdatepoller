package org.nypl.harvester.sierra.api.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class AvroSerializer {

  private static Logger logger = LoggerFactory.getLogger(AvroSerializer.class);

  public static Schema getSchema(Object object) {
    return ReflectData.get().getSchema(object.getClass());
  }

  public static byte[] encode(Schema schema, StreamDataModel streamDataObject, String resourceType)
      throws SierraHarvesterException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DatumWriter<Object> userDatumWriter = new ReflectDatumWriter<>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

    try {
      userDatumWriter.write(streamDataObject, encoder);
    } catch (IOException e) {
      logger.error(resourceType + ": Error occurred while encoding in avro - ", e);
      throw new SierraHarvesterException("Unable to encode object as Avro: " + e.getMessage(),
          resourceType);
    }

    try {
      encoder.flush();
    } catch (IOException e) {
      throw new SierraHarvesterException("Unable to flush Avro object: " + e.getMessage(),
          resourceType);
    }

    return outputStream.toByteArray();
  }
}
