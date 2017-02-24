package org.nypl.harvester.sierra.api.utils;

import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.springframework.stereotype.Component;

@Component
public class AvroSerializer {
  public static Schema getSchema(Object object) {
    return ReflectData.get().getSchema(object.getClass());
  }

  public static byte[] encode(Schema schema, Object object) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DatumWriter<Object> userDatumWriter = new ReflectDatumWriter<>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

    userDatumWriter.write(object, encoder);

    encoder.flush();

    return outputStream.toByteArray();
  }
}
