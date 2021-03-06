package org.nypl.harvester.sierra.api.utils;

import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.springframework.stereotype.Component;

@Component
public class StreamDataTranslator {
  public static StreamDataModel translate(StreamDataModel streamData, Resource resource,
      String resourceType) throws SierraHarvesterException {
    try {
      StreamDataModel newStreamData =
          (StreamDataModel) Class.forName(streamData.getClass().getName()).newInstance();

      newStreamData.translateToStreamData(resource);

      return newStreamData;
    } catch (Exception exception) {
      throw new SierraHarvesterException(
          "Unable to translate object to stream data: " + exception.getMessage(), resourceType);
    }
  }
}
