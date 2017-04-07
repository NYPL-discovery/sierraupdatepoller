package org.nypl.harvester.sierra.api.utils;

import org.nypl.harvester.sierra.exception.SierraHarvesterException;
import org.nypl.harvester.sierra.model.StreamDataModel;
import org.nypl.harvester.sierra.utils.HarvesterConstants;
import org.springframework.stereotype.Component;

@Component
public class StreamDataTranslator {
  public static StreamDataModel translate(StreamDataModel streamData, Object object)
      throws SierraHarvesterException {
    try {
      StreamDataModel newStreamData =
          (StreamDataModel) Class.forName(streamData.getClass().getName()).newInstance();

      newStreamData.translateToStreamData(object);

      return newStreamData;
    } catch (Exception exception) {
      throw new SierraHarvesterException(HarvesterConstants.getResource()
          + " : Unable to translate object to stream data: " + exception.getMessage());
    }
  }
}
