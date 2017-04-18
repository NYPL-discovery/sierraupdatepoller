package org.nypl.harvester.sierra.model.streamdatamodel;

import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.model.StreamDataModel;

public class SierraResourceRetrievalRequest extends StreamDataModel {
  private String id;

  @Override
  public boolean translateToStreamData(Resource resource) {
    setId(resource.getId());

    return true;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
