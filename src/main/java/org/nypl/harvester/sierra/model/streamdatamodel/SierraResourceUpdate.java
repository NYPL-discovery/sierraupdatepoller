package org.nypl.harvester.sierra.model.streamdatamodel;

import org.nypl.harvester.sierra.model.Resource;
import org.nypl.harvester.sierra.model.StreamDataModel;

public class SierraResourceUpdate extends StreamDataModel {
  private String id;

  @Override
  public boolean translateToStreamData(Object data) {
    Resource resource = (Resource) data;

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
