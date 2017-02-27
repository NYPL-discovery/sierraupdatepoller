package org.nypl.harvester.sierra.model.streamdatamodel;

import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.model.StreamDataModel;

public class SierraBibUpdate extends StreamDataModel {
  private String id;

  @Override
  public boolean translateToStreamData(Object data) {
    Item item = (Item) data;

    setId(item.getId());

    return false;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
