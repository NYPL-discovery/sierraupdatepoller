package org.nypl.harvester.sierra.model.streamdatamodel;

import org.nypl.harvester.sierra.model.Item;
import org.nypl.harvester.sierra.model.StreamDataModel;

public class SierraItemRetrievalRequest extends StreamDataModel {
  private String id;

  private String priority;

  @Override
  public boolean translateToStreamData(Object data) {
    Item item = (Item) data;

    setId(item.getId());
    setPriority("1");

    return false;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getPriority() {
    return priority;
  }

  public void setPriority(String priority) {
    this.priority = priority;
  }
}
