package org.apache.druid.catalog.sync;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.catalog.model.TableMetadata;

public class UpdateEvent
{
  public enum EventType
  {
    CREATE,
    UPDATE,
    PROPERTY_UPDATE,
    COLUMNS_UPDATE,
    DELETE
  }

  @JsonProperty("type")
  public final EventType type;
  @JsonProperty("table")
  public final TableMetadata table;

  @JsonCreator
  public UpdateEvent(
      @JsonProperty("type") final EventType type,
      @JsonProperty("table") final TableMetadata table
  )
  {
    this.type = type;
    this.table = table;
  }
}
