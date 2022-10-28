package org.apache.druid.catalog.storage.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;

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
  @JsonProperty("id")
  public final TableId id;
  @JsonProperty("id")
  public final long updateTime;
  @JsonProperty("spec")
  public final TableSpec spec;

  @JsonCreator
  public UpdateEvent(
      @JsonProperty("type") final EventType type,
      @JsonProperty("id") final TableId id,
      @JsonProperty("updateTime") final long updateTime,
      @JsonProperty("spec") final TableSpec spec
  )
  {
    this.type = type;
    this.id = id;
    this.updateTime = updateTime;
    this.spec = spec;
  }

  public UpdateEvent(
      final EventType type,
      final TableMetadata table
  )
  {
    this(type, table.id(), table.updateTime(), table.spec());
  }

  public UpdateEvent(
      final EventType type,
      final TableId id
  )
  {
    this(type, id, 0, null);
  }
}
