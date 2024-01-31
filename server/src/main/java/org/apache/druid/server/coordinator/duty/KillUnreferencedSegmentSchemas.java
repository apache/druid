package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.segment.metadata.SchemaManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.skife.jdbi.v2.Update;
import javax.annotation.Nullable;

/**
 *
 */
public class KillUnreferencedSegmentSchemas implements CoordinatorDuty
{
  private final SchemaManager schemaManager;

  public KillUnreferencedSegmentSchemas(SchemaManager schemaManager)
  {
    this.schemaManager = schemaManager;
  }

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    schemaManager.cleanUpUnreferencedSchema();
    // todo should retrigger schema poll from db?
    return params;
  }
}
