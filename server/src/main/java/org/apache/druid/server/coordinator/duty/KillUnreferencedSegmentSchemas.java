package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.Update;
import javax.annotation.Nullable;

/**
 *
 */
public class KillUnreferencedSegmentSchemas implements CoordinatorDuty
{
  private final SQLMetadataConnector connector;

  public KillUnreferencedSegmentSchemas(SQLMetadataConnector connector)
  {
    this.connector = connector;
  }

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    connector.retryTransaction((TransactionCallback<Integer>) (handle, transactionStatus) -> {
      Update deleteStatement = handle.createStatement(
          StringUtils.format("DELETE FROM %s WHERE schema_id NOT IN (SELECT schema_id FROM %s)"));
      return deleteStatement.execute();
      }, 1, 3
    );
    return params;
  }
}
