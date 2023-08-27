package org.apache.druid.sql.calcite.schema;

import com.google.inject.Inject;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.sql.calcite.table.DatasourceTable;

public class PhysicalDatasourceMetadataBuilder
{
  private final JoinableFactory joinableFactory;
  private final SegmentManager segmentManager;

  @Inject
  public PhysicalDatasourceMetadataBuilder(JoinableFactory joinableFactory, SegmentManager segmentManager)
  {
    this.joinableFactory = joinableFactory;
    this.segmentManager = segmentManager;
  }

  DatasourceTable.PhysicalDatasourceMetadata build(String dataSource, RowSignature rowSignature)
  {
    final TableDataSource tableDataSource;

    // to be a GlobalTableDataSource instead of a TableDataSource, it must appear on all servers (inferred by existing
    // in the segment cache, which in this case belongs to the broker meaning only broadcast segments live here)
    // to be joinable, it must be possibly joinable according to the factory. we only consider broadcast datasources
    // at this time, and isGlobal is currently strongly coupled with joinable, so only make a global table datasource
    // if also joinable
    final GlobalTableDataSource maybeGlobal = new GlobalTableDataSource(dataSource);
    final boolean isJoinable = joinableFactory.isDirectlyJoinable(maybeGlobal);
    final boolean isBroadcast = segmentManager.getDataSourceNames().contains(dataSource);
    if (isBroadcast && isJoinable) {
      tableDataSource = maybeGlobal;
    } else {
      tableDataSource = new TableDataSource(dataSource);
    }
    return new DatasourceTable.PhysicalDatasourceMetadata(tableDataSource, rowSignature, isJoinable, isBroadcast);
  }
}
