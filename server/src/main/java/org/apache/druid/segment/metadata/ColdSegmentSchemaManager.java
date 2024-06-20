package org.apache.druid.segment.metadata;

import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordinator.DruidCoordinator;

import java.util.Collection;
import java.util.Map;

public class ColdSegmentSchemaManager
{
  private SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private DruidCoordinator druidCoordinator;
  private SegmentSchemaCache segmentSchemaCache;

  public ColdSegmentSchemaManager(
      SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      DruidCoordinator druidCoordinator,
      SegmentSchemaCache segmentSchemaCache
  )
  {
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.druidCoordinator = druidCoordinator;
    this.segmentSchemaCache = segmentSchemaCache;
  }

  public Map<String, RowSignature> buildSignatures()
  {
    Collection<ImmutableDruidDataSource> dataSources = sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments();

  }
}
