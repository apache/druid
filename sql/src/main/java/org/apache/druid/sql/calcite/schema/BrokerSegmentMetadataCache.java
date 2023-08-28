package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.metadata.DatasourceSchema;
import org.apache.druid.segment.metadata.SegmentMetadataCache;
import org.apache.druid.segment.metadata.SegmentMetadataCacheConfig;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.table.DatasourceTable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
@ManageLifecycle
public class BrokerSegmentMetadataCache extends SegmentMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(SegmentMetadataCache.class);
  private final PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder;

  private final ConcurrentMap<String, DatasourceTable.PhysicalDatasourceMetadata> tables = new ConcurrentHashMap<>();

  @Inject
  public BrokerSegmentMetadataCache(
      QueryLifecycleFactory queryLifecycleFactory,
      TimelineServerView serverView,
      SegmentMetadataCacheConfig config,
      Escalator escalator,
      InternalQueryConfig internalQueryConfig,
      ServiceEmitter emitter,
      PhysicalDatasourceMetadataBuilder physicalDatasourceMetadataBuilder
  )
  {
    super(
        queryLifecycleFactory,
        serverView,
        config,
        escalator,
        internalQueryConfig,
        emitter
    );
    this.physicalDatasourceMetadataBuilder = physicalDatasourceMetadataBuilder;
  }

  @Override
  public void rebuildDatasource(String dataSource)
  {
    final DatasourceSchema druidTable = buildDruidTable(dataSource);
    if (druidTable == null) {
      log.info("dataSource [%s] no longer exists, all metadata removed.", dataSource);
      tables.remove(dataSource);
      return;
    }
    final DatasourceTable.PhysicalDatasourceMetadata physicalDatasourceMetadata =
        physicalDatasourceMetadataBuilder.build(dataSource, druidTable.getRowSignature());
    final DatasourceTable.PhysicalDatasourceMetadata oldTable = tables.put(dataSource, physicalDatasourceMetadata);
    if (oldTable == null || !oldTable.rowSignature().equals(physicalDatasourceMetadata.rowSignature())) {
      log.info("[%s] has new signature: %s.", dataSource, druidTable.getRowSignature());
    } else {
      log.info("[%s] signature is unchanged.", dataSource);
    }
  }

  @Override
  public Set<String> getDatasourceNames()
  {
    return tables.keySet();
  }

  @Override
  public void removeFromTable(String s)
  {
    tables.remove(s);
  }

  @Override
  public boolean tablesContains(String s)
  {
    return tables.containsKey(s);
  }

  public DatasourceTable.PhysicalDatasourceMetadata getPhysicalDatasourceMetadata(String name)
  {
    return tables.get(name);
  }
}
