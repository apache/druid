/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.metadata.metadata.AllColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.metadata.AbstractSegmentMetadataCache;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.segment.metadata.DataSourceInformation;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BrokerSegmentMetadataCacheTest extends BrokerSegmentMetadataCacheCommon
{
  private static final BrokerSegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = BrokerSegmentMetadataCacheConfig.create("PT1S");
  // Timeout to allow (rapid) debugging, while not blocking tests with errors.
  private static final int WAIT_TIMEOUT_SECS = 6;
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private BrokerSegmentMetadataCache runningSchema;
  private CountDownLatch buildTableLatch = new CountDownLatch(1);
  private CountDownLatch markDataSourceLatch = new CountDownLatch(1);
  private CountDownLatch refreshLatch = new CountDownLatch(1);

  @Before
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception
  {
    super.tearDown();
    if (runningSchema != null) {
      runningSchema.stop();
    }
    walker.close();
  }

  public BrokerSegmentMetadataCache buildSchemaMarkAndTableLatch() throws InterruptedException
  {
    return buildSchemaMarkAndTableLatch(SEGMENT_CACHE_CONFIG_DEFAULT, new NoopCoordinatorClient());
  }

  public BrokerSegmentMetadataCache buildSchemaMarkAndTableLatch(BrokerSegmentMetadataCacheConfig config, CoordinatorClient coordinatorClient) throws InterruptedException
  {
    Preconditions.checkState(runningSchema == null);
    runningSchema = new BrokerSegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        config,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        coordinatorClient
    )
    {
      @Override
      public RowSignature buildDataSourceRowSignature(String dataSource)
      {
        RowSignature table = super.buildDataSourceRowSignature(dataSource);
        buildTableLatch.countDown();
        return table;
      }

      @Override
      public void markDataSourceAsNeedRebuild(String datasource)
      {
        super.markDataSourceAsNeedRebuild(datasource);
        markDataSourceLatch.countDown();
      }
    };

    runningSchema.start();
    runningSchema.awaitInitialization();
    return runningSchema;
  }

  public BrokerSegmentMetadataCache buildSchemaMarkAndRefreshLatch() throws InterruptedException
  {
    Preconditions.checkState(runningSchema == null);
    runningSchema = new BrokerSegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        new NoopCoordinatorClient()
    )
    {
      @Override
      public void markDataSourceAsNeedRebuild(String datasource)
      {
        super.markDataSourceAsNeedRebuild(datasource);
        markDataSourceLatch.countDown();
      }

      @Override
      @VisibleForTesting
      public void refresh(
          final Set<SegmentId> segmentsToRefresh,
          final Set<String> dataSourcesToRebuild) throws IOException
      {
        super.refresh(segmentsToRefresh, dataSourcesToRebuild);
        refreshLatch.countDown();
      }
    };

    runningSchema.start();
    runningSchema.awaitInitialization();
    return runningSchema;
  }

  /**
   * Test the case when coordinator returns information for all the requested datasources.
   */
  @Test
  public void testCoordinatorReturnsAllDSSchema() throws InterruptedException
  {
    final RowSignature dataSource1RowSignature = new QueryableIndexStorageAdapter(index1).getRowSignature();
    final RowSignature dataSource2RowSignature = new QueryableIndexStorageAdapter(index2).getRowSignature();
    final RowSignature someDataSourceRowSignature = new QueryableIndexStorageAdapter(indexAuto1).getRowSignature();
    final RowSignature foo3RowSignature = new QueryableIndexStorageAdapter(indexAuto2).getRowSignature();

    NoopCoordinatorClient coordinatorClient = new NoopCoordinatorClient() {
      @Override
      public ListenableFuture<List<DataSourceInformation>> fetchDataSourceInformation(Set<String> datasources)
      {
        Map<String, DataSourceInformation> dataSourceInformationMap = new HashMap<>();
        dataSourceInformationMap.put(DATASOURCE1, new DataSourceInformation(DATASOURCE1, dataSource1RowSignature));
        dataSourceInformationMap.put(DATASOURCE2, new DataSourceInformation(DATASOURCE2, dataSource2RowSignature));
        dataSourceInformationMap.put(SOME_DATASOURCE, new DataSourceInformation(SOME_DATASOURCE, someDataSourceRowSignature));
        dataSourceInformationMap.put("foo3", new DataSourceInformation("foo3", foo3RowSignature));

        return Futures.immediateFuture(new ArrayList<>(dataSourceInformationMap.values()));
      }
    };

    // don't expect any segment metadata queries
    QueryLifecycleFactory factoryMock = EasyMock.createMock(QueryLifecycleFactory.class);

    BrokerSegmentMetadataCache schema = new BrokerSegmentMetadataCache(
        factoryMock,
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        coordinatorClient
    );

    schema.start();
    schema.awaitInitialization();
    final Set<String> tableNames = schema.getDatasourceNames();
    Assert.assertEquals(ImmutableSet.of(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE2, CalciteTests.SOME_DATASOURCE, "foo3"), tableNames);

    Assert.assertEquals(dataSource1RowSignature, schema.getDatasource(DATASOURCE1).getRowSignature());
    Assert.assertEquals(dataSource2RowSignature, schema.getDatasource(DATASOURCE2).getRowSignature());
    Assert.assertEquals(someDataSourceRowSignature, schema.getDatasource(SOME_DATASOURCE).getRowSignature());
    Assert.assertEquals(foo3RowSignature, schema.getDatasource("foo3").getRowSignature());
  }

  /**
   * Test the case when Coordinator returns information for a subset of datasources.
   * Check if SegmentMetadataQuery is fired for segments of the remaining datasources.
   */
  @Test
  public void testCoordinatorReturnsFewDSSchema() throws InterruptedException
  {
    final RowSignature dataSource1RowSignature = new QueryableIndexStorageAdapter(index1).getRowSignature();
    final RowSignature dataSource2RowSignature = new QueryableIndexStorageAdapter(index2).getRowSignature();
    final RowSignature someDataSourceRowSignature = new QueryableIndexStorageAdapter(indexAuto1).getRowSignature();

    NoopCoordinatorClient coordinatorClient = new NoopCoordinatorClient() {
      @Override
      public ListenableFuture<List<DataSourceInformation>> fetchDataSourceInformation(Set<String> datasources)
      {
        Map<String, DataSourceInformation> dataSourceInformationMap = new HashMap<>();
        dataSourceInformationMap.put(DATASOURCE1, new DataSourceInformation(DATASOURCE1, dataSource1RowSignature));
        dataSourceInformationMap.put(DATASOURCE2, new DataSourceInformation(DATASOURCE2, dataSource2RowSignature));
        dataSourceInformationMap.put(SOME_DATASOURCE, new DataSourceInformation(SOME_DATASOURCE, someDataSourceRowSignature));
        return Futures.immediateFuture(new ArrayList<>(dataSourceInformationMap.values()));
      }
    };

    SegmentMetadataQuery expectedMetadataQuery = new SegmentMetadataQuery(
        new TableDataSource("foo3"),
        new MultipleSpecificSegmentSpec(Collections.singletonList(realtimeSegment1.getId().toDescriptor())),
        new AllColumnIncluderator(),
        false,
        ImmutableMap.of(QueryContexts.BROKER_PARALLEL_MERGE_KEY, false),
        EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        null,
        null
    );

    QueryLifecycleFactory factoryMock = EasyMock.createMock(QueryLifecycleFactory.class);
    QueryLifecycle lifecycleMock = EasyMock.createMock(QueryLifecycle.class);
    EasyMock.expect(factoryMock.factorize()).andReturn(lifecycleMock).once();
    EasyMock.expect(lifecycleMock.runSimple(expectedMetadataQuery, AllowAllAuthenticator.ALLOW_ALL_RESULT, Access.OK))
            .andReturn(QueryResponse.withEmptyContext(Sequences.empty()));

    BrokerSegmentMetadataCache schema = new BrokerSegmentMetadataCache(
        factoryMock,
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        coordinatorClient
    );

    EasyMock.replay(factoryMock, lifecycleMock);

    schema.start();
    schema.awaitInitialization();

    EasyMock.verify(factoryMock, lifecycleMock);
  }

  /**
   * Verify that broker polls schema for all datasources in every cycle.
   */
  @Test
  public void testBrokerPollsAllDSSchema() throws InterruptedException
  {
    ArgumentCaptor<Set<String>> argumentCaptor = ArgumentCaptor.forClass(Set.class);
    CoordinatorClient coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.fetchDataSourceInformation(argumentCaptor.capture())).thenReturn(Futures.immediateFuture(null));
    BrokerSegmentMetadataCache schema = new BrokerSegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        coordinatorClient
    );

    schema.start();
    schema.awaitInitialization();

    Assert.assertEquals(Sets.newHashSet(DATASOURCE1, DATASOURCE2, DATASOURCE3, SOME_DATASOURCE), argumentCaptor.getValue());

    refreshLatch = new CountDownLatch(1);
    serverView.addSegment(newSegment("xyz", 0), ServerType.HISTORICAL);

    refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS);

    // verify that previously refreshed are included in the last coordinator poll
    Assert.assertEquals(Sets.newHashSet(DATASOURCE1, DATASOURCE2, DATASOURCE3, SOME_DATASOURCE, "xyz"), argumentCaptor.getValue());
  }

  @Test
  public void testGetTableMap() throws InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    Assert.assertEquals(ImmutableSet.of(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE2, CalciteTests.SOME_DATASOURCE), schema.getDatasourceNames());
  }

  @Test
  public void testGetTableMapFoo() throws InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    final DatasourceTable.PhysicalDatasourceMetadata fooDs = schema.getDatasource("foo");
    final DruidTable fooTable = new DatasourceTable(fooDs);
    final RelDataType rowType = fooTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(6, fields.size());

    Assert.assertEquals("__time", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.TIMESTAMP, fields.get(0).getType().getSqlTypeName());

    Assert.assertEquals("dim2", fields.get(1).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(1).getType().getSqlTypeName());

    Assert.assertEquals("m1", fields.get(2).getName());
    Assert.assertEquals(SqlTypeName.DOUBLE, fields.get(2).getType().getSqlTypeName());

    Assert.assertEquals("dim1", fields.get(3).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(3).getType().getSqlTypeName());

    Assert.assertEquals("cnt", fields.get(4).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(4).getType().getSqlTypeName());

    Assert.assertEquals("unique_dim1", fields.get(5).getName());
    Assert.assertEquals(SqlTypeName.OTHER, fields.get(5).getType().getSqlTypeName());
  }

  @Test
  public void testGetTableMapFoo2() throws InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    final DatasourceTable.PhysicalDatasourceMetadata fooDs = schema.getDatasource("foo2");
    final DruidTable fooTable = new DatasourceTable(fooDs);
    final RelDataType rowType = fooTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(3, fields.size());

    Assert.assertEquals("__time", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.TIMESTAMP, fields.get(0).getType().getSqlTypeName());

    Assert.assertEquals("dim2", fields.get(1).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(1).getType().getSqlTypeName());

    Assert.assertEquals("m1", fields.get(2).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(2).getType().getSqlTypeName());
  }

  @Test
  public void testGetTableMapSomeTable() throws InterruptedException
  {
    // using 'newest first' column type merge strategy, the types are expected to be the types defined in the newer
    // segment, except for json, which is special handled
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch(
        new BrokerSegmentMetadataCacheConfig() {
          @Override
          public AbstractSegmentMetadataCache.ColumnTypeMergePolicy getMetadataColumnTypeMergePolicy()
          {
            return new AbstractSegmentMetadataCache.FirstTypeMergePolicy();
          }
        },
        new NoopCoordinatorClient()
    );
    final DatasourceTable.PhysicalDatasourceMetadata fooDs = schema.getDatasource(CalciteTests.SOME_DATASOURCE);
    final DruidTable table = new DatasourceTable(fooDs);
    final RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(9, fields.size());

    Assert.assertEquals("__time", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.TIMESTAMP, fields.get(0).getType().getSqlTypeName());

    Assert.assertEquals("numbery", fields.get(1).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(1).getType().getSqlTypeName());

    Assert.assertEquals("numberyArrays", fields.get(2).getName());
    Assert.assertEquals(SqlTypeName.ARRAY, fields.get(2).getType().getSqlTypeName());
    Assert.assertEquals(SqlTypeName.DOUBLE, fields.get(2).getType().getComponentType().getSqlTypeName());

    Assert.assertEquals("stringy", fields.get(3).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(3).getType().getSqlTypeName());

    Assert.assertEquals("array", fields.get(4).getName());
    Assert.assertEquals(SqlTypeName.ARRAY, fields.get(4).getType().getSqlTypeName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(4).getType().getComponentType().getSqlTypeName());

    Assert.assertEquals("nested", fields.get(5).getName());
    Assert.assertEquals(SqlTypeName.OTHER, fields.get(5).getType().getSqlTypeName());

    Assert.assertEquals("cnt", fields.get(6).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(6).getType().getSqlTypeName());

    Assert.assertEquals("m1", fields.get(7).getName());
    Assert.assertEquals(SqlTypeName.DOUBLE, fields.get(7).getType().getSqlTypeName());

    Assert.assertEquals("unique_dim1", fields.get(8).getName());
    Assert.assertEquals(SqlTypeName.OTHER, fields.get(8).getType().getSqlTypeName());
  }

  @Test
  public void testGetTableMapSomeTableLeastRestrictiveTypeMerge() throws InterruptedException
  {
    // using 'least restrictive' column type merge strategy, the types are expected to be the types defined as the
    // least restrictive blend across all segments
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    final DatasourceTable.PhysicalDatasourceMetadata fooDs = schema.getDatasource(CalciteTests.SOME_DATASOURCE);
    final DruidTable table = new DatasourceTable(fooDs);
    final RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(9, fields.size());

    Assert.assertEquals("__time", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.TIMESTAMP, fields.get(0).getType().getSqlTypeName());

    Assert.assertEquals("numbery", fields.get(1).getName());
    Assert.assertEquals(SqlTypeName.DOUBLE, fields.get(1).getType().getSqlTypeName());

    Assert.assertEquals("numberyArrays", fields.get(2).getName());
    Assert.assertEquals(SqlTypeName.ARRAY, fields.get(2).getType().getSqlTypeName());
    Assert.assertEquals(SqlTypeName.DOUBLE, fields.get(2).getType().getComponentType().getSqlTypeName());

    Assert.assertEquals("stringy", fields.get(3).getName());
    Assert.assertEquals(SqlTypeName.ARRAY, fields.get(3).getType().getSqlTypeName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(3).getType().getComponentType().getSqlTypeName());

    Assert.assertEquals("array", fields.get(4).getName());
    Assert.assertEquals(SqlTypeName.ARRAY, fields.get(4).getType().getSqlTypeName());
    Assert.assertEquals(SqlTypeName.DOUBLE, fields.get(4).getType().getComponentType().getSqlTypeName());

    Assert.assertEquals("nested", fields.get(5).getName());
    Assert.assertEquals(SqlTypeName.OTHER, fields.get(5).getType().getSqlTypeName());

    Assert.assertEquals("cnt", fields.get(6).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(6).getType().getSqlTypeName());

    Assert.assertEquals("m1", fields.get(7).getName());
    Assert.assertEquals(SqlTypeName.DOUBLE, fields.get(7).getType().getSqlTypeName());

    Assert.assertEquals("unique_dim1", fields.get(8).getName());
    Assert.assertEquals(SqlTypeName.OTHER, fields.get(8).getType().getSqlTypeName());
  }

  /**
   * This tests that {@link AvailableSegmentMetadata#getNumRows()} is correct in case
   * of multiple replicas i.e. when {@link AbstractSegmentMetadataCache#addSegment(DruidServerMetadata, DataSegment)}
   * is called more than once for same segment
   * @throws InterruptedException
   */
  @Test
  public void testAvailableSegmentMetadataNumRows() throws InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();

    Map<SegmentId, AvailableSegmentMetadata> segmentsMetadata = schema.getSegmentMetadataSnapshot();
    final List<DataSegment> segments = segmentsMetadata.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(6, segments.size());
    // find the only segment with datasource "foo2"
    final DataSegment existingSegment = segments.stream()
                                                .filter(segment -> segment.getDataSource().equals("foo2"))
                                                .findFirst()
                                                .orElse(null);
    Assert.assertNotNull(existingSegment);
    final AvailableSegmentMetadata existingMetadata = segmentsMetadata.get(existingSegment.getId());
    // update AvailableSegmentMetadata of existingSegment with numRows=5
    AvailableSegmentMetadata updatedMetadata = AvailableSegmentMetadata.from(existingMetadata).withNumRows(5).build();
    schema.setAvailableSegmentMetadata(existingSegment.getId(), updatedMetadata);
    // find a druidServer holding existingSegment
    final Pair<ImmutableDruidServer, DataSegment> pair = druidServers
        .stream()
        .flatMap(druidServer -> druidServer
            .iterateAllSegments()
            .stream()
            .filter(segment -> segment.getId().equals(existingSegment.getId()))
            .map(segment -> Pair.of(druidServer, segment))
        )
        .findAny()
        .orElse(null);
    Assert.assertNotNull(pair);
    final ImmutableDruidServer server = pair.lhs;
    Assert.assertNotNull(server);
    final DruidServerMetadata druidServerMetadata = server.getMetadata();
    // invoke SegmentMetadataCache#addSegment on existingSegment
    schema.addSegment(druidServerMetadata, existingSegment);
    segmentsMetadata = schema.getSegmentMetadataSnapshot();
    // get the only segment with datasource "foo2"
    final DataSegment currentSegment = segments.stream()
                                               .filter(segment -> segment.getDataSource().equals("foo2"))
                                               .findFirst()
                                               .orElse(null);
    final AvailableSegmentMetadata currentMetadata = segmentsMetadata.get(currentSegment.getId());
    Assert.assertEquals(updatedMetadata.getSegment().getId(), currentMetadata.getSegment().getId());
    Assert.assertEquals(updatedMetadata.getNumRows(), currentMetadata.getNumRows());
    // numreplicas do not change here since we addSegment with the same server which was serving existingSegment before
    Assert.assertEquals(updatedMetadata.getNumReplicas(), currentMetadata.getNumReplicas());
  }

  @Test
  public void testNullDatasource() throws IOException, InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadatas = schema.getSegmentMetadataSnapshot();
    final List<DataSegment> segments = segmentMetadatas.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(6, segments.size());
    // segments contains two segments with datasource "foo" and one with datasource "foo2"
    // let's remove the only segment with datasource "foo2"
    final DataSegment segmentToRemove = segments.stream()
                                                .filter(segment -> segment.getDataSource().equals("foo2"))
                                                .findFirst()
                                                .orElse(null);
    Assert.assertNotNull(segmentToRemove);
    schema.removeSegment(segmentToRemove);

    // The following line can cause NPE without segmentMetadata null check in
    // SegmentMetadataCache#refreshSegmentsForDataSource
    schema.refreshSegments(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()));
    Assert.assertEquals(5, schema.getSegmentMetadataSnapshot().size());
  }

  @Test
  public void testAllDatasourcesRebuiltOnDatasourceRemoval() throws IOException, InterruptedException
  {
    CountDownLatch addSegmentLatch = new CountDownLatch(7);
    BrokerSegmentMetadataCache schema = new BrokerSegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        new NoopCoordinatorClient()
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        addSegmentLatch.countDown();
      }

      @Override
      public void removeSegment(final DataSegment segment)
      {
        super.removeSegment(segment);
      }

      @Override
      public void markDataSourceAsNeedRebuild(String datasource)
      {
        super.markDataSourceAsNeedRebuild(datasource);
      }

      @Override
      @VisibleForTesting
      public void refresh(
          final Set<SegmentId> segmentsToRefresh,
          final Set<String> dataSourcesToRebuild) throws IOException
      {
        super.refresh(segmentsToRefresh, dataSourcesToRebuild);
      }
    };

    schema.start();
    schema.awaitInitialization();

    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadatas = schema.getSegmentMetadataSnapshot();
    List<DataSegment> segments = segmentMetadatas.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(6, segments.size());

    // verify that dim3 column isn't present in the schema for foo
    DatasourceTable.PhysicalDatasourceMetadata fooDs = schema.getDatasource("foo");
    Assert.assertTrue(fooDs.getRowSignature().getColumnNames().stream().noneMatch("dim3"::equals));

    // segments contains two segments with datasource "foo" and one with datasource "foo2"
    // let's remove the only segment with datasource "foo2"
    final DataSegment segmentToRemove = segments.stream()
                                                .filter(segment -> segment.getDataSource().equals("foo2"))
                                                .findFirst()
                                                .orElse(null);
    Assert.assertNotNull(segmentToRemove);
    schema.removeSegment(segmentToRemove);

    // we will add a segment to another datasource and
    // check if columns in this segment is reflected in the datasource schema
    DataSegment newSegment =
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(Intervals.of("2002/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();

    final File tmpDir = temporaryFolder.newFolder();

    List<InputRow> rows = ImmutableList.of(
        createRow(ImmutableMap.of("t", "2002-01-01", "m1", "1.0", "dim1", "", "dim3", "c1")),
        createRow(ImmutableMap.of("t", "2002-01-02", "m1", "2.0", "dim1", "10.1", "dim3", "c2")),
        createRow(ImmutableMap.of("t", "2002-01-03", "m1", "3.0", "dim1", "2", "dim3", "c3"))
    );

    QueryableIndex index = IndexBuilder.create()
                                        .tmpDir(new File(tmpDir, "1"))
                                        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                        .schema(
                                            new IncrementalIndexSchema.Builder()
                                                .withMetrics(
                                                    new CountAggregatorFactory("cnt"),
                                                    new DoubleSumAggregatorFactory("m1", "m1"),
                                                    new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                                )
                                                .withRollup(false)
                                                .build()
                                        )
                                        .rows(rows)
                                        .buildMMappedIndex();

    walker.add(newSegment, index);
    serverView.addSegment(newSegment, ServerType.HISTORICAL);

    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Set<String> dataSources = segments.stream().map(DataSegment::getDataSource).collect(Collectors.toSet());
    dataSources.remove("foo2");

    // LinkedHashSet to ensure that the datasource with no segments is encountered first
    Set<String> dataSourcesToRefresh = new LinkedHashSet<>();
    dataSourcesToRefresh.add("foo2");
    dataSourcesToRefresh.addAll(dataSources);

    segments = schema.getSegmentMetadataSnapshot().values()
                     .stream()
                     .map(AvailableSegmentMetadata::getSegment)
                     .collect(Collectors.toList());

    schema.refresh(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()), dataSourcesToRefresh);
    Assert.assertEquals(6, schema.getSegmentMetadataSnapshot().size());

    fooDs = schema.getDatasource("foo");

    // check if the new column present in the added segment is present in the datasource schema
    // ensuring that the schema is rebuilt
    Assert.assertTrue(fooDs.getRowSignature().getColumnNames().stream().anyMatch("dim3"::equals));
  }

  @Test
  public void testNullAvailableSegmentMetadata() throws IOException, InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadatas = schema.getSegmentMetadataSnapshot();
    final List<DataSegment> segments = segmentMetadatas.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(6, segments.size());
    // remove one of the segments with datasource "foo"
    final DataSegment segmentToRemove = segments.stream()
                                                .filter(segment -> segment.getDataSource().equals("foo"))
                                                .findFirst()
                                                .orElse(null);
    Assert.assertNotNull(segmentToRemove);
    schema.removeSegment(segmentToRemove);

    // The following line can cause NPE without segmentMetadata null check in
    // SegmentMetadataCache#refreshSegmentsForDataSource
    schema.refreshSegments(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()));
    Assert.assertEquals(5, schema.getSegmentMetadataSnapshot().size());
  }

  /**
   * Test actions on the cache. The current design of the cache makes testing far harder
   * than it should be.
   *
   * - The cache is refreshed on a schedule.
   * - Datasources are added to the refresh queue via an unsynchronized thread.
   * - The refresh loop always refreshes since one of the segments is dynamic.
   *
   * The use of latches tries to keep things synchronized, but there are many
   * moving parts. A simpler technique is sorely needed.
   */
  @Test
  public void testLocalSegmentCacheSetsDataSourceAsGlobalAndJoinable() throws InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndRefreshLatch();
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    DatasourceTable.PhysicalDatasourceMetadata fooTable = schema.getDatasource("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.dataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.dataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isJoinable());
    Assert.assertFalse(fooTable.isBroadcast());

    markDataSourceLatch = new CountDownLatch(1);
    refreshLatch = new CountDownLatch(1);
    final DataSegment someNewBrokerSegment = new DataSegment(
        "foo",
        Intervals.of("2012/2013"),
        "version1",
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        new NumberedShardSpec(2, 3),
        null,
        1,
        100L,
        DataSegment.PruneSpecsHolder.DEFAULT
    );
    segmentDataSourceNames.add("foo");
    joinableDataSourceNames.add("foo");
    serverView.addSegment(someNewBrokerSegment, ServerType.BROKER);
    Assert.assertTrue(markDataSourceLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for build twice
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    refreshLatch = new CountDownLatch(1);
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));

    fooTable = schema.getDatasource("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.dataSource() instanceof TableDataSource);
    Assert.assertTrue(fooTable.dataSource() instanceof GlobalTableDataSource);
    Assert.assertTrue(fooTable.isJoinable());
    Assert.assertTrue(fooTable.isBroadcast());

    // now remove it
    markDataSourceLatch = new CountDownLatch(1);
    refreshLatch = new CountDownLatch(1);
    joinableDataSourceNames.remove("foo");
    segmentDataSourceNames.remove("foo");
    serverView.removeSegment(someNewBrokerSegment, ServerType.BROKER);

    Assert.assertTrue(markDataSourceLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for build twice
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    refreshLatch = new CountDownLatch(1);
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));

    fooTable = schema.getDatasource("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.dataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.dataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isJoinable());
    Assert.assertFalse(fooTable.isBroadcast());
  }

  @Test
  public void testLocalSegmentCacheSetsDataSourceAsBroadcastButNotJoinable() throws InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndRefreshLatch();
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    DatasourceTable.PhysicalDatasourceMetadata fooTable = schema.getDatasource("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.dataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.dataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isJoinable());
    Assert.assertFalse(fooTable.isBroadcast());

    markDataSourceLatch = new CountDownLatch(1);
    refreshLatch = new CountDownLatch(1);
    final DataSegment someNewBrokerSegment = new DataSegment(
        "foo",
        Intervals.of("2012/2013"),
        "version1",
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        new NumberedShardSpec(2, 3),
        null,
        1,
        100L,
        DataSegment.PruneSpecsHolder.DEFAULT
    );
    segmentDataSourceNames.add("foo");
    serverView.addSegment(someNewBrokerSegment, ServerType.BROKER);

    Assert.assertTrue(markDataSourceLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for build twice
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    refreshLatch = new CountDownLatch(1);
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));

    fooTable = schema.getDatasource("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.dataSource() instanceof TableDataSource);
    // Should not be a GlobalTableDataSource for now, because isGlobal is couple with joinability. Ideally this will be
    // changed in the future and we should expect.
    Assert.assertFalse(fooTable.dataSource() instanceof GlobalTableDataSource);
    Assert.assertTrue(fooTable.isBroadcast());
    Assert.assertFalse(fooTable.isJoinable());

    // now remove it
    markDataSourceLatch = new CountDownLatch(1);
    refreshLatch = new CountDownLatch(1);
    segmentDataSourceNames.remove("foo");
    serverView.removeSegment(someNewBrokerSegment, ServerType.BROKER);

    Assert.assertTrue(markDataSourceLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for build twice
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    refreshLatch = new CountDownLatch(1);
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));

    fooTable = schema.getDatasource("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.dataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.dataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isBroadcast());
    Assert.assertFalse(fooTable.isJoinable());
  }

  /**
   * Ensure that the BrokerInternalQueryConfig context is honored for this internally generated SegmentMetadata Query
   */
  @Test
  public void testRunSegmentMetadataQueryWithContext() throws Exception
  {
    String brokerInternalQueryConfigJson = "{\"context\": { \"priority\": 5} }";

    TestHelper.makeJsonMapper();
    InternalQueryConfig internalQueryConfig = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(brokerInternalQueryConfigJson, InternalQueryConfig.class)
        ),
        InternalQueryConfig.class
    );

    QueryLifecycleFactory factoryMock = EasyMock.createMock(QueryLifecycleFactory.class);
    QueryLifecycle lifecycleMock = EasyMock.createMock(QueryLifecycle.class);

    // Need to create schema for this test because the available schemas don't mock the QueryLifecycleFactory, which I need for this test.
    BrokerSegmentMetadataCache schema = new BrokerSegmentMetadataCache(
        factoryMock,
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        internalQueryConfig,
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        new NoopCoordinatorClient()
    );

    Map<String, Object> queryContext = ImmutableMap.of(
        QueryContexts.PRIORITY_KEY, 5,
        QueryContexts.BROKER_PARALLEL_MERGE_KEY, false
    );

    DataSegment segment = newSegment("test", 0);
    List<SegmentId> segmentIterable = ImmutableList.of(segment.getId());

    // This is the query that we expect this method to create. We will be testing that it matches the query generated by the method under test.
    SegmentMetadataQuery expectedMetadataQuery = new SegmentMetadataQuery(
        new TableDataSource(segment.getDataSource()),
        new MultipleSpecificSegmentSpec(
            segmentIterable.stream()
                           .map(SegmentId::toDescriptor).collect(Collectors.toList())),
        new AllColumnIncluderator(),
        false,
        queryContext,
        EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        null,
        null
    );

    EasyMock.expect(factoryMock.factorize()).andReturn(lifecycleMock).once();
    // This is the mat of the test, making sure that the query created by the method under test matches the expected query, specifically the operator configured context
    EasyMock.expect(lifecycleMock.runSimple(expectedMetadataQuery, AllowAllAuthenticator.ALLOW_ALL_RESULT, Access.OK))
            .andReturn(QueryResponse.withEmptyContext(Sequences.empty()));

    EasyMock.replay(factoryMock, lifecycleMock);

    schema.runSegmentMetadataQuery(segmentIterable);

    EasyMock.verify(factoryMock, lifecycleMock);
  }

  @Test
  public void testStaleDatasourceRefresh() throws IOException, InterruptedException
  {
    BrokerSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    Set<SegmentId> segments = new HashSet<>();
    Set<String> datasources = new HashSet<>();
    datasources.add("wat");
    Assert.assertNull(schema.getDatasource("wat"));
    schema.refresh(segments, datasources);
    Assert.assertNull(schema.getDatasource("wat"));
  }

  @Test
  public void testRefreshShouldEmitMetrics() throws InterruptedException, IOException
  {
    String dataSource = "xyz";
    CountDownLatch addSegmentLatch = new CountDownLatch(2);
    StubServiceEmitter emitter = new StubServiceEmitter("broker", "host");
    BrokerSegmentMetadataCache schema = new BrokerSegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        emitter,
        new PhysicalDatasourceMetadataFactory(globalTableJoinable, segmentManager),
        new NoopCoordinatorClient()
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (dataSource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      public void removeSegment(final DataSegment segment)
      {
        super.removeSegment(segment);
      }
    };

    List<DataSegment> segments = ImmutableList.of(
        newSegment(dataSource, 1),
        newSegment(dataSource, 2)
    );
    serverView.addSegment(segments.get(0), ServerType.HISTORICAL);
    serverView.addSegment(segments.get(1), ServerType.INDEXER_EXECUTOR);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));
    schema.refresh(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()), Sets.newHashSet(dataSource));

    emitter.verifyEmitted("metadatacache/refresh/time", ImmutableMap.of(DruidMetrics.DATASOURCE, dataSource), 1);
    emitter.verifyEmitted("metadatacache/refresh/count", ImmutableMap.of(DruidMetrics.DATASOURCE, dataSource), 1);
  }

  // This test is present to achieve coverage for BrokerSegmentMetadataCache#initServerViewTimelineCallback
  @Test
  public void testInvokeSegmentSchemaAnnounced() throws InterruptedException
  {
    buildSchemaMarkAndTableLatch();
    serverView.invokeSegmentSchemasAnnouncedDummy();
  }
}
