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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.metadata.metadata.AllColumnIncluderator;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.SegmentReplicaCount;
import org.apache.druid.server.coordinator.loading.SegmentReplicationStatus;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.skife.jdbi.v2.StatementContext;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CoordinatorSegmentMetadataCacheTest extends CoordinatorSegmentMetadataCacheTestBase
{
  // Timeout to allow (rapid) debugging, while not blocking tests with errors.
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final SegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = SegmentMetadataCacheConfig.create("PT1S");
  private CoordinatorSegmentMetadataCache runningSchema;
  private CountDownLatch buildTableLatch = new CountDownLatch(1);
  private CountDownLatch markDataSourceLatch = new CountDownLatch(1);
  private SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private Supplier<SegmentsMetadataManagerConfig> segmentsMetadataManagerConfigSupplier;

  @Before
  @Override
  public void setUp() throws Exception
  {
    super.setUp();
    sqlSegmentsMetadataManager = Mockito.mock(SqlSegmentsMetadataManager.class);
    Mockito.when(sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments()).thenReturn(Collections.emptyList());
    SegmentsMetadataManagerConfig metadataManagerConfig = Mockito.mock(SegmentsMetadataManagerConfig.class);
    Mockito.when(metadataManagerConfig.getPollDuration()).thenReturn(Period.millis(1000));
    segmentsMetadataManagerConfigSupplier = Suppliers.ofInstance(metadataManagerConfig);
  }

  @After
  @Override
  public void tearDown() throws Exception
  {
    super.tearDown();
    if (runningSchema != null) {
      runningSchema.onLeaderStop();
    }
  }

  public CoordinatorSegmentMetadataCache buildSchemaMarkAndTableLatch() throws InterruptedException
  {
    return buildSchemaMarkAndTableLatch(SEGMENT_CACHE_CONFIG_DEFAULT);
  }

  public CoordinatorSegmentMetadataCache buildSchemaMarkAndTableLatch(SegmentMetadataCacheConfig config) throws InterruptedException
  {
    Preconditions.checkState(runningSchema == null);

    runningSchema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        config,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
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

    runningSchema.onLeaderStart();
    runningSchema.awaitInitialization();
    return runningSchema;
  }

  @Test
  public void testGetTableMap() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    Assert.assertEquals(ImmutableSet.of(DATASOURCE1, DATASOURCE2, SOME_DATASOURCE), schema.getDatasourceNames());

    final Set<String> tableNames = schema.getDatasourceNames();
    Assert.assertEquals(ImmutableSet.of(DATASOURCE1, DATASOURCE2, SOME_DATASOURCE), tableNames);
  }

  @Test
  public void testGetTableMapFoo() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    verifyFooDSSchema(schema, 6);
  }

  @Test
  public void testGetTableMapFoo2() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    verifyFoo2DSSchema(schema);
  }

  @Test
  public void testGetTableMapSomeTable() throws InterruptedException
  {
    // using 'newest first' column type merge strategy, the types are expected to be the types defined in the newer
    // segment, except for json, which is special handled
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch(
        new SegmentMetadataCacheConfig() {
          @Override
          public AbstractSegmentMetadataCache.ColumnTypeMergePolicy getMetadataColumnTypeMergePolicy()
          {
            return new AbstractSegmentMetadataCache.FirstTypeMergePolicy();
          }
        }
    );
    final DataSourceInformation fooDs = schema.getDatasource(SOME_DATASOURCE);
    final RowSignature fooRowSignature = fooDs.getRowSignature();
    List<String> columnNames = fooRowSignature.getColumnNames();
    Assert.assertEquals(9, columnNames.size());

    Assert.assertEquals("__time", columnNames.get(0));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("numbery", columnNames.get(1));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(1)).get());

    Assert.assertEquals("numberyArrays", columnNames.get(2));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, fooRowSignature.getColumnType(columnNames.get(2)).get());

    Assert.assertEquals("stringy", columnNames.get(3));
    Assert.assertEquals(ColumnType.STRING, fooRowSignature.getColumnType(columnNames.get(3)).get());

    Assert.assertEquals("array", columnNames.get(4));
    Assert.assertEquals(ColumnType.LONG_ARRAY, fooRowSignature.getColumnType(columnNames.get(4)).get());

    Assert.assertEquals("nested", columnNames.get(5));
    Assert.assertEquals(ColumnType.ofComplex("json"), fooRowSignature.getColumnType(columnNames.get(5)).get());

    Assert.assertEquals("cnt", columnNames.get(6));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(6)).get());

    Assert.assertEquals("m1", columnNames.get(7));
    Assert.assertEquals(ColumnType.DOUBLE, fooRowSignature.getColumnType(columnNames.get(7)).get());

    Assert.assertEquals("unique_dim1", columnNames.get(8));
    Assert.assertEquals(ColumnType.ofComplex("hyperUnique"), fooRowSignature.getColumnType(columnNames.get(8)).get());
  }

  @Test
  public void testGetTableMapSomeTableLeastRestrictiveTypeMerge() throws InterruptedException
  {
    // using 'least restrictive' column type merge strategy, the types are expected to be the types defined as the
    // least restrictive blend across all segments
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    final DataSourceInformation fooDs = schema.getDatasource(SOME_DATASOURCE);

    final RowSignature fooRowSignature = fooDs.getRowSignature();
    List<String> columnNames = fooRowSignature.getColumnNames();
    Assert.assertEquals(9, columnNames.size());

    Assert.assertEquals("__time", columnNames.get(0));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("numbery", columnNames.get(1));
    Assert.assertEquals(ColumnType.DOUBLE, fooRowSignature.getColumnType(columnNames.get(1)).get());

    Assert.assertEquals("numberyArrays", columnNames.get(2));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, fooRowSignature.getColumnType(columnNames.get(2)).get());

    Assert.assertEquals("stringy", columnNames.get(3));
    Assert.assertEquals(ColumnType.STRING_ARRAY, fooRowSignature.getColumnType(columnNames.get(3)).get());

    Assert.assertEquals("array", columnNames.get(4));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, fooRowSignature.getColumnType(columnNames.get(4)).get());

    Assert.assertEquals("nested", columnNames.get(5));
    Assert.assertEquals(ColumnType.ofComplex("json"), fooRowSignature.getColumnType(columnNames.get(5)).get());

    Assert.assertEquals("cnt", columnNames.get(6));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(6)).get());

    Assert.assertEquals("m1", columnNames.get(7));
    Assert.assertEquals(ColumnType.DOUBLE, fooRowSignature.getColumnType(columnNames.get(7)).get());

    Assert.assertEquals("unique_dim1", columnNames.get(8));
    Assert.assertEquals(ColumnType.ofComplex("hyperUnique"), fooRowSignature.getColumnType(columnNames.get(8)).get());
  }

  @Test
  public void testNullDatasource() throws IOException, InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
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
        markDataSourceLatch.countDown();
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

    schema.onLeaderStart();
    schema.awaitInitialization();

    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadatas = schema.getSegmentMetadataSnapshot();
    List<DataSegment> segments = segmentMetadatas.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(6, segments.size());

    // verify that dim3 column isn't present in schema for datasource foo
    DataSourceInformation fooDs = schema.getDatasource("foo");
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
                   .dataSource(DATASOURCE1)
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

    // LinkedHashSet to ensure we encounter the remove datasource first
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
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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

  @Test
  public void testAvailableSegmentMetadataIsRealtime() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    Map<SegmentId, AvailableSegmentMetadata> segmentsMetadata = schema.getSegmentMetadataSnapshot();
    final List<DataSegment> segments = segmentsMetadata.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    // find the only realtime segment with datasource "foo3"
    final DataSegment existingSegment = segments.stream()
                                                .filter(segment -> segment.getDataSource().equals("foo3"))
                                                .findFirst()
                                                .orElse(null);
    Assert.assertNotNull(existingSegment);
    final AvailableSegmentMetadata metadata = segmentsMetadata.get(existingSegment.getId());
    Assert.assertEquals(1L, metadata.isRealtime());
    // get the historical server
    final DruidServer historicalServer = druidServers.stream()
                                                              .filter(s -> s.getType().equals(ServerType.HISTORICAL))
                                                              .findAny()
                                                              .orElse(null);

    Assert.assertNotNull(historicalServer);
    final DruidServerMetadata historicalServerMetadata = historicalServer.getMetadata();

    // add existingSegment to historical
    schema.addSegment(historicalServerMetadata, existingSegment);
    segmentsMetadata = schema.getSegmentMetadataSnapshot();
    // get the segment with datasource "foo3"
    DataSegment currentSegment = segments.stream()
                                         .filter(segment -> segment.getDataSource().equals("foo3"))
                                         .findFirst()
                                         .orElse(null);
    Assert.assertNotNull(currentSegment);
    AvailableSegmentMetadata currentMetadata = segmentsMetadata.get(currentSegment.getId());
    Assert.assertEquals(0L, currentMetadata.isRealtime());

    DruidServer realtimeServer = druidServers.stream()
                                                      .filter(s -> s.getType().equals(ServerType.INDEXER_EXECUTOR))
                                                      .findAny()
                                                      .orElse(null);
    Assert.assertNotNull(realtimeServer);
    // drop existingSegment from realtime task
    schema.removeServerSegment(realtimeServer.getMetadata(), existingSegment);
    segmentsMetadata = schema.getSegmentMetadataSnapshot();
    currentSegment = segments.stream()
                             .filter(segment -> segment.getDataSource().equals("foo3"))
                             .findFirst()
                             .orElse(null);
    Assert.assertNotNull(currentSegment);
    currentMetadata = segmentsMetadata.get(currentSegment.getId());
    Assert.assertEquals(0L, currentMetadata.isRealtime());
  }

  @Test
  public void testSegmentAddedCallbackAddNewHistoricalSegment() throws InterruptedException
  {
    String datasource = "newSegmentAddTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.HISTORICAL);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(7, schema.getTotalSegments());
    List<AvailableSegmentMetadata> metadatas = schema
        .getSegmentMetadataSnapshot()
        .values()
        .stream()
        .filter(metadata -> datasource.equals(metadata.getSegment().getDataSource()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, metadatas.size());
    AvailableSegmentMetadata metadata = metadatas.get(0);
    Assert.assertEquals(0, metadata.isRealtime());
    Assert.assertEquals(0, metadata.getNumRows());
    Assert.assertTrue(schema.getSegmentsNeedingRefresh().contains(metadata.getSegment().getId()));
  }

  @Test
  public void testSegmentAddedCallbackAddExistingSegment() throws InterruptedException
  {
    String datasource = "newSegmentAddTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(2);
    SqlSegmentsMetadataManager sqlSegmentsMetadataManager = Mockito.mock(SqlSegmentsMetadataManager.class);
    Mockito.when(sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments()).thenReturn(Collections.emptyList());
    SegmentsMetadataManagerConfig metadataManagerConfig = Mockito.mock(SegmentsMetadataManagerConfig.class);
    Mockito.when(metadataManagerConfig.getPollDuration()).thenReturn(Period.millis(1000));
    Supplier<SegmentsMetadataManagerConfig> segmentsMetadataManagerConfigSupplier = Suppliers.ofInstance(metadataManagerConfig);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    DataSegment segment = newSegment(datasource, 1);
    serverView.addSegment(segment, ServerType.INDEXER_EXECUTOR);
    serverView.addSegment(segment, ServerType.HISTORICAL);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(7, schema.getTotalSegments());
    List<AvailableSegmentMetadata> metadatas = schema
        .getSegmentMetadataSnapshot()
        .values()
        .stream()
        .filter(metadata -> datasource.equals(metadata.getSegment().getDataSource()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, metadatas.size());
    AvailableSegmentMetadata metadata = metadatas.get(0);
    Assert.assertEquals(0, metadata.isRealtime()); // realtime flag is unset when there is any historical
    Assert.assertEquals(0, metadata.getNumRows());
    Assert.assertEquals(2, metadata.getNumReplicas());
    Assert.assertTrue(schema.getSegmentsNeedingRefresh().contains(metadata.getSegment().getId()));
    Assert.assertFalse(schema.getMutableSegments().contains(metadata.getSegment().getId()));
  }

  @Test
  public void testSegmentAddedCallbackAddNewRealtimeSegment() throws InterruptedException
  {
    String datasource = "newSegmentAddTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    SqlSegmentsMetadataManager sqlSegmentsMetadataManager = Mockito.mock(SqlSegmentsMetadataManager.class);
    Mockito.when(sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments()).thenReturn(Collections.emptyList());
    SegmentsMetadataManagerConfig metadataManagerConfig = Mockito.mock(SegmentsMetadataManagerConfig.class);
    Mockito.when(metadataManagerConfig.getPollDuration()).thenReturn(Period.millis(1000));
    Supplier<SegmentsMetadataManagerConfig> segmentsMetadataManagerConfigSupplier = Suppliers.ofInstance(metadataManagerConfig);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.INDEXER_EXECUTOR);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(7, schema.getTotalSegments());
    List<AvailableSegmentMetadata> metadatas = schema
        .getSegmentMetadataSnapshot()
        .values()
        .stream()
        .filter(metadata -> datasource.equals(metadata.getSegment().getDataSource()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, metadatas.size());
    AvailableSegmentMetadata metadata = metadatas.get(0);
    Assert.assertEquals(1, metadata.isRealtime());
    Assert.assertEquals(0, metadata.getNumRows());
    Assert.assertTrue(schema.getSegmentsNeedingRefresh().contains(metadata.getSegment().getId()));
    Assert.assertTrue(schema.getMutableSegments().contains(metadata.getSegment().getId()));
  }

  @Test
  public void testSegmentAddedCallbackAddNewBroadcastSegment() throws InterruptedException
  {
    String datasource = "newSegmentAddTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    SqlSegmentsMetadataManager sqlSegmentsMetadataManager = Mockito.mock(SqlSegmentsMetadataManager.class);
    Mockito.when(sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments()).thenReturn(Collections.emptyList());
    SegmentsMetadataManagerConfig metadataManagerConfig = Mockito.mock(SegmentsMetadataManagerConfig.class);
    Mockito.when(metadataManagerConfig.getPollDuration()).thenReturn(Period.millis(1000));
    Supplier<SegmentsMetadataManagerConfig> segmentsMetadataManagerConfigSupplier = Suppliers.ofInstance(metadataManagerConfig);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.BROKER);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(6, schema.getTotalSegments());
    List<AvailableSegmentMetadata> metadatas = schema
        .getSegmentMetadataSnapshot()
        .values()
        .stream()
        .filter(metadata -> datasource.equals(metadata.getSegment().getDataSource()))
        .collect(Collectors.toList());
    Assert.assertEquals(0, metadatas.size());
    Assert.assertTrue(schema.getDataSourcesNeedingRebuild().contains(datasource));
  }

  @Test
  public void testSegmentRemovedCallbackEmptyDataSourceAfterRemove() throws InterruptedException, IOException
  {
    String datasource = "segmentRemoveTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    CountDownLatch removeSegmentLatch = new CountDownLatch(1);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      public void removeSegment(final DataSegment segment)
      {
        super.removeSegment(segment);
        if (datasource.equals(segment.getDataSource())) {
          removeSegmentLatch.countDown();
        }
      }
    };

    DataSegment segment = newSegment(datasource, 1);
    serverView.addSegment(segment, ServerType.INDEXER_EXECUTOR);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));
    schema.refresh(Sets.newHashSet(segment.getId()), Sets.newHashSet(datasource));

    serverView.removeSegment(segment, ServerType.INDEXER_EXECUTOR);
    Assert.assertTrue(removeSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(6, schema.getTotalSegments());
    List<AvailableSegmentMetadata> metadatas = schema
        .getSegmentMetadataSnapshot()
        .values()
        .stream()
        .filter(metadata -> datasource.equals(metadata.getSegment().getDataSource()))
        .collect(Collectors.toList());
    Assert.assertEquals(0, metadatas.size());
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(segment.getId()));
    Assert.assertFalse(schema.getMutableSegments().contains(segment.getId()));
    Assert.assertFalse(schema.getDataSourcesNeedingRebuild().contains(datasource));
    Assert.assertFalse(schema.getDatasourceNames().contains(datasource));
  }

  @Test
  public void testSegmentRemovedCallbackNonEmptyDataSourceAfterRemove() throws InterruptedException, IOException
  {
    String datasource = "segmentRemoveTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(2);
    CountDownLatch removeSegmentLatch = new CountDownLatch(1);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      public void removeSegment(final DataSegment segment)
      {
        super.removeSegment(segment);
        if (datasource.equals(segment.getDataSource())) {
          removeSegmentLatch.countDown();
        }
      }
    };

    List<DataSegment> segments = ImmutableList.of(
        newSegment(datasource, 1),
        newSegment(datasource, 2)
    );
    serverView.addSegment(segments.get(0), ServerType.INDEXER_EXECUTOR);
    serverView.addSegment(segments.get(1), ServerType.HISTORICAL);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));
    schema.refresh(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()), Sets.newHashSet(datasource));

    serverView.removeSegment(segments.get(0), ServerType.INDEXER_EXECUTOR);
    Assert.assertTrue(removeSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(7, schema.getTotalSegments());
    List<AvailableSegmentMetadata> metadatas = schema
        .getSegmentMetadataSnapshot()
        .values()
        .stream()
        .filter(metadata -> datasource.equals(metadata.getSegment().getDataSource()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, metadatas.size());
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(segments.get(0).getId()));
    Assert.assertFalse(schema.getMutableSegments().contains(segments.get(0).getId()));
    Assert.assertTrue(schema.getDataSourcesNeedingRebuild().contains(datasource));
    Assert.assertTrue(schema.getDatasourceNames().contains(datasource));
  }

  @Test
  public void testServerSegmentRemovedCallbackRemoveUnknownSegment() throws InterruptedException
  {
    String datasource = "serverSegmentRemoveTest";
    CountDownLatch removeServerSegmentLatch = new CountDownLatch(1);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.removeServerSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          removeServerSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.BROKER);

    serverView.removeSegment(newSegment(datasource, 1), ServerType.HISTORICAL);
    Assert.assertTrue(removeServerSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(6, schema.getTotalSegments());
  }

  @Test
  public void testServerSegmentRemovedCallbackRemoveBrokerSegment() throws InterruptedException
  {
    String datasource = "serverSegmentRemoveTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    CountDownLatch removeServerSegmentLatch = new CountDownLatch(1);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      public void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.removeServerSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          removeServerSegmentLatch.countDown();
        }
      }
    };

    DataSegment segment = newSegment(datasource, 1);
    serverView.addSegment(segment, ServerType.HISTORICAL);
    serverView.addSegment(segment, ServerType.BROKER);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    serverView.removeSegment(segment, ServerType.BROKER);
    Assert.assertTrue(removeServerSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(7, schema.getTotalSegments());
    Assert.assertTrue(schema.getDataSourcesNeedingRebuild().contains(datasource));
  }

  @Test
  public void testServerSegmentRemovedCallbackRemoveHistoricalSegment() throws InterruptedException
  {
    String datasource = "serverSegmentRemoveTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    CountDownLatch removeServerSegmentLatch = new CountDownLatch(1);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    )
    {
      @Override
      public void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      public void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.removeServerSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          removeServerSegmentLatch.countDown();
        }
      }
    };

    DataSegment segment = newSegment(datasource, 1);
    serverView.addSegment(segment, ServerType.HISTORICAL);
    serverView.addSegment(segment, ServerType.BROKER);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    serverView.removeSegment(segment, ServerType.HISTORICAL);
    Assert.assertTrue(removeServerSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(7, schema.getTotalSegments());
    List<AvailableSegmentMetadata> metadatas = schema
        .getSegmentMetadataSnapshot()
        .values()
        .stream()
        .filter(metadata -> datasource.equals(metadata.getSegment().getDataSource()))
        .collect(Collectors.toList());
    Assert.assertEquals(1, metadatas.size());
    AvailableSegmentMetadata metadata = metadatas.get(0);
    Assert.assertEquals(0, metadata.isRealtime());
    Assert.assertEquals(0, metadata.getNumRows());
    Assert.assertEquals(0, metadata.getNumReplicas()); // brokers are not counted as replicas yet
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
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        factoryMock,
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        internalQueryConfig,
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
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
        EnumSet.of(SegmentMetadataQuery.AnalysisType.AGGREGATORS),
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
  public void testSegmentMetadataColumnType()
  {
    // Verify order is preserved.
    final LinkedHashMap<String, ColumnAnalysis> columns = new LinkedHashMap<>();
    columns.put(
        "a",
        new ColumnAnalysis(ColumnType.STRING, ColumnType.STRING.asTypeString(), false, true, 1234, 26, "a", "z", null)
    );

    columns.put(
        "count",
        new ColumnAnalysis(ColumnType.LONG, ColumnType.LONG.asTypeString(), false, true, 1234, 26, "a", "z", null)
    );

    columns.put(
        "b",
        new ColumnAnalysis(ColumnType.DOUBLE, ColumnType.DOUBLE.asTypeString(), false, true, 1234, 26, null, null, null)
    );

    RowSignature signature = AbstractSegmentMetadataCache.analysisToRowSignature(
        new SegmentAnalysis(
            "id",
            ImmutableList.of(Intervals.utc(1L, 2L)),
            columns,
            1234,
            100,
            null,
            null,
            null,
            null
        )
    );

    Assert.assertEquals(
        RowSignature.builder()
                    .add("a", ColumnType.STRING)
                    .add("count", ColumnType.LONG)
                    .add("b", ColumnType.DOUBLE)
                    .build(),
        signature
    );
  }

  @Test
  public void testSegmentMetadataFallbackType()
  {
    RowSignature signature = AbstractSegmentMetadataCache.analysisToRowSignature(
        new SegmentAnalysis(
            "id",
            ImmutableList.of(Intervals.utc(1L, 2L)),
            new LinkedHashMap<>(
                ImmutableMap.of(
                    "a",
                    new ColumnAnalysis(
                        null,
                        ColumnType.STRING.asTypeString(),
                        false,
                        true,
                        1234,
                        26,
                        "a",
                        "z",
                        null
                    ),
                    "count",
                    new ColumnAnalysis(
                        null,
                        ColumnType.LONG.asTypeString(),
                        false,
                        true,
                        1234,
                        null,
                        null,
                        null,
                        null
                    ),
                    "distinct",
                    new ColumnAnalysis(
                        null,
                        "hyperUnique",
                        false,
                        true,
                        1234,
                        null,
                        null,
                        null,
                        null
                    )
                )
            ),
            1234,
            100,
            null,
            null,
            null,
            null
        )
    );
    Assert.assertEquals(
        RowSignature.builder().add("a", ColumnType.STRING).add("count", ColumnType.LONG).add("distinct", ColumnType.ofComplex("hyperUnique")).build(),
        signature
    );
  }

  @Test
  public void testStaleDatasourceRefresh() throws IOException, InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        emitter,
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
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

  @Test
  public void testMergeOrCreateRowSignatureDeltaSchemaNoPreviousSignature() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();

    EmittingLogger.registerEmitter(new StubServiceEmitter("coordinator", "dummy"));

    Assert.assertFalse(schema.mergeOrCreateRowSignature(
        segment1.getId(),
        null,
        new SegmentSchemas.SegmentSchema(
            DATASOURCE1,
            segment1.getId().toString(),
            true,
            20,
            ImmutableList.of("dim1"),
            Collections.emptyList(),
            ImmutableMap.of("dim1", ColumnType.STRING)
        )
    ).isPresent());
  }

  @Test
  public void testMergeOrCreateRowSignatureDeltaSchema() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();

    AvailableSegmentMetadata availableSegmentMetadata = schema.getAvailableSegmentMetadata(DATASOURCE1, segment1.getId());

    Optional<RowSignature> mergedSignature = schema.mergeOrCreateRowSignature(
        segment1.getId(),
        availableSegmentMetadata.getRowSignature(),
        new SegmentSchemas.SegmentSchema(
            DATASOURCE1,
            segment1.getId().toString(),
            true,
            1000,
            ImmutableList.of("dim2"),
            ImmutableList.of("m1"),
            ImmutableMap.of("dim2", ColumnType.STRING, "m1", ColumnType.STRING)
        )
    );

    Assert.assertTrue(mergedSignature.isPresent());
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add("__time", ColumnType.LONG);
    rowSignatureBuilder.add("dim1", ColumnType.STRING);
    rowSignatureBuilder.add("cnt", ColumnType.LONG);
    rowSignatureBuilder.add("m1", ColumnType.STRING);
    rowSignatureBuilder.add("unique_dim1", ColumnType.ofComplex("hyperUnique"));
    rowSignatureBuilder.add("dim2", ColumnType.STRING);
    Assert.assertEquals(rowSignatureBuilder.build(), mergedSignature.get());
  }

  @Test
  public void testMergeOrCreateRowSignatureDeltaSchemaNewUpdateColumnOldNewColumn() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();

    EmittingLogger.registerEmitter(new StubServiceEmitter("coordinator", "dummy"));

    AvailableSegmentMetadata availableSegmentMetadata = schema.getAvailableSegmentMetadata(DATASOURCE1, segment1.getId());

    Optional<RowSignature> mergedSignature = schema.mergeOrCreateRowSignature(
        segment1.getId(),
        availableSegmentMetadata.getRowSignature(),
        new SegmentSchemas.SegmentSchema(
            DATASOURCE1,
            segment1.getId().toString(),
            true,
            1000,
            ImmutableList.of("m1"), // m1 is a new column in the delta update, but it already exists
            ImmutableList.of("m2"), // m2 is a column to be updated in the delta update, but it doesn't exist
            ImmutableMap.of("m1", ColumnType.LONG, "m2", ColumnType.STRING)
        )
    );

    Assert.assertTrue(mergedSignature.isPresent());
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add("__time", ColumnType.LONG);
    rowSignatureBuilder.add("dim1", ColumnType.STRING);
    rowSignatureBuilder.add("cnt", ColumnType.LONG);
    // type for m1 is updated
    rowSignatureBuilder.add("m1", ColumnType.DOUBLE);
    rowSignatureBuilder.add("unique_dim1", ColumnType.ofComplex("hyperUnique"));
    // m2 is added
    rowSignatureBuilder.add("m2", ColumnType.STRING);
    Assert.assertEquals(rowSignatureBuilder.build(), mergedSignature.get());
  }

  @Test
  public void testMergeOrCreateRowSignatureAbsoluteSchema() throws InterruptedException
  {
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch();

    AvailableSegmentMetadata availableSegmentMetadata = schema.getAvailableSegmentMetadata(DATASOURCE1, segment1.getId());

    Optional<RowSignature> mergedSignature = schema.mergeOrCreateRowSignature(
        segment1.getId(),
        availableSegmentMetadata.getRowSignature(),
        new SegmentSchemas.SegmentSchema(
            DATASOURCE1,
            segment1.getId().toString(),
            false,
            1000,
            ImmutableList.of("__time", "cnt", "dim2"),
            ImmutableList.of(),
            ImmutableMap.of("__time", ColumnType.LONG, "dim2", ColumnType.STRING, "cnt", ColumnType.LONG)
        )
    );

    Assert.assertTrue(mergedSignature.isPresent());
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add("__time", ColumnType.LONG);
    rowSignatureBuilder.add("cnt", ColumnType.LONG);
    rowSignatureBuilder.add("dim2", ColumnType.STRING);
    Assert.assertEquals(rowSignatureBuilder.build(), mergedSignature.get());
  }

  @Test
  public void testRealtimeSchemaAnnouncement() throws InterruptedException, IOException
  {
    // test schema update is applied and realtime segments are not refereshed via segment metadata query
    CountDownLatch schemaAddedLatch = new CountDownLatch(1);

    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    ) {
      @Override
      void updateSchemaForRealtimeSegments(SegmentSchemas segmentSchemas)
      {
        super.updateSchemaForRealtimeSegments(segmentSchemas);
        schemaAddedLatch.countDown();
      }
    };

    schema.onLeaderStart();
    schema.awaitInitialization();

    AvailableSegmentMetadata availableSegmentMetadata = schema.getAvailableSegmentMetadata(DATASOURCE3, realtimeSegment1.getId());
    Assert.assertNull(availableSegmentMetadata.getRowSignature());

    // refresh all segments, verify that realtime segments isn't refreshed
    schema.refresh(walker.getSegments().stream().map(DataSegment::getId).collect(Collectors.toSet()), new HashSet<>());

    Assert.assertNull(schema.getDatasource(DATASOURCE3));
    Assert.assertNotNull(schema.getDatasource(DATASOURCE1));
    Assert.assertNotNull(schema.getDatasource(DATASOURCE2));
    Assert.assertNotNull(schema.getDatasource(SOME_DATASOURCE));

    serverView.addSegmentSchemas(
        new SegmentSchemas(Collections.singletonList(
            new SegmentSchemas.SegmentSchema(
                DATASOURCE3,
                realtimeSegment1.getId().toString(),
                false,
                1000,
                ImmutableList.of("__time", "dim1", "cnt", "m1", "unique_dim1", "dim2"),
                ImmutableList.of(),
                ImmutableMap.of(
                    "__time",
                    ColumnType.LONG,
                    "dim1",
                    ColumnType.STRING,
                    "cnt",
                    ColumnType.LONG,
                    "m1",
                    ColumnType.STRING,
                    "unique_dim1",
                    ColumnType.ofComplex("hyperUnique"),
                    "dim2",
                    ColumnType.STRING
                )
            )
        )));

    Assert.assertTrue(schemaAddedLatch.await(1, TimeUnit.SECONDS));

    availableSegmentMetadata = schema.getAvailableSegmentMetadata(DATASOURCE3, realtimeSegment1.getId());

    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add("__time", ColumnType.LONG);
    rowSignatureBuilder.add("dim1", ColumnType.STRING);
    rowSignatureBuilder.add("cnt", ColumnType.LONG);
    rowSignatureBuilder.add("m1", ColumnType.STRING);
    rowSignatureBuilder.add("unique_dim1", ColumnType.ofComplex("hyperUnique"));
    rowSignatureBuilder.add("dim2", ColumnType.STRING);
    Assert.assertEquals(rowSignatureBuilder.build(), availableSegmentMetadata.getRowSignature());
  }

  @Test
  public void testRealtimeSchemaAnnouncementDataSourceSchemaUpdated() throws InterruptedException
  {
    // test schema update is applied and realtime segments are not refereshed via segment metadata query
    CountDownLatch refresh1Latch = new CountDownLatch(1);
    CountDownLatch refresh2Latch = new CountDownLatch(1);

    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    ) {
      @Override
      public void refresh(Set<SegmentId> segmentsToRefresh, Set<String> dataSourcesToRebuild)
          throws IOException
      {
        super.refresh(segmentsToRefresh, dataSourcesToRebuild);
        if (refresh1Latch.getCount() == 0) {
          refresh2Latch.countDown();
        } else {
          refresh1Latch.countDown();
        }
      }
    };

    schema.onLeaderStart();
    schema.awaitInitialization();
    Assert.assertTrue(refresh1Latch.await(10, TimeUnit.SECONDS));

    AvailableSegmentMetadata availableSegmentMetadata = schema.getAvailableSegmentMetadata(DATASOURCE3, realtimeSegment1.getId());
    Assert.assertNull(availableSegmentMetadata.getRowSignature());

    Assert.assertNull(schema.getDatasource(DATASOURCE3));
    Assert.assertNotNull(schema.getDatasource(DATASOURCE1));
    Assert.assertNotNull(schema.getDatasource(DATASOURCE2));
    Assert.assertNotNull(schema.getDatasource(SOME_DATASOURCE));

    serverView.addSegmentSchemas(
        new SegmentSchemas(Collections.singletonList(
            new SegmentSchemas.SegmentSchema(
                DATASOURCE3,
                realtimeSegment1.getId().toString(),
                false,
                1000,
                ImmutableList.of("__time", "dim1", "cnt", "m1", "unique_dim1", "dim2"),
                ImmutableList.of(),
                ImmutableMap.of(
                    "__time",
                    ColumnType.LONG,
                    "dim1",
                    ColumnType.STRING,
                    "cnt",
                    ColumnType.LONG,
                    "m1",
                    ColumnType.STRING,
                    "unique_dim1",
                    ColumnType.ofComplex("hyperUnique"),
                    "dim2",
                    ColumnType.STRING
                )
            )
        )));

    Assert.assertTrue(refresh2Latch.await(10, TimeUnit.SECONDS));

    Assert.assertNotNull(schema.getDatasource(DATASOURCE3));
    Assert.assertNotNull(schema.getDatasource(DATASOURCE1));
    Assert.assertNotNull(schema.getDatasource(DATASOURCE2));
    Assert.assertNotNull(schema.getDatasource(SOME_DATASOURCE));

    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add("__time", ColumnType.LONG);
    rowSignatureBuilder.add("dim1", ColumnType.STRING);
    rowSignatureBuilder.add("cnt", ColumnType.LONG);
    rowSignatureBuilder.add("m1", ColumnType.STRING);
    rowSignatureBuilder.add("unique_dim1", ColumnType.ofComplex("hyperUnique"));
    rowSignatureBuilder.add("dim2", ColumnType.STRING);
    Assert.assertEquals(rowSignatureBuilder.build(), schema.getDatasource(DATASOURCE3).getRowSignature());
  }

  @Test
  public void testSchemaBackfilling() throws InterruptedException
  {
    CentralizedDatasourceSchemaConfig config = CentralizedDatasourceSchemaConfig.create();
    config.setEnabled(true);
    config.setBackFillEnabled(true);
    config.setBackFillPeriod(1);

    backFillQueue =
        new SegmentSchemaBackFillQueue(
            segmentSchemaManager,
            ScheduledExecutors::fixed,
            segmentSchemaCache,
            fingerprintGenerator,
            new NoopServiceEmitter(),
            config
        );

    QueryableIndexCursorFactory index1CursorFactory = new QueryableIndexCursorFactory(index1);
    QueryableIndexCursorFactory index2CursorFactory = new QueryableIndexCursorFactory(index2);

    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();

    Set<DataSegment> segmentsToPersist = new HashSet<>();
    segmentsToPersist.add(segment1);
    segmentsToPersist.add(segment2);
    segmentsToPersist.add(segment3);

    List<SegmentSchemaManager.SegmentSchemaMetadataPlus> pluses = new ArrayList<>();
    pluses.add(new SegmentSchemaManager.SegmentSchemaMetadataPlus(
        segment1.getId(),
        fingerprintGenerator.generateFingerprint(
            new SchemaPayload(index1CursorFactory.getRowSignature()),
            segment1.getDataSource(),
            CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
        ),
        new SchemaPayloadPlus(
            new SchemaPayload(
                index1CursorFactory.getRowSignature()),
            (long) index1.getNumRows()
        )
    ));
    pluses.add(new SegmentSchemaManager.SegmentSchemaMetadataPlus(
        segment2.getId(),
        fingerprintGenerator.generateFingerprint(
            new SchemaPayload(index2CursorFactory.getRowSignature()),
            segment1.getDataSource(),
            CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
        ),
        new SchemaPayloadPlus(
            new SchemaPayload(
                index2CursorFactory.getRowSignature()),
            (long) index2.getNumRows()
        )
    ));

    SegmentSchemaTestUtils segmentSchemaTestUtils = new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, mapper);
    segmentSchemaTestUtils.insertUsedSegments(segmentsToPersist, Collections.emptyMap());

    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable(DATASOURCE1, pluses, CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentMetadataMap = new ImmutableMap.Builder<>();
    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadMap = new ImmutableMap.Builder<>();

    derbyConnector.retryWithHandle(handle -> {
      handle.createQuery(StringUtils.format(
                "select s1.id, s1.dataSource, s1.schema_fingerprint, s1.num_rows, s2.payload "
                + "from %1$s as s1 inner join %2$s as s2 on s1.schema_fingerprint = s2.fingerprint",
                tablesConfig.getSegmentsTable(),
                tablesConfig.getSegmentSchemasTable()
            ))
            .map((int index, ResultSet r, StatementContext ctx) -> {
              try {
                String segmentId = r.getString(1);
                String dataSource = r.getString(2);
                String schemaFingerprint = r.getString(3);
                long numRows = r.getLong(4);
                SchemaPayload schemaPayload = mapper.readValue(r.getBytes(5), SchemaPayload.class);
                schemaPayloadMap.put(schemaFingerprint, schemaPayload);
                segmentMetadataMap.put(SegmentId.tryParse(dataSource, segmentId), new SegmentMetadata(numRows, schemaFingerprint));
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
              return null;
            }).list();
      return null;
    });

    segmentSchemaCache.updateFinalizedSegmentSchema(
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentMetadataMap.build(), schemaPayloadMap.build()));
    segmentSchemaCache.setInitialized();

    serverView = new TestCoordinatorServerView(Collections.emptyList(), Collections.emptyList());

    AtomicInteger refreshCount = new AtomicInteger();

    CountDownLatch latch = new CountDownLatch(2);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    ) {
      @Override
      public Set<SegmentId> refreshSegmentsForDataSource(String dataSource, Set<SegmentId> segments)
          throws IOException
      {
        refreshCount.incrementAndGet();
        return super.refreshSegmentsForDataSource(dataSource, segments);
      }

      @Override
      public void refresh(Set<SegmentId> segmentsToRefresh, Set<String> dataSourcesToRebuild)
          throws IOException
      {
        super.refresh(segmentsToRefresh, dataSourcesToRebuild);
        latch.countDown();
      }
    };

    serverView.addSegment(segment1, ServerType.HISTORICAL);
    serverView.addSegment(segment2, ServerType.HISTORICAL);

    schema.onLeaderStart();
    schema.awaitInitialization();

    // verify metadata query is not executed, since the schema is already cached
    Assert.assertEquals(0, refreshCount.get());

    // verify that datasource schema is built
    verifyFooDSSchema(schema, 6);

    serverView.addSegment(segment3, ServerType.HISTORICAL);

    latch.await();

    verifyFoo2DSSchema(schema);

    derbyConnector.retryWithHandle(handle -> {
      handle.createQuery(
                StringUtils.format(
                    "select s2.payload, s1.num_rows "
                    + "from %1$s as s1 inner join %2$s as s2 on s1.schema_fingerprint = s2.fingerprint where s1.id = '%3$s'",
                    tablesConfig.getSegmentsTable(),
                    tablesConfig.getSegmentSchemasTable(),
                    segment3.getId().toString()
                ))
            .map((int index, ResultSet r, StatementContext ctx) -> {
              try {
                SchemaPayload schemaPayload = mapper.readValue(r.getBytes(1), SchemaPayload.class);
                long numRows = r.getLong(2);
                QueryableIndexCursorFactory cursorFa = new QueryableIndexCursorFactory(index2);
                Assert.assertEquals(cursorFa.getRowSignature(), schemaPayload.getRowSignature());
                Assert.assertEquals(index2.getNumRows(), numRows);
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
              return null;
            })
            .list();
      return null;
    });
  }

  /**
   * Segment metadata query is disabled in this test.
   * foo2 datasource has only 1 segment, we add its schema to the cache.
   * This segment is added again.
   * In the end we verify the schema for foo2 datasource.
   */
  @Test
  public void testSameSegmentAddedOnMultipleServer() throws InterruptedException, IOException
  {
    SegmentMetadataCacheConfig config = SegmentMetadataCacheConfig.create("PT1S");
    config.setDisableSegmentMetadataQueries(true);
    CoordinatorSegmentMetadataCache schema = buildSchemaMarkAndTableLatch(config);

    QueryableIndexSegment queryableIndexSegment = new QueryableIndexSegment(index2, SegmentId.dummy("test"));
    PhysicalSegmentInspector rowCountInspector = queryableIndexSegment.as(PhysicalSegmentInspector.class);
    QueryableIndexCursorFactory cursorFactory = new QueryableIndexCursorFactory(index2);

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentStatsMap = new ImmutableMap.Builder<>();
    segmentStatsMap.put(segment3.getId(), new SegmentMetadata((long) rowCountInspector.getNumRows(), "fp"));
    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadMap = new ImmutableMap.Builder<>();
    schemaPayloadMap.put("fp", new SchemaPayload(cursorFactory.getRowSignature()));
    segmentSchemaCache.updateFinalizedSegmentSchema(
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentStatsMap.build(), schemaPayloadMap.build())
    );

    Map<SegmentId, AvailableSegmentMetadata> segmentsMetadata = schema.getSegmentMetadataSnapshot();
    List<DataSegment> segments = segmentsMetadata.values()
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

    AvailableSegmentMetadata existingMetadata = segmentsMetadata.get(existingSegment.getId());

    segmentStatsMap = new ImmutableMap.Builder<>();
    segmentStatsMap.put(
        existingSegment.getId(),
        new SegmentMetadata(5L, "fp")
    );
    segmentSchemaCache.updateFinalizedSegmentSchema(
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentStatsMap.build(), schemaPayloadMap.build())
    );

    // find a druidServer holding existingSegment
    final Pair<DruidServer, DataSegment> pair = druidServers
        .stream()
        .flatMap(druidServer ->
                     serverView.getSegmentsOfServer(druidServer).stream()
                               .filter(segment -> segment.getId().equals(existingSegment.getId()))
                               .map(segment -> Pair.of(druidServer, segment))
        )
        .findAny()
        .orElse(null);

    Assert.assertNotNull(pair);
    final DruidServer server = pair.lhs;
    Assert.assertNotNull(server);
    final DruidServerMetadata druidServerMetadata = server.getMetadata();
    // invoke SegmentMetadataCache#addSegment on existingSegment
    schema.addSegment(druidServerMetadata, existingSegment);

    segmentsMetadata = schema.getSegmentMetadataSnapshot();

    segments = segmentsMetadata.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(6, segments.size());

    schema.refresh(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()), new HashSet<>());

    verifyFoo2DSSchema(schema);

    // invoke SegmentMetadataCache#addSegment on existingSegment
    schema.addSegment(druidServerMetadata, existingSegment);
    segmentsMetadata = schema.getSegmentMetadataSnapshot();
    // get the only segment with datasource "foo2"
    final DataSegment currentSegment = segments.stream()
                                               .filter(segment -> segment.getDataSource().equals("foo2"))
                                               .findFirst()
                                               .orElse(null);
    final AvailableSegmentMetadata currentMetadata = segmentsMetadata.get(currentSegment.getId());
    Assert.assertEquals(currentSegment.getId(), currentMetadata.getSegment().getId());
    Assert.assertEquals(5L, currentMetadata.getNumRows());
    // numreplicas do not change here since we addSegment with the same server which was serving existingSegment before
    Assert.assertEquals(existingMetadata.getNumReplicas(), currentMetadata.getNumReplicas());
  }

  private CoordinatorSegmentMetadataCache setupForColdDatasourceSchemaTest(ServiceEmitter emitter)
  {
    // foo has both hot and cold segments
    DataSegment coldSegment =
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(Intervals.of("1998/P2Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();

    // cold has only cold segments
    DataSegment singleColdSegment =
        DataSegment.builder()
                   .dataSource("cold")
                   .interval(Intervals.of("2000/P2Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentStatsMap = new ImmutableMap.Builder<>();
    segmentStatsMap.put(coldSegment.getId(), new SegmentMetadata(20L, "foo-fingerprint"));
    segmentStatsMap.put(singleColdSegment.getId(), new SegmentMetadata(20L, "cold-fingerprint"));
    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadMap = new ImmutableMap.Builder<>();
    schemaPayloadMap.put(
        "foo-fingerprint",
        new SchemaPayload(RowSignature.builder()
                                      .add("dim1", ColumnType.STRING)
                                      .add("c1", ColumnType.STRING)
                                      .add("c2", ColumnType.LONG)
                                      .build())
    );
    schemaPayloadMap.put(
        "cold-fingerprint",
        new SchemaPayload(
            RowSignature.builder()
                              .add("f1", ColumnType.STRING)
                              .add("f2", ColumnType.DOUBLE)
                              .build()
        )
    );

    segmentSchemaCache.updateFinalizedSegmentSchema(
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentStatsMap.build(), schemaPayloadMap.build())
    );

    List<ImmutableDruidDataSource> druidDataSources = new ArrayList<>();
    Map<SegmentId, DataSegment> segmentMap = new HashMap<>();
    segmentMap.put(coldSegment.getId(), coldSegment);
    segmentMap.put(segment1.getId(), segment1);
    segmentMap.put(segment2.getId(), segment2);
    druidDataSources.add(new ImmutableDruidDataSource(
        coldSegment.getDataSource(),
        Collections.emptyMap(),
        segmentMap
    ));
    druidDataSources.add(new ImmutableDruidDataSource(
        singleColdSegment.getDataSource(),
        Collections.emptyMap(),
        Collections.singletonMap(singleColdSegment.getId(), singleColdSegment)
    ));

    Mockito.when(
               sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments())
           .thenReturn(druidDataSources);

    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        emitter,
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    );

    SegmentReplicaCount zeroSegmentReplicaCount = Mockito.mock(SegmentReplicaCount.class);
    SegmentReplicaCount nonZeroSegmentReplicaCount = Mockito.mock(SegmentReplicaCount.class);
    Mockito.when(zeroSegmentReplicaCount.required()).thenReturn(0);
    Mockito.when(nonZeroSegmentReplicaCount.required()).thenReturn(1);
    SegmentReplicationStatus segmentReplicationStatus = Mockito.mock(SegmentReplicationStatus.class);
    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(coldSegment.getId())))
           .thenReturn(zeroSegmentReplicaCount);
    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(singleColdSegment.getId())))
           .thenReturn(zeroSegmentReplicaCount);
    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(segment1.getId())))
           .thenReturn(nonZeroSegmentReplicaCount);

    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(segment2.getId())))
           .thenReturn(nonZeroSegmentReplicaCount);

    schema.updateSegmentReplicationStatus(segmentReplicationStatus);
    schema.updateSegmentReplicationStatus(segmentReplicationStatus);

    return schema;
  }

  @Test
  public void testColdDatasourceSchema_refreshAfterColdSchemaExec() throws IOException
  {
    StubServiceEmitter emitter = new StubServiceEmitter("coordinator", "host");
    CoordinatorSegmentMetadataCache schema = setupForColdDatasourceSchemaTest(emitter);

    schema.coldDatasourceSchemaExec();

    emitter.verifyEmitted("metadatacache/deepStorageOnly/segment/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "foo"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/refresh/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "foo"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/segment/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "cold"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/refresh/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "cold"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/process/time", 1);

    Assert.assertEquals(new HashSet<>(Arrays.asList("foo", "cold")), schema.getDataSourceInformationMap().keySet());

    // verify that cold schema for both foo and cold is present
    RowSignature fooSignature = schema.getDatasource("foo").getRowSignature();
    List<String> columnNames = fooSignature.getColumnNames();

    // verify that foo schema doesn't contain columns from hot segments
    Assert.assertEquals(3, columnNames.size());

    Assert.assertEquals("dim1", columnNames.get(0));
    Assert.assertEquals(ColumnType.STRING, fooSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("c1", columnNames.get(1));
    Assert.assertEquals(ColumnType.STRING, fooSignature.getColumnType(columnNames.get(1)).get());

    Assert.assertEquals("c2", columnNames.get(2));
    Assert.assertEquals(ColumnType.LONG, fooSignature.getColumnType(columnNames.get(2)).get());

    RowSignature coldSignature = schema.getDatasource("cold").getRowSignature();
    columnNames = coldSignature.getColumnNames();
    Assert.assertEquals("f1", columnNames.get(0));
    Assert.assertEquals(ColumnType.STRING, coldSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("f2", columnNames.get(1));
    Assert.assertEquals(ColumnType.DOUBLE, coldSignature.getColumnType(columnNames.get(1)).get());

    Set<SegmentId> segmentIds = new HashSet<>();
    segmentIds.add(segment1.getId());
    segmentIds.add(segment2.getId());

    schema.refresh(segmentIds, new HashSet<>());

    Assert.assertEquals(new HashSet<>(Arrays.asList("foo", "cold")), schema.getDataSourceInformationMap().keySet());

    coldSignature = schema.getDatasource("cold").getRowSignature();
    columnNames = coldSignature.getColumnNames();
    Assert.assertEquals("f1", columnNames.get(0));
    Assert.assertEquals(ColumnType.STRING, coldSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("f2", columnNames.get(1));
    Assert.assertEquals(ColumnType.DOUBLE, coldSignature.getColumnType(columnNames.get(1)).get());

    // foo now contains schema from both hot and cold segments
    verifyFooDSSchema(schema, 8);
    RowSignature rowSignature = schema.getDatasource("foo").getRowSignature();

    // cold columns should be present at the end
    columnNames = rowSignature.getColumnNames();
    Assert.assertEquals("c1", columnNames.get(6));
    Assert.assertEquals(ColumnType.STRING, rowSignature.getColumnType(columnNames.get(6)).get());

    Assert.assertEquals("c2", columnNames.get(7));
    Assert.assertEquals(ColumnType.LONG, rowSignature.getColumnType(columnNames.get(7)).get());
  }

  @Test
  public void testColdDatasourceSchema_coldSchemaExecAfterRefresh() throws IOException
  {
    StubServiceEmitter emitter = new StubServiceEmitter("coordinator", "host");
    CoordinatorSegmentMetadataCache schema = setupForColdDatasourceSchemaTest(emitter);

    Set<SegmentId> segmentIds = new HashSet<>();
    segmentIds.add(segment1.getId());
    segmentIds.add(segment2.getId());

    schema.refresh(segmentIds, new HashSet<>());
    // cold datasource shouldn't be present
    Assert.assertEquals(Collections.singleton("foo"), schema.getDataSourceInformationMap().keySet());

    // cold columns shouldn't be present
    verifyFooDSSchema(schema, 6);
    Assert.assertNull(schema.getDatasource("cold"));

    schema.coldDatasourceSchemaExec();

    emitter.verifyEmitted("metadatacache/deepStorageOnly/segment/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "foo"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/refresh/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "foo"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/segment/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "cold"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/refresh/count", ImmutableMap.of(DruidMetrics.DATASOURCE, "cold"), 1);
    emitter.verifyEmitted("metadatacache/deepStorageOnly/process/time", 1);

    // cold datasource should be present now
    Assert.assertEquals(new HashSet<>(Arrays.asList("foo", "cold")), schema.getDataSourceInformationMap().keySet());

    RowSignature coldSignature = schema.getDatasource("cold").getRowSignature();
    List<String> columnNames = coldSignature.getColumnNames();
    Assert.assertEquals("f1", columnNames.get(0));
    Assert.assertEquals(ColumnType.STRING, coldSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("f2", columnNames.get(1));
    Assert.assertEquals(ColumnType.DOUBLE, coldSignature.getColumnType(columnNames.get(1)).get());

    // columns from cold datasource should be present
    verifyFooDSSchema(schema, 8);
    RowSignature rowSignature = schema.getDatasource("foo").getRowSignature();

    columnNames = rowSignature.getColumnNames();
    Assert.assertEquals("c1", columnNames.get(6));
    Assert.assertEquals(ColumnType.STRING, rowSignature.getColumnType(columnNames.get(6)).get());

    Assert.assertEquals("c2", columnNames.get(7));
    Assert.assertEquals(ColumnType.LONG, rowSignature.getColumnType(columnNames.get(7)).get());
  }

  @Test
  public void testColdDatasourceSchema_verifyStaleDatasourceRemoved()
  {
    DataSegment coldSegmentAlpha =
        DataSegment.builder()
                   .dataSource("alpha")
                   .interval(Intervals.of("2000/P2Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();

    DataSegment coldSegmentBeta =
        DataSegment.builder()
                   .dataSource("beta")
                   .interval(Intervals.of("2000/P2Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();

    DataSegment coldSegmentGamma =
        DataSegment.builder()
            .dataSource("gamma")
            .interval(Intervals.of("2000/P2Y"))
            .version("1")
            .shardSpec(new LinearShardSpec(0))
            .size(0)
            .build();

    DataSegment hotSegmentGamma =
        DataSegment.builder()
                   .dataSource("gamma")
                   .interval(Intervals.of("2001/P2Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build();

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentStatsMap = new ImmutableMap.Builder<>();
    segmentStatsMap.put(coldSegmentAlpha.getId(), new SegmentMetadata(20L, "cold"));
    segmentStatsMap.put(coldSegmentBeta.getId(), new SegmentMetadata(20L, "cold"));
    segmentStatsMap.put(hotSegmentGamma.getId(), new SegmentMetadata(20L, "hot"));
    segmentStatsMap.put(coldSegmentGamma.getId(), new SegmentMetadata(20L, "cold"));

    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadMap = new ImmutableMap.Builder<>();
    schemaPayloadMap.put(
        "cold",
        new SchemaPayload(RowSignature.builder()
                                      .add("dim1", ColumnType.STRING)
                                      .add("c1", ColumnType.STRING)
                                      .add("c2", ColumnType.LONG)
                                      .build())
    );
    schemaPayloadMap.put(
        "hot",
        new SchemaPayload(RowSignature.builder()
                                      .add("c3", ColumnType.STRING)
                                      .add("c4", ColumnType.STRING)
                                      .build())
    );
    segmentSchemaCache.updateFinalizedSegmentSchema(
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentStatsMap.build(), schemaPayloadMap.build())
    );

    List<ImmutableDruidDataSource> druidDataSources = new ArrayList<>();
    druidDataSources.add(
        new ImmutableDruidDataSource(
            "alpha",
            Collections.emptyMap(),
            Collections.singletonMap(coldSegmentAlpha.getId(), coldSegmentAlpha)
        )
    );

    Map<SegmentId, DataSegment> gammaSegments = new HashMap<>();
    gammaSegments.put(hotSegmentGamma.getId(), hotSegmentGamma);
    gammaSegments.put(coldSegmentGamma.getId(), coldSegmentGamma);

    druidDataSources.add(
        new ImmutableDruidDataSource(
            "gamma",
            Collections.emptyMap(),
            gammaSegments
        )
    );

    Mockito.when(sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments())
           .thenReturn(druidDataSources);

    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    );

    SegmentReplicaCount zeroSegmentReplicaCount = Mockito.mock(SegmentReplicaCount.class);
    SegmentReplicaCount nonZeroSegmentReplicaCount = Mockito.mock(SegmentReplicaCount.class);
    Mockito.when(zeroSegmentReplicaCount.required()).thenReturn(0);
    Mockito.when(nonZeroSegmentReplicaCount.required()).thenReturn(1);
    SegmentReplicationStatus segmentReplicationStatus = Mockito.mock(SegmentReplicationStatus.class);
    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(coldSegmentAlpha.getId())))
           .thenReturn(zeroSegmentReplicaCount);
    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(coldSegmentBeta.getId())))
           .thenReturn(zeroSegmentReplicaCount);
    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(coldSegmentGamma.getId())))
           .thenReturn(zeroSegmentReplicaCount);

    Mockito.when(segmentReplicationStatus.getReplicaCountsInCluster(ArgumentMatchers.eq(hotSegmentGamma.getId())))
           .thenReturn(nonZeroSegmentReplicaCount);

    schema.updateSegmentReplicationStatus(segmentReplicationStatus);

    schema.coldDatasourceSchemaExec();
    // alpha has only 1 cold segment
    Assert.assertNotNull(schema.getDatasource("alpha"));
    // gamma has both hot and cold segment
    Assert.assertNotNull(schema.getDatasource("gamma"));
    // assert that cold schema for gamma doesn't contain any columns from hot segment
    RowSignature rowSignature = schema.getDatasource("gamma").getRowSignature();
    Assert.assertTrue(rowSignature.contains("dim1"));
    Assert.assertTrue(rowSignature.contains("c1"));
    Assert.assertTrue(rowSignature.contains("c2"));
    Assert.assertFalse(rowSignature.contains("c3"));
    Assert.assertFalse(rowSignature.contains("c4"));

    Assert.assertEquals(new HashSet<>(Arrays.asList("alpha", "gamma")), schema.getDataSourceInformationMap().keySet());

    druidDataSources.clear();
    druidDataSources.add(
        new ImmutableDruidDataSource(
            "beta",
            Collections.emptyMap(),
            Collections.singletonMap(coldSegmentBeta.getId(), coldSegmentBeta)
        )
    );

    druidDataSources.add(
        new ImmutableDruidDataSource(
            "gamma",
            Collections.emptyMap(),
            Collections.singletonMap(hotSegmentGamma.getId(), hotSegmentGamma)
        )
    );

    Mockito.when(sqlSegmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments())
           .thenReturn(druidDataSources);

    schema.coldDatasourceSchemaExec();
    Assert.assertNotNull(schema.getDatasource("beta"));
    // alpha doesn't have any segments
    Assert.assertNull(schema.getDatasource("alpha"));
    // gamma just has 1 hot segment
    Assert.assertNull(schema.getDatasource("gamma"));

    Assert.assertNull(schema.getDatasource("doesnotexist"));

    Assert.assertEquals(Collections.singleton("beta"), schema.getDataSourceInformationMap().keySet());
  }

  @Test
  public void testColdDatasourceSchemaExecRunsPeriodically() throws InterruptedException
  {
    // Make sure the thread runs more than once
    CountDownLatch latch = new CountDownLatch(2);

    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    ) {
      @Override
      long getColdSchemaExecPeriodMillis()
      {
        return 10;
      }

      @Override
      protected void coldDatasourceSchemaExec()
      {
        latch.countDown();
        super.coldDatasourceSchemaExec();
      }
    };

    schema.onLeaderStart();
    schema.awaitInitialization();

    latch.await(1, TimeUnit.SECONDS);
    Assert.assertEquals(0, latch.getCount());
  }

  @Test
  public void testTombstoneSegmentIsNotRefreshed() throws IOException
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

    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        factoryMock,
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        internalQueryConfig,
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    );

    Map<String, Object> queryContext = ImmutableMap.of(
        QueryContexts.PRIORITY_KEY, 5,
        QueryContexts.BROKER_PARALLEL_MERGE_KEY, false
    );

    DataSegment segment = newSegment("test", 0);
    DataSegment tombstone = DataSegment.builder()
                                       .dataSource("test")
                                       .interval(Intervals.of("2012-01-01/2012-01-02"))
                                       .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                       .shardSpec(new TombstoneShardSpec())
                                       .loadSpec(Collections.singletonMap(
                                           "type",
                                           DataSegment.TOMBSTONE_LOADSPEC_TYPE
                                       ))
                                       .size(0)
                                       .build();

    final DruidServer historicalServer = druidServers.stream()
                                                     .filter(s -> s.getType().equals(ServerType.HISTORICAL))
                                                     .findAny()
                                                     .orElse(null);

    Assert.assertNotNull(historicalServer);
    final DruidServerMetadata historicalServerMetadata = historicalServer.getMetadata();

    schema.addSegment(historicalServerMetadata, segment);
    schema.addSegment(historicalServerMetadata, tombstone);
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(tombstone.getId()));

    List<SegmentId> segmentIterable = ImmutableList.of(segment.getId(), tombstone.getId());

    SegmentMetadataQuery expectedMetadataQuery = new SegmentMetadataQuery(
        new TableDataSource(segment.getDataSource()),
        new MultipleSpecificSegmentSpec(
            segmentIterable.stream()
                           .filter(id -> !id.equals(tombstone.getId()))
                           .map(SegmentId::toDescriptor)
                           .collect(Collectors.toList())
        ),
        new AllColumnIncluderator(),
        false,
        queryContext,
        EnumSet.of(SegmentMetadataQuery.AnalysisType.AGGREGATORS),
        false,
        null,
        null
    );

    EasyMock.expect(factoryMock.factorize()).andReturn(lifecycleMock).once();
    EasyMock.expect(lifecycleMock.runSimple(expectedMetadataQuery, AllowAllAuthenticator.ALLOW_ALL_RESULT, Access.OK))
            .andReturn(QueryResponse.withEmptyContext(Sequences.empty())).once();

    EasyMock.replay(factoryMock, lifecycleMock);

    schema.refresh(Collections.singleton(segment.getId()), Collections.singleton("test"));

    // verify that metadata query is not issued for tombstone segment
    EasyMock.verify(factoryMock, lifecycleMock);

    // Verify that datasource schema building logic doesn't mark the tombstone segment for refresh
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(tombstone.getId()));

    AvailableSegmentMetadata availableSegmentMetadata = schema.getAvailableSegmentMetadata("test", tombstone.getId());
    Assert.assertNotNull(availableSegmentMetadata);
    // fetching metadata for tombstone segment shouldn't mark it for refresh
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(tombstone.getId()));

    Set<AvailableSegmentMetadata> metadatas = new HashSet<>();
    schema.iterateSegmentMetadata().forEachRemaining(metadatas::add);

    Assert.assertEquals(1, metadatas.stream().filter(metadata -> metadata.getSegment().isTombstone()).count());

    // iterating over entire metadata doesn't cause tombstone to be marked for refresh
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(tombstone.getId()));
  }

  @Test
  public void testUnusedSegmentIsNotRefreshed() throws InterruptedException, IOException
  {
    String dataSource = "xyz";
    CountDownLatch latch = new CountDownLatch(1);
    CoordinatorSegmentMetadataCache schema = new CoordinatorSegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        segmentSchemaCache,
        backFillQueue,
        sqlSegmentsMetadataManager,
        segmentsMetadataManagerConfigSupplier
    ) {
      @Override
      public void refresh(Set<SegmentId> segmentsToRefresh, Set<String> dataSourcesToRebuild)
          throws IOException
      {
        super.refresh(segmentsToRefresh, dataSourcesToRebuild);
        latch.countDown();
      }
    };

    List<DataSegment> segments = ImmutableList.of(
        newSegment(dataSource, 1),
        newSegment(dataSource, 2),
        newSegment(dataSource, 3)
    );

    final DruidServer historicalServer = druidServers.stream()
                                                     .filter(s -> s.getType().equals(ServerType.HISTORICAL))
                                                     .findAny()
                                                     .orElse(null);

    Assert.assertNotNull(historicalServer);
    final DruidServerMetadata historicalServerMetadata = historicalServer.getMetadata();

    ImmutableMap.Builder<SegmentId, SegmentMetadata> segmentStatsMap = new ImmutableMap.Builder<>();
    segmentStatsMap.put(segments.get(0).getId(), new SegmentMetadata(20L, "fp"));
    segmentStatsMap.put(segments.get(1).getId(), new SegmentMetadata(20L, "fp"));
    segmentStatsMap.put(segments.get(2).getId(), new SegmentMetadata(20L, "fp"));

    ImmutableMap.Builder<String, SchemaPayload> schemaPayloadMap = new ImmutableMap.Builder<>();
    schemaPayloadMap.put("fp", new SchemaPayload(RowSignature.builder().add("c1", ColumnType.DOUBLE).build()));
    segmentSchemaCache.updateFinalizedSegmentSchema(
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentStatsMap.build(), schemaPayloadMap.build())
    );

    schema.addSegment(historicalServerMetadata, segments.get(0));
    schema.addSegment(historicalServerMetadata, segments.get(1));
    schema.addSegment(historicalServerMetadata, segments.get(2));

    serverView.addSegment(segments.get(0), ServerType.HISTORICAL);
    serverView.addSegment(segments.get(1), ServerType.HISTORICAL);
    serverView.addSegment(segments.get(2), ServerType.HISTORICAL);

    schema.onLeaderStart();
    schema.awaitInitialization();

    Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));

    // make segment3 unused
    segmentStatsMap = new ImmutableMap.Builder<>();
    segmentStatsMap.put(segments.get(0).getId(), new SegmentMetadata(20L, "fp"));

    segmentSchemaCache.updateFinalizedSegmentSchema(
        new SegmentSchemaCache.FinalizedSegmentSchemaInfo(segmentStatsMap.build(), schemaPayloadMap.build())
    );

    Map<SegmentId, DataSegment> segmentMap = new HashMap<>();
    segmentMap.put(segments.get(0).getId(), segments.get(0));
    segmentMap.put(segments.get(1).getId(), segments.get(1));

    ImmutableDruidDataSource druidDataSource =
        new ImmutableDruidDataSource(
            "xyz",
            Collections.emptyMap(),
            segmentMap
        );

    Mockito.when(sqlSegmentsMetadataManager.getImmutableDataSourceWithUsedSegments(ArgumentMatchers.anyString()))
           .thenReturn(druidDataSource);

    Set<SegmentId> segmentsToRefresh = segments.stream().map(DataSegment::getId).collect(Collectors.toSet());
    segmentsToRefresh.remove(segments.get(1).getId());
    segmentsToRefresh.remove(segments.get(2).getId());

    schema.refresh(segmentsToRefresh, Sets.newHashSet(dataSource));

    Assert.assertTrue(schema.getSegmentsNeedingRefresh().contains(segments.get(1).getId()));
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(segments.get(2).getId()));

    AvailableSegmentMetadata availableSegmentMetadata =
        schema.getAvailableSegmentMetadata(dataSource, segments.get(0).getId());

    Assert.assertNotNull(availableSegmentMetadata);
    // fetching metadata for unused segment shouldn't mark it for refresh
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(segments.get(0).getId()));

    Set<AvailableSegmentMetadata> metadatas = new HashSet<>();
    schema.iterateSegmentMetadata().forEachRemaining(metadatas::add);

    Assert.assertEquals(
        1,
        metadatas.stream()
                 .filter(
                     metadata ->
                         metadata.getSegment().getId().equals(segments.get(0).getId())).count()
    );

    // iterating over entire metadata doesn't cause unsed segment to be marked for refresh
    Assert.assertFalse(schema.getSegmentsNeedingRefresh().contains(segments.get(0).getId()));
  }

  private void verifyFooDSSchema(CoordinatorSegmentMetadataCache schema, int columns)
  {
    final DataSourceInformation fooDs = schema.getDatasource("foo");
    final RowSignature fooRowSignature = fooDs.getRowSignature();
    List<String> columnNames = fooRowSignature.getColumnNames();
    Assert.assertEquals(columns, columnNames.size());

    Assert.assertEquals("__time", columnNames.get(0));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("dim2", columnNames.get(1));
    Assert.assertEquals(ColumnType.STRING, fooRowSignature.getColumnType(columnNames.get(1)).get());

    Assert.assertEquals("m1", columnNames.get(2));
    Assert.assertEquals(ColumnType.DOUBLE, fooRowSignature.getColumnType(columnNames.get(2)).get());

    Assert.assertEquals("dim1", columnNames.get(3));
    Assert.assertEquals(ColumnType.STRING, fooRowSignature.getColumnType(columnNames.get(3)).get());

    Assert.assertEquals("cnt", columnNames.get(4));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(4)).get());

    Assert.assertEquals("unique_dim1", columnNames.get(5));
    Assert.assertEquals(ColumnType.ofComplex("hyperUnique"), fooRowSignature.getColumnType(columnNames.get(5)).get());
  }

  private void verifyFoo2DSSchema(CoordinatorSegmentMetadataCache schema)
  {
    final DataSourceInformation fooDs = schema.getDatasource("foo2");
    final RowSignature fooRowSignature = fooDs.getRowSignature();
    List<String> columnNames = fooRowSignature.getColumnNames();
    Assert.assertEquals(3, columnNames.size());

    Assert.assertEquals("__time", columnNames.get(0));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(0)).get());

    Assert.assertEquals("dim2", columnNames.get(1));
    Assert.assertEquals(ColumnType.STRING, fooRowSignature.getColumnType(columnNames.get(1)).get());

    Assert.assertEquals("m1", columnNames.get(2));
    Assert.assertEquals(ColumnType.LONG, fooRowSignature.getColumnType(columnNames.get(2)).get());
  }
}
