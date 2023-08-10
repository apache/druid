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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.BrokerInternalQueryConfig;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
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
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.metadata.metadata.AllColumnIncluderator;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.MapJoinableFactory;
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
import org.apache.druid.sql.calcite.planner.SegmentMetadataCacheConfig;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.sql.calcite.util.TestServerInventoryView;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SegmentMetadataCacheTest extends SegmentMetadataCacheCommon
{
  // Timeout to allow (rapid) debugging, while not blocking tests with errors.
  private static final int WAIT_TIMEOUT_SECS = 6;

  private SpecificSegmentsQuerySegmentWalker walker;
  private TestServerInventoryView serverView;
  private List<ImmutableDruidServer> druidServers;
  private SegmentMetadataCache runningSchema;
  private CountDownLatch buildTableLatch = new CountDownLatch(1);
  private CountDownLatch markDataSourceLatch = new CountDownLatch(1);
  private CountDownLatch refreshLatch = new CountDownLatch(1);
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Before
  public void setUp() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    final QueryableIndex index1 = IndexBuilder.create()
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
                                              .rows(ROWS1)
                                              .buildMMappedIndex();

    final QueryableIndex index2 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "2"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withMetrics(new LongSumAggregatorFactory("m1", "m1"))
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(ROWS2)
                                              .buildMMappedIndex();

    final InputRowSchema rowSchema = new InputRowSchema(
        new TimestampSpec("t", null, null),
        DimensionsSpec.builder().useSchemaDiscovery(true).build(),
        null
    );
    final List<InputRow> autoRows1 = ImmutableList.of(
        TestDataBuilder.createRow(
            ImmutableMap.<String, Object>builder()
                        .put("t", "2023-01-01T00:00Z")
                        .put("numbery", 1.1f)
                        .put("numberyArrays", ImmutableList.of(1L, 2L, 3L))
                        .put("stringy", ImmutableList.of("a", "b", "c"))
                        .put("array", ImmutableList.of(1.1, 2.2, 3.3))
                        .put("nested", ImmutableMap.of("x", 1L, "y", 2L))
                        .build(),
            rowSchema
        )
    );
    final List<InputRow> autoRows2 = ImmutableList.of(
        TestDataBuilder.createRow(
            ImmutableMap.<String, Object>builder()
                        .put("t", "2023-01-02T00:00Z")
                        .put("numbery", 1L)
                        .put("numberyArrays", ImmutableList.of(3.3, 2.2, 3.1))
                        .put("stringy", "a")
                        .put("array", ImmutableList.of(1L, 2L, 3L))
                        .put("nested", "hello")
                        .build(),
            rowSchema
        )
    );
    final QueryableIndex indexAuto1 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "1"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withTimestampSpec(rowSchema.getTimestampSpec())
                                                      .withDimensionsSpec(rowSchema.getDimensionsSpec())
                                                      .withMetrics(
                                                          new CountAggregatorFactory("cnt"),
                                                          new DoubleSumAggregatorFactory("m1", "m1"),
                                                          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                                      )
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(autoRows1)
                                              .buildMMappedIndex();

    final QueryableIndex indexAuto2 = IndexBuilder.create()
                                                  .tmpDir(new File(tmpDir, "1"))
                                                  .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                                  .schema(
                                                      new IncrementalIndexSchema.Builder()
                                                          .withTimestampSpec(
                                                              new TimestampSpec("t", null, null)
                                                          )
                                                          .withDimensionsSpec(
                                                              DimensionsSpec.builder().useSchemaDiscovery(true).build()
                                                          )
                                                          .withMetrics(
                                                              new CountAggregatorFactory("cnt"),
                                                              new DoubleSumAggregatorFactory("m1", "m1"),
                                                              new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                                          )
                                                          .withRollup(false)
                                                          .build()
                                                  )
                                                  .rows(autoRows2)
                                                  .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE1)
                   .interval(Intervals.of("2000/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE1)
                   .interval(Intervals.of("2001/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.SOME_DATASOURCE)
                   .interval(Intervals.of("2023-01-01T00Z/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(0)
                   .build(),
        indexAuto1
    ).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.SOME_DATASOURCE)
                   .interval(Intervals.of("2023-01-02T00Z/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(0)
                   .build(),
        indexAuto2
    );
    final DataSegment segment1 = new DataSegment(
        "foo3",
        Intervals.of("2012/2013"),
        "version3",
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        new NumberedShardSpec(2, 3),
        null,
        1,
        100L,
        PruneSpecsHolder.DEFAULT
    );
    final List<DataSegment> realtimeSegments = ImmutableList.of(segment1);
    serverView = new TestServerInventoryView(walker.getSegments(), realtimeSegments);
    druidServers = serverView.getDruidServers();
  }

  public SegmentMetadataCache buildSchemaMarkAndTableLatch() throws InterruptedException
  {
    return buildSchemaMarkAndTableLatch(SEGMENT_CACHE_CONFIG_DEFAULT);
  }

  public SegmentMetadataCache buildSchemaMarkAndTableLatch(SegmentMetadataCacheConfig config) throws InterruptedException
  {
    Preconditions.checkState(runningSchema == null);
    runningSchema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(
            ImmutableSet.of(globalTableJoinable),
            ImmutableMap.of(globalTableJoinable.getClass(), GlobalTableDataSource.class)
        ),
        config,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected DatasourceTable.PhysicalDatasourceMetadata buildDruidTable(String dataSource)
      {
        DatasourceTable.PhysicalDatasourceMetadata table = super.buildDruidTable(dataSource);
        buildTableLatch.countDown();
        return table;
      }

      @Override
      void markDataSourceAsNeedRebuild(String datasource)
      {
        super.markDataSourceAsNeedRebuild(datasource);
        markDataSourceLatch.countDown();
      }
    };

    runningSchema.start();
    runningSchema.awaitInitialization();
    return runningSchema;
  }

  public SegmentMetadataCache buildSchemaMarkAndRefreshLatch() throws InterruptedException
  {
    Preconditions.checkState(runningSchema == null);
    runningSchema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(
            ImmutableSet.of(globalTableJoinable),
            ImmutableMap.of(globalTableJoinable.getClass(), GlobalTableDataSource.class)
        ),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
        )
    {
      @Override
      void markDataSourceAsNeedRebuild(String datasource)
      {
        super.markDataSourceAsNeedRebuild(datasource);
        markDataSourceLatch.countDown();
      }

      @Override
      @VisibleForTesting
      void refresh(final Set<SegmentId> segmentsToRefresh, final Set<String> dataSourcesToRebuild) throws IOException
      {
        super.refresh(segmentsToRefresh, dataSourcesToRebuild);
        refreshLatch.countDown();
      }
    };

    runningSchema.start();
    runningSchema.awaitInitialization();
    return runningSchema;
  }

  @After
  public void tearDown() throws Exception
  {
    if (runningSchema != null) {
      runningSchema.stop();
    }
    walker.close();
  }

  @Test
  public void testGetTableMap() throws InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    Assert.assertEquals(ImmutableSet.of(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE2, CalciteTests.SOME_DATASOURCE), schema.getDatasourceNames());

    final Set<String> tableNames = schema.getDatasourceNames();
    Assert.assertEquals(ImmutableSet.of(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE2, CalciteTests.SOME_DATASOURCE), tableNames);
  }

  @Test
  public void testSchemaInit() throws InterruptedException
  {
    SegmentMetadataCache schema2 = buildSchemaMarkAndTableLatch();
    Assert.assertEquals(ImmutableSet.of(CalciteTests.DATASOURCE1, CalciteTests.DATASOURCE2, CalciteTests.SOME_DATASOURCE), schema2.getDatasourceNames());
  }

  @Test
  public void testGetTableMapFoo() throws InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch(
        new SegmentMetadataCacheConfig() {
          @Override
          public SegmentMetadataCache.ColumnTypeMergePolicy getMetadataColumnTypeMergePolicy()
          {
            return new SegmentMetadataCache.FirstTypeMergePolicy();
          }
        }
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
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
   * of multiple replicas i.e. when {@link SegmentMetadataCache#addSegment(DruidServerMetadata, DataSegment)}
   * is called more than once for same segment
   * @throws InterruptedException
   */
  @Test
  public void testAvailableSegmentMetadataNumRows() throws InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
  public void testNullAvailableSegmentMetadata() throws IOException, InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    final ImmutableDruidServer historicalServer = druidServers.stream()
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

    ImmutableDruidServer realtimeServer = druidServers.stream()
                                                      .filter(s -> s.getType().equals(ServerType.REALTIME))
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    DataSegment segment = newSegment(datasource, 1);
    serverView.addSegment(segment, ServerType.REALTIME);
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.REALTIME);
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      void removeSegment(final DataSegment segment)
      {
        super.removeSegment(segment);
        if (datasource.equals(segment.getDataSource())) {
          removeSegmentLatch.countDown();
        }
      }
    };

    DataSegment segment = newSegment(datasource, 1);
    serverView.addSegment(segment, ServerType.REALTIME);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));
    schema.refresh(Sets.newHashSet(segment.getId()), Sets.newHashSet(datasource));

    serverView.removeSegment(segment, ServerType.REALTIME);
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      void removeSegment(final DataSegment segment)
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
    serverView.addSegment(segments.get(0), ServerType.REALTIME);
    serverView.addSegment(segments.get(1), ServerType.HISTORICAL);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));
    schema.refresh(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()), Sets.newHashSet(datasource));

    serverView.removeSegment(segments.get(0), ServerType.REALTIME);
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      void removeServerSegment(final DruidServerMetadata server, final DataSegment segment)
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
    SegmentMetadataCache schema3 = buildSchemaMarkAndRefreshLatch();
    Assert.assertTrue(refreshLatch.await(WAIT_TIMEOUT_SECS, TimeUnit.SECONDS));
    DatasourceTable.PhysicalDatasourceMetadata fooTable = schema3.getDatasource("foo");
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
        PruneSpecsHolder.DEFAULT
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

    fooTable = schema3.getDatasource("foo");
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

    fooTable = schema3.getDatasource("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.dataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.dataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isJoinable());
    Assert.assertFalse(fooTable.isBroadcast());
  }

  @Test
  public void testLocalSegmentCacheSetsDataSourceAsBroadcastButNotJoinable() throws InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndRefreshLatch();
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
        PruneSpecsHolder.DEFAULT
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
    Map<String, Object> queryContext = ImmutableMap.of(
        QueryContexts.PRIORITY_KEY, 5,
        QueryContexts.BROKER_PARALLEL_MERGE_KEY, false
    );

    String brokerInternalQueryConfigJson = "{\"context\": { \"priority\": 5} }";

    TestHelper.makeJsonMapper();
    BrokerInternalQueryConfig brokerInternalQueryConfig = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(brokerInternalQueryConfigJson, BrokerInternalQueryConfig.class)
        ),
        BrokerInternalQueryConfig.class
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

    QueryLifecycleFactory factoryMock = EasyMock.createMock(QueryLifecycleFactory.class);
    QueryLifecycle lifecycleMock = EasyMock.createMock(QueryLifecycle.class);

    // Need to create schema for this test because the available schemas don't mock the QueryLifecycleFactory, which I need for this test.
    SegmentMetadataCache mySchema = new SegmentMetadataCache(
        factoryMock,
        serverView,
        segmentManager,
        new MapJoinableFactory(
            ImmutableSet.of(globalTableJoinable),
            ImmutableMap.of(globalTableJoinable.getClass(), GlobalTableDataSource.class)
        ),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        brokerInternalQueryConfig,
        new NoopServiceEmitter()
    );

    EasyMock.expect(factoryMock.factorize()).andReturn(lifecycleMock).once();
    // This is the mat of the test, making sure that the query created by the method under test matches the expected query, specifically the operator configured context
    EasyMock.expect(lifecycleMock.runSimple(expectedMetadataQuery, AllowAllAuthenticator.ALLOW_ALL_RESULT, Access.OK))
            .andReturn(QueryResponse.withEmptyContext(Sequences.empty()));

    EasyMock.replay(factoryMock, lifecycleMock);

    mySchema.runSegmentMetadataQuery(segmentIterable);

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

    RowSignature signature = SegmentMetadataCache.analysisToRowSignature(
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
    RowSignature signature = SegmentMetadataCache.analysisToRowSignature(
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
                        26,
                        "a",
                        "z",
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
        RowSignature.builder().add("a", ColumnType.STRING).add("count", ColumnType.LONG).build(),
        signature
    );
  }

  @Test
  public void testStaleDatasourceRefresh() throws IOException, InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    String datasource = "xyz";
    CountDownLatch addSegmentLatch = new CountDownLatch(2);
    StubServiceEmitter emitter = new StubServiceEmitter("broker", "host");
    SegmentMetadataCache schema = new SegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig(),
        emitter
    )
    {
      @Override
      protected void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }

      @Override
      void removeSegment(final DataSegment segment)
      {
        super.removeSegment(segment);
      }
    };

    List<DataSegment> segments = ImmutableList.of(
        newSegment(datasource, 1),
        newSegment(datasource, 2)
    );
    serverView.addSegment(segments.get(0), ServerType.HISTORICAL);
    serverView.addSegment(segments.get(1), ServerType.REALTIME);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));
    schema.refresh(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()), Sets.newHashSet(datasource));

    emitter.verifyEmitted("metadatacache/refresh/time", ImmutableMap.of(DruidMetrics.DATASOURCE, datasource), 1);
    emitter.verifyEmitted("metadatacache/refresh/count", ImmutableMap.of(DruidMetrics.DATASOURCE, datasource), 1);
  }

  private static DataSegment newSegment(String datasource, int partitionId)
  {
    return new DataSegment(
        datasource,
        Intervals.of("2012/2013"),
        "version1",
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        new NumberedShardSpec(partitionId, 0),
        null,
        1,
        100L,
        PruneSpecsHolder.DEFAULT
    );
  }
}
