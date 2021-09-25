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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestServerInventoryView;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DruidSchemaTest extends DruidSchemaTestCommon
{
  private SpecificSegmentsQuerySegmentWalker walker = null;
  private TestServerInventoryView serverView;
  private List<ImmutableDruidServer> druidServers;
  private DruidSchema schema = null;
  private DruidSchema schema2 = null;
  private CountDownLatch buildTableLatch = new CountDownLatch(1);
  private CountDownLatch markDataSourceLatch = new CountDownLatch(1);

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

    schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(globalTableJoinable), ImmutableMap.of(globalTableJoinable.getClass(), GlobalTableDataSource.class)),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      protected DruidTable buildDruidTable(String dataSource)
      {
        DruidTable table = super.buildDruidTable(dataSource);
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

    schema2 = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(globalTableJoinable), ImmutableMap.of(globalTableJoinable.getClass(), GlobalTableDataSource.class)),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {

      boolean throwException = true;
      @Override
      protected DruidTable buildDruidTable(String dataSource)
      {
        DruidTable table = super.buildDruidTable(dataSource);
        buildTableLatch.countDown();
        return table;
      }

      @Override
      Set<SegmentId> refreshSegments(final Set<SegmentId> segments) throws IOException
      {
        if (throwException) {
          throwException = false;
          throw new RuntimeException("Query[xxxx] url[http://xxxx:8083/druid/v2/] timed out.");
        } else {
          return super.refreshSegments(segments);
        }
      }

      @Override
      void markDataSourceAsNeedRebuild(String datasource)
      {
        super.markDataSourceAsNeedRebuild(datasource);
        markDataSourceLatch.countDown();
      }
    };

    schema.start();
    schema.awaitInitialization();
  }

  @After
  public void tearDown() throws Exception
  {
    schema.stop();
    walker.close();
  }

  @Test
  public void testGetTableMap()
  {
    Assert.assertEquals(ImmutableSet.of("foo", "foo2"), schema.getTableNames());

    final Map<String, Table> tableMap = schema.getTableMap();
    Assert.assertEquals(ImmutableSet.of("foo", "foo2"), tableMap.keySet());
  }

  @Test
  public void testSchemaInit() throws InterruptedException
  {
    schema2.start();
    schema2.awaitInitialization();
    Map<String, Table> tableMap = schema2.getTableMap();
    Assert.assertEquals(2, tableMap.size());
    Assert.assertTrue(tableMap.containsKey("foo"));
    Assert.assertTrue(tableMap.containsKey("foo2"));
    schema2.stop();
  }


  @Test
  public void testGetTableMapFoo()
  {
    final DruidTable fooTable = (DruidTable) schema.getTableMap().get("foo");
    final RelDataType rowType = fooTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(6, fields.size());

    Assert.assertEquals("__time", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.TIMESTAMP, fields.get(0).getType().getSqlTypeName());

    Assert.assertEquals("cnt", fields.get(1).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(1).getType().getSqlTypeName());

    Assert.assertEquals("dim1", fields.get(2).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(2).getType().getSqlTypeName());

    Assert.assertEquals("dim2", fields.get(3).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(3).getType().getSqlTypeName());

    Assert.assertEquals("m1", fields.get(4).getName());
    Assert.assertEquals(SqlTypeName.BIGINT, fields.get(4).getType().getSqlTypeName());

    Assert.assertEquals("unique_dim1", fields.get(5).getName());
    Assert.assertEquals(SqlTypeName.OTHER, fields.get(5).getType().getSqlTypeName());
  }

  @Test
  public void testGetTableMapFoo2()
  {
    final DruidTable fooTable = (DruidTable) schema.getTableMap().get("foo2");
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

  /**
   * This tests that {@link AvailableSegmentMetadata#getNumRows()} is correct in case
   * of multiple replicas i.e. when {@link DruidSchema#addSegment(DruidServerMetadata, DataSegment)}
   * is called more than once for same segment
   */
  @Test
  public void testAvailableSegmentMetadataNumRows()
  {
    Map<SegmentId, AvailableSegmentMetadata> segmentsMetadata = schema.getSegmentMetadataSnapshot();
    final List<DataSegment> segments = segmentsMetadata.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(4, segments.size());
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
    // invoke DruidSchema#addSegment on existingSegment
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
  public void testNullDatasource() throws IOException
  {
    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadatas = schema.getSegmentMetadataSnapshot();
    final List<DataSegment> segments = segmentMetadatas.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(4, segments.size());
    // segments contains two segments with datasource "foo" and one with datasource "foo2"
    // let's remove the only segment with datasource "foo2"
    final DataSegment segmentToRemove = segments.stream()
                                                .filter(segment -> segment.getDataSource().equals("foo2"))
                                                .findFirst()
                                                .orElse(null);
    Assert.assertNotNull(segmentToRemove);
    schema.removeSegment(segmentToRemove);

    // The following line can cause NPE without segmentMetadata null check in DruidSchema#refreshSegmentsForDataSource
    schema.refreshSegments(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()));
    Assert.assertEquals(3, schema.getSegmentMetadataSnapshot().size());
  }

  @Test
  public void testNullAvailableSegmentMetadata() throws IOException
  {
    final Map<SegmentId, AvailableSegmentMetadata> segmentMetadatas = schema.getSegmentMetadataSnapshot();
    final List<DataSegment> segments = segmentMetadatas.values()
                                                       .stream()
                                                       .map(AvailableSegmentMetadata::getSegment)
                                                       .collect(Collectors.toList());
    Assert.assertEquals(4, segments.size());
    // remove one of the segments with datasource "foo"
    final DataSegment segmentToRemove = segments.stream()
                                                .filter(segment -> segment.getDataSource().equals("foo"))
                                                .findFirst()
                                                .orElse(null);
    Assert.assertNotNull(segmentToRemove);
    schema.removeSegment(segmentToRemove);

    // The following line can cause NPE without segmentMetadata null check in DruidSchema#refreshSegmentsForDataSource
    schema.refreshSegments(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()));
    Assert.assertEquals(3, schema.getSegmentMetadataSnapshot().size());
  }

  @Test
  public void testAvailableSegmentMetadataIsRealtime()
  {
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
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.HISTORICAL);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(5, schema.getTotalSegments());
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
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
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

    Assert.assertEquals(5, schema.getTotalSegments());
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
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.REALTIME);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(5, schema.getTotalSegments());
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
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
      {
        super.addSegment(server, segment);
        if (datasource.equals(segment.getDataSource())) {
          addSegmentLatch.countDown();
        }
      }
    };

    serverView.addSegment(newSegment(datasource, 1), ServerType.BROKER);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));

    Assert.assertEquals(4, schema.getTotalSegments());
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
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
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

    Assert.assertEquals(4, schema.getTotalSegments());
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
    Assert.assertFalse(schema.getTableNames().contains(datasource));
  }

  @Test
  public void testSegmentRemovedCallbackNonEmptyDataSourceAfterRemove() throws InterruptedException, IOException
  {
    String datasource = "segmentRemoveTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(2);
    CountDownLatch removeSegmentLatch = new CountDownLatch(1);
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
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

    Assert.assertEquals(5, schema.getTotalSegments());
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
    Assert.assertTrue(schema.getTableNames().contains(datasource));
  }

  @Test
  public void testServerSegmentRemovedCallbackRemoveUnknownSegment() throws InterruptedException
  {
    String datasource = "serverSegmentRemoveTest";
    CountDownLatch removeServerSegmentLatch = new CountDownLatch(1);
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
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

    Assert.assertEquals(4, schema.getTotalSegments());
  }

  @Test
  public void testServerSegmentRemovedCallbackRemoveBrokerSegment() throws InterruptedException
  {
    String datasource = "serverSegmentRemoveTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    CountDownLatch removeServerSegmentLatch = new CountDownLatch(1);
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
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

    Assert.assertEquals(5, schema.getTotalSegments());
    Assert.assertTrue(schema.getDataSourcesNeedingRebuild().contains(datasource));
  }

  @Test
  public void testServerSegmentRemovedCallbackRemoveHistoricalSegment() throws InterruptedException
  {
    String datasource = "serverSegmentRemoveTest";
    CountDownLatch addSegmentLatch = new CountDownLatch(1);
    CountDownLatch removeServerSegmentLatch = new CountDownLatch(1);
    DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        serverView,
        segmentManager,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator()
    )
    {
      @Override
      void addSegment(final DruidServerMetadata server, final DataSegment segment)
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

    Assert.assertEquals(5, schema.getTotalSegments());
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

  @Test
  public void testLocalSegmentCacheSetsDataSourceAsGlobalAndJoinable() throws InterruptedException
  {
    DruidTable fooTable = (DruidTable) schema.getTableMap().get("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.getDataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.getDataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isJoinable());
    Assert.assertFalse(fooTable.isBroadcast());

    Assert.assertTrue(buildTableLatch.await(1, TimeUnit.SECONDS));

    buildTableLatch = new CountDownLatch(1);
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
    Assert.assertTrue(markDataSourceLatch.await(2, TimeUnit.SECONDS));
    // wait for build twice
    Assert.assertTrue(buildTableLatch.await(2, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    Assert.assertTrue(getDatasourcesLatch.await(2, TimeUnit.SECONDS));

    fooTable = (DruidTable) schema.getTableMap().get("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.getDataSource() instanceof TableDataSource);
    Assert.assertTrue(fooTable.getDataSource() instanceof GlobalTableDataSource);
    Assert.assertTrue(fooTable.isJoinable());
    Assert.assertTrue(fooTable.isBroadcast());

    // now remove it
    markDataSourceLatch = new CountDownLatch(1);
    buildTableLatch = new CountDownLatch(1);
    getDatasourcesLatch = new CountDownLatch(1);
    joinableDataSourceNames.remove("foo");
    segmentDataSourceNames.remove("foo");
    serverView.removeSegment(someNewBrokerSegment, ServerType.BROKER);

    Assert.assertTrue(markDataSourceLatch.await(2, TimeUnit.SECONDS));
    // wait for build
    Assert.assertTrue(buildTableLatch.await(2, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    Assert.assertTrue(getDatasourcesLatch.await(2, TimeUnit.SECONDS));

    fooTable = (DruidTable) schema.getTableMap().get("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.getDataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.getDataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isJoinable());
    Assert.assertFalse(fooTable.isBroadcast());
  }

  @Test
  public void testLocalSegmentCacheSetsDataSourceAsBroadcastButNotJoinable() throws InterruptedException
  {
    DruidTable fooTable = (DruidTable) schema.getTableMap().get("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.getDataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.getDataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isJoinable());
    Assert.assertFalse(fooTable.isBroadcast());

    // wait for build twice
    Assert.assertTrue(buildTableLatch.await(1, TimeUnit.SECONDS));

    buildTableLatch = new CountDownLatch(1);
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

    Assert.assertTrue(markDataSourceLatch.await(2, TimeUnit.SECONDS));
    Assert.assertTrue(buildTableLatch.await(2, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    Assert.assertTrue(getDatasourcesLatch.await(2, TimeUnit.SECONDS));

    fooTable = (DruidTable) schema.getTableMap().get("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.getDataSource() instanceof TableDataSource);
    // should not be a GlobalTableDataSource for now, because isGlobal is couple with joinability. idealy this will be
    // changed in the future and we should expect
    Assert.assertFalse(fooTable.getDataSource() instanceof GlobalTableDataSource);
    Assert.assertTrue(fooTable.isBroadcast());
    Assert.assertFalse(fooTable.isJoinable());

    // now remove it
    markDataSourceLatch = new CountDownLatch(1);
    buildTableLatch = new CountDownLatch(1);
    getDatasourcesLatch = new CountDownLatch(1);
    segmentDataSourceNames.remove("foo");
    serverView.removeSegment(someNewBrokerSegment, ServerType.BROKER);

    Assert.assertTrue(markDataSourceLatch.await(2, TimeUnit.SECONDS));
    // wait for build
    Assert.assertTrue(buildTableLatch.await(2, TimeUnit.SECONDS));
    // wait for get again, just to make sure table has been updated (latch counts down just before tables are updated)
    Assert.assertTrue(getDatasourcesLatch.await(2, TimeUnit.SECONDS));

    fooTable = (DruidTable) schema.getTableMap().get("foo");
    Assert.assertNotNull(fooTable);
    Assert.assertTrue(fooTable.getDataSource() instanceof TableDataSource);
    Assert.assertFalse(fooTable.getDataSource() instanceof GlobalTableDataSource);
    Assert.assertFalse(fooTable.isBroadcast());
    Assert.assertFalse(fooTable.isJoinable());
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
