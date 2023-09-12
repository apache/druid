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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  static final SegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = SegmentMetadataCacheConfig.create("PT1S");
  private SegmentMetadataCache runningSchema;
  private CountDownLatch buildTableLatch = new CountDownLatch(1);
  private CountDownLatch markDataSourceLatch = new CountDownLatch(1);

  @Before
  public void setup() throws Exception
  {
    setUpCommon();
    setupData();
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

  public SegmentMetadataCache buildSchemaMarkAndTableLatch() throws InterruptedException
  {
    return buildSchemaMarkAndTableLatch(SEGMENT_CACHE_CONFIG_DEFAULT);
  }

  public SegmentMetadataCache buildSchemaMarkAndTableLatch(SegmentMetadataCacheConfig config) throws InterruptedException
  {
    Preconditions.checkState(runningSchema == null);
    runningSchema = new SegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        config,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
    )
    {
      @Override
      public DataSourceInformation buildDruidTable(String dataSource)
      {
        DataSourceInformation table = super.buildDruidTable(dataSource);
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

  @Test
  public void testGetTableMap() throws InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    Assert.assertEquals(ImmutableSet.of(DATASOURCE1, DATASOURCE2, SOME_DATASOURCE), schema.getDatasourceNames());

    final Set<String> tableNames = schema.getDatasourceNames();
    Assert.assertEquals(ImmutableSet.of(DATASOURCE1, DATASOURCE2, SOME_DATASOURCE), tableNames);
  }

  @Test
  public void testGetTableMapFoo() throws InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    final DataSourceInformation fooDs = schema.getDatasource("foo");
    final RowSignature fooRowSignature = fooDs.getRowSignature();
    List<String> columnNames = fooRowSignature.getColumnNames();
    Assert.assertEquals(6, columnNames.size());

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

  @Test
  public void testGetTableMapFoo2() throws InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
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
    checkAvailableSegmentMetadataNumRows(schema);
  }

  @Test
  public void testNullDatasource() throws IOException, InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    checkNullDatasource(schema);
  }

  @Test
  public void testNullAvailableSegmentMetadata() throws IOException, InterruptedException
  {
    SegmentMetadataCache schema = buildSchemaMarkAndTableLatch();
    checkNullAvailableSegmentMetadata(schema);
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
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
    SegmentMetadataCache schema = new SegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter()
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
    SegmentMetadataCache mySchema = new SegmentMetadataCache(
        factoryMock,
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        internalQueryConfig,
        new NoopServiceEmitter()
    );

    checkRunSegmentMetadataQueryWithContext(mySchema, factoryMock, lifecycleMock);
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
    checkStaleDatasourceRefresh(schema);
  }

  @Test
  public void testRefreshShouldEmitMetrics() throws InterruptedException, IOException
  {
    String dataSource = "xyz";
    CountDownLatch addSegmentLatch = new CountDownLatch(2);
    StubServiceEmitter emitter = new StubServiceEmitter("broker", "host");
    SegmentMetadataCache schema = new SegmentMetadataCache(
        getQueryLifecycleFactory(walker),
        serverView,
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        emitter
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

    checkRefreshShouldEmitMetrics(schema, dataSource, emitter, addSegmentLatch);
  }
}
