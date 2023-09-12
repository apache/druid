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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.metadata.metadata.AllColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class SegmentMetadataCacheCommon
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "numfoo";
  public static final String DATASOURCE4 = "foo4";
  public static final String DATASOURCE5 = "lotsocolumns";
  public static final String BROADCAST_DATASOURCE = "broadcast";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";
  public static final String SOME_DATASOURCE = "some_datasource";
  public static final String TIMESTAMP_COLUMN = "t";
  private static final InputRowSchema FOO_SCHEMA = new InputRowSchema(
      new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3"))
      ),
      null
  );

  final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  final List<InputRow> ROWS2 = ImmutableList.of(
      createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  public QueryRunnerFactoryConglomerate conglomerate;
  public Closer resourceCloser;
  public QueryToolChestWarehouse queryToolChestWarehouse;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public SpecificSegmentsQuerySegmentWalker walker;
  public TestTimelineServerView serverView;
  public List<ImmutableDruidServer> druidServers;

  public QueryableIndex index1;
  public QueryableIndex index2;

  public QueryableIndex indexAuto1;
  public QueryableIndex indexAuto2;
  public DataSegment realtimeSegment1;

  public void setUpCommon()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    queryToolChestWarehouse = new QueryToolChestWarehouse()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
      {
        return conglomerate.findFactory(query).getToolchest();
      }
    };
  }

  public void setupData() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    index1 = IndexBuilder.create()
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

    index2 = IndexBuilder.create()
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
        createRow(
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
        createRow(
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

    indexAuto1 = IndexBuilder.create()
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

    indexAuto2 = IndexBuilder.create()
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
                   .dataSource(DATASOURCE1)
                   .interval(Intervals.of("2000/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(Intervals.of("2001/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(SOME_DATASOURCE)
                   .interval(Intervals.of("2023-01-01T00Z/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(0)
                   .build(),
        indexAuto1
    ).add(
        DataSegment.builder()
                   .dataSource(SOME_DATASOURCE)
                   .interval(Intervals.of("2023-01-02T00Z/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(0)
                   .build(),
        indexAuto2
    );
    realtimeSegment1 = new DataSegment(
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
        DataSegment.PruneSpecsHolder.DEFAULT
    );

    final List<DataSegment> realtimeSegments = ImmutableList.of(realtimeSegment1);
    serverView = new TestTimelineServerView(walker.getSegments(), realtimeSegments);
    druidServers = serverView.getDruidServers();
  }

  public void tearDown() throws Exception
  {
    resourceCloser.close();
  }

  InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return MapInputRowParser.parse(FOO_SCHEMA, (Map<String, Object>) map);
  }

  InputRow createRow(final ImmutableMap<String, ?> map, InputRowSchema inputRowSchema)
  {
    return MapInputRowParser.parse(inputRowSchema, (Map<String, Object>) map);
  }

  QueryLifecycleFactory getQueryLifecycleFactory(QuerySegmentWalker walker)
  {
    return new QueryLifecycleFactory(
        queryToolChestWarehouse,
        walker,
        new DefaultGenericQueryMetricsFactory(),
        new NoopServiceEmitter(),
        new TestRequestLogger(),
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
    );
  }

  public void checkStaleDatasourceRefresh(SegmentMetadataCache schema) throws IOException
  {
    Set<SegmentId> segments = new HashSet<>();
    Set<String> datasources = new HashSet<>();
    datasources.add("wat");
    Assert.assertNull(schema.getDatasource("wat"));
    schema.refresh(segments, datasources);
    Assert.assertNull(schema.getDatasource("wat"));
  }

  public void checkRefreshShouldEmitMetrics(
      SegmentMetadataCache schema,
      String dataSource,
      StubServiceEmitter emitter,
      CountDownLatch addSegmentLatch
  )
      throws IOException, InterruptedException
  {
    List<DataSegment> segments = ImmutableList.of(
        newSegment(dataSource, 1),
        newSegment(dataSource, 2)
    );
    serverView.addSegment(segments.get(0), ServerType.HISTORICAL);
    serverView.addSegment(segments.get(1), ServerType.REALTIME);
    Assert.assertTrue(addSegmentLatch.await(1, TimeUnit.SECONDS));
    schema.refresh(segments.stream().map(DataSegment::getId).collect(Collectors.toSet()), Sets.newHashSet(dataSource));

    emitter.verifyEmitted("metadatacache/refresh/time", ImmutableMap.of(DruidMetrics.DATASOURCE, dataSource), 1);
    emitter.verifyEmitted("metadatacache/refresh/count", ImmutableMap.of(DruidMetrics.DATASOURCE, dataSource), 1);
  }

  public void checkNullAvailableSegmentMetadata(SegmentMetadataCache schema) throws IOException
  {
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

  public void checkNullDatasource(SegmentMetadataCache schema) throws IOException
  {
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

  public void checkAvailableSegmentMetadataNumRows(SegmentMetadataCache schema) throws InterruptedException
  {
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

  public void checkRunSegmentMetadataQueryWithContext(SegmentMetadataCache schema, QueryLifecycleFactory factoryMock, QueryLifecycle lifecycleMock)
  {
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

  public DataSegment newSegment(String datasource, int partitionId)
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
        DataSegment.PruneSpecsHolder.DEFAULT
    );
  }
}
