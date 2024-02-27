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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import junitparams.converters.Nullable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.InternalQueryConfig;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.schema.SystemSchema.SegmentsTable;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.sql.calcite.util.TestTimelineServerView;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SystemSchemaTest extends CalciteTestBase
{
  private static final BrokerSegmentMetadataCacheConfig SEGMENT_CACHE_CONFIG_DEFAULT = BrokerSegmentMetadataCacheConfig.create();

  private static final List<InputRow> ROWS1 = ImmutableList.of(
      TestDataBuilder.createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  private static final List<InputRow> ROWS2 = ImmutableList.of(
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  private static final List<InputRow> ROWS3 = ImmutableList.of(
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-01", "m1", "7.0", "dim3", ImmutableList.of("x"))),
      TestDataBuilder.createRow(ImmutableMap.of("t", "2001-01-02", "m1", "8.0", "dim3", ImmutableList.of("xyz")))
  );

  private SystemSchema schema;
  private SpecificSegmentsQuerySegmentWalker walker;
  private DruidLeaderClient client;
  private DruidLeaderClient coordinatorClient;
  private OverlordClient overlordClient;
  private TimelineServerView serverView;
  private ObjectMapper mapper;
  private StringFullResponseHolder responseHolder;
  private BytesAccumulatingResponseHandler responseHandler;
  private Request request;
  private DruidSchema druidSchema;
  private AuthorizerMapper authMapper;
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  private MetadataSegmentView metadataView;
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private FilteredServerInventoryView serverInventoryView;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Before
  public void setUp() throws Exception
  {
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    client = EasyMock.createMock(DruidLeaderClient.class);
    coordinatorClient = EasyMock.createMock(DruidLeaderClient.class);
    overlordClient = EasyMock.createMock(OverlordClient.class);
    mapper = TestHelper.makeJsonMapper();
    responseHolder = EasyMock.createMock(StringFullResponseHolder.class);
    responseHandler = EasyMock.createMockBuilder(BytesAccumulatingResponseHandler.class)
                              .withConstructor()
                              .addMockedMethod(
                                  "handleResponse",
                                  HttpResponse.class,
                                  HttpResponseHandler.TrafficCop.class
                              )
                              .addMockedMethod("getStatus")
                              .createMock();
    request = EasyMock.createMock(Request.class);
    authMapper = createAuthMapper();

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
    final QueryableIndex index3 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "3"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withMetrics(new LongSumAggregatorFactory("m1", "m1"))
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(ROWS3)
                                              .buildMMappedIndex();

    walker = SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate)
        .add(segment1, index1)
        .add(segment2, index2)
        .add(segment3, index3);

    BrokerSegmentMetadataCache cache = new BrokerSegmentMetadataCache(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        new TestTimelineServerView(walker.getSegments(), realtimeSegments),
        SEGMENT_CACHE_CONFIG_DEFAULT,
        new NoopEscalator(),
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(
            new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
            new SegmentManager(EasyMock.createMock(SegmentLoader.class))
        ),
        new NoopCoordinatorClient()
    );
    cache.start();
    cache.awaitInitialization();
    druidSchema = new DruidSchema(cache, null, CatalogResolver.NULL_RESOLVER);
    metadataView = EasyMock.createMock(MetadataSegmentView.class);
    druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    serverInventoryView = EasyMock.createMock(FilteredServerInventoryView.class);
    schema = new SystemSchema(
        druidSchema,
        metadataView,
        serverView,
        serverInventoryView,
        EasyMock.createStrictMock(AuthorizerMapper.class),
        client,
        overlordClient,
        druidNodeDiscoveryProvider,
        mapper
    );
  }

  private final CompactionState expectedCompactionState = new CompactionState(
      new DynamicPartitionsSpec(null, null),
      null,
      null,
      null,
      Collections.singletonMap("test", "map"),
      Collections.singletonMap("test2", "map2")
  );

  private final DataSegment publishedCompactedSegment1 = new DataSegment(
      "wikipedia1",
      Intervals.of("2007/2008"),
      "version1",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      expectedCompactionState,
      1,
      53000L
  );
  private final DataSegment publishedCompactedSegment2 = new DataSegment(
      "wikipedia2",
      Intervals.of("2008/2009"),
      "version2",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      expectedCompactionState,
      1,
      83000L
  );
  private final DataSegment publishedUncompactedSegment3 = new DataSegment(
      "wikipedia3",
      Intervals.of("2009/2010"),
      "version3",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      null,
      1,
      47000L
  );

  private final DataSegment segment1 = new DataSegment(
      "test1",
      Intervals.of("2010/2011"),
      "version1",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment2 = new DataSegment(
      "test2",
      Intervals.of("2011/2012"),
      "version2",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment3 = new DataSegment(
      "test3",
      Intervals.of("2012/2013"),
      "version3",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      new NumberedShardSpec(2, 3),
      1,
      100L
  );
  private final DataSegment segment4 = new DataSegment(
      "test4",
      Intervals.of("2014/2015"),
      "version4",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment5 = new DataSegment(
      "test5",
      Intervals.of("2015/2016"),
      "version5",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );

  final List<DataSegment> realtimeSegments = ImmutableList.of(segment2, segment4, segment5);

  private final DateTime startTime = DateTimes.nowUtc();

  private final DiscoveryDruidNode coordinator = new DiscoveryDruidNode(
      new DruidNode("s1", "localhost", false, 8081, null, true, false),
      NodeRole.COORDINATOR,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode coordinator2 = new DiscoveryDruidNode(
      new DruidNode("s1", "localhost", false, 8181, null, true, false),
      NodeRole.COORDINATOR,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode overlord = new DiscoveryDruidNode(
      new DruidNode("s2", "localhost", false, 8090, null, true, false),
      NodeRole.OVERLORD,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode overlord2 = new DiscoveryDruidNode(
      new DruidNode("s2", "localhost", false, 8190, null, true, false),
      NodeRole.OVERLORD,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode broker1 = new DiscoveryDruidNode(
      new DruidNode("s3", "localhost", false, 8082, null, true, false),
      NodeRole.BROKER,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode broker2 = new DiscoveryDruidNode(
      new DruidNode("s3", "brokerHost", false, 8082, null, true, false),
      NodeRole.BROKER,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode brokerWithBroadcastSegments = new DiscoveryDruidNode(
      new DruidNode("s3", "brokerHostWithBroadcastSegments", false, 8082, 8282, true, true),
      NodeRole.BROKER,
      ImmutableMap.of(
          DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.BROKER, 0)
      ),
      startTime
  );

  private final DiscoveryDruidNode router = new DiscoveryDruidNode(
      new DruidNode("s4", "localhost", false, 8888, null, true, false),
      NodeRole.ROUTER,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode historical1 = new DiscoveryDruidNode(
      new DruidNode("s5", "localhost", false, 8083, null, true, false),
      NodeRole.HISTORICAL,
      ImmutableMap.of(
          DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0)
      ),
      startTime
  );

  private final DiscoveryDruidNode historical2 = new DiscoveryDruidNode(
      new DruidNode("s5", "histHost", false, 8083, null, true, false),
      NodeRole.HISTORICAL,
      ImmutableMap.of(
          DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0)
      ),
      startTime
  );

  private final DiscoveryDruidNode lameHistorical = new DiscoveryDruidNode(
      new DruidNode("s5", "lameHost", false, 8083, null, true, false),
      NodeRole.HISTORICAL,
      ImmutableMap.of(
          DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0)
      ),
      startTime
  );

  private final DiscoveryDruidNode middleManager = new DiscoveryDruidNode(
      new DruidNode("s6", "mmHost", false, 8091, null, true, false),
      NodeRole.MIDDLE_MANAGER,
      ImmutableMap.of(),
      startTime
  );

  private final DiscoveryDruidNode peon1 = new DiscoveryDruidNode(
      new DruidNode("s7", "localhost", false, 8080, null, true, false),
      NodeRole.PEON,
      ImmutableMap.of(
          DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0)
      ),
      startTime
  );

  private final DiscoveryDruidNode peon2 = new DiscoveryDruidNode(
      new DruidNode("s7", "peonHost", false, 8080, null, true, false),
      NodeRole.PEON,
      ImmutableMap.of(
          DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0)
      ),
      startTime
  );

  private final DiscoveryDruidNode indexer = new DiscoveryDruidNode(
      new DruidNode("s8", "indexerHost", false, 8091, null, true, false),
      NodeRole.INDEXER,
      ImmutableMap.of(
          DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0)
      ),
      startTime
  );

  private final ImmutableDruidServer druidServer1 = new ImmutableDruidServer(
      new DruidServerMetadata("server1", "localhost:0000", null, 5L, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 0),
      1L,
      ImmutableMap.of(
          "dummy",
          new ImmutableDruidDataSource("dummy", Collections.emptyMap(), Arrays.asList(segment1, segment2))
      ),
      2
  );

  private final ImmutableDruidServer druidServer2 = new ImmutableDruidServer(
      new DruidServerMetadata("server2", "server2:1234", null, 5L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
      1L,
      ImmutableMap.of(
          "dummy",
          new ImmutableDruidDataSource("dummy", Collections.emptyMap(), Arrays.asList(segment3, segment4, segment5))
      ),
      3
  );

  private final List<ImmutableDruidServer> immutableDruidServers = ImmutableList.of(druidServer1, druidServer2);

  @Test
  public void testGetTableMap()
  {
    Assert.assertEquals(
        ImmutableSet.of("segments", "servers", "server_segments", "tasks", "supervisors"),
        schema.getTableNames()
    );

    final Map<String, Table> tableMap = schema.getTableMap();
    Assert.assertEquals(
        ImmutableSet.of("segments", "servers", "server_segments", "tasks", "supervisors"),
        tableMap.keySet()
    );
    final SystemSchema.SegmentsTable segmentsTable = (SystemSchema.SegmentsTable) schema.getTableMap().get("segments");
    final RelDataType rowType = segmentsTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(19, fields.size());

    final SystemSchema.TasksTable tasksTable = (SystemSchema.TasksTable) schema.getTableMap().get("tasks");
    final RelDataType sysRowType = tasksTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> sysFields = sysRowType.getFieldList();
    Assert.assertEquals(14, sysFields.size());

    Assert.assertEquals("task_id", sysFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, sysFields.get(0).getType().getSqlTypeName());

    final SystemSchema.ServersTable serversTable = (SystemSchema.ServersTable) schema.getTableMap().get("servers");
    final RelDataType serverRowType = serversTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> serverFields = serverRowType.getFieldList();
    Assert.assertEquals(10, serverFields.size());
    Assert.assertEquals("server", serverFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, serverFields.get(0).getType().getSqlTypeName());
  }

  @Test
  public void testSegmentsTable() throws Exception
  {
    final SegmentsTable segmentsTable = new SegmentsTable(druidSchema, metadataView, new ObjectMapper(), authMapper);
    final Set<SegmentStatusInCluster> publishedSegments = new HashSet<>(Arrays.asList(
        new SegmentStatusInCluster(publishedCompactedSegment1, true, 2, null, false),
        new SegmentStatusInCluster(publishedCompactedSegment2, false, 0, null, false),
        new SegmentStatusInCluster(publishedUncompactedSegment3, false, 2, null, false),
        new SegmentStatusInCluster(segment1, true, 2, null, false),
        new SegmentStatusInCluster(segment2, false, 0, null, false)
    ));

    EasyMock.expect(metadataView.getSegments()).andReturn(publishedSegments.iterator()).once();

    EasyMock.replay(client, request, responseHolder, responseHandler, metadataView);
    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = segmentsTable.scan(dataContext).toList();
    rows.sort((Object[] row1, Object[] row2) -> ((Comparable) row1[0]).compareTo(row2[0]));

    // total segments = 8
    // segments test1, test2  are published and available
    // segment test3 is served by historical but unpublished or unused
    // segments test4, test5 are not published but available (realtime segments)
    // segment test2 is both published and served by a realtime server.

    Assert.assertEquals(8, rows.size());

    verifyRow(
        rows.get(0),
        "test1_2010-01-01T00:00:00.000Z_2011-01-01T00:00:00.000Z_version1",
        100L,
        0L, //partition_num
        1L, //num_replicas
        3L, //numRows
        1L, //is_published
        1L, //is_available
        0L, //is_realtime
        1L, //is_overshadowed
        null, //is_compacted
        2L  // replication_factor
    );

    verifyRow(
        rows.get(1),
        "test2_2011-01-01T00:00:00.000Z_2012-01-01T00:00:00.000Z_version2",
        100L,
        0L, //partition_num
        2L, //x§segment test2 is served by historical and realtime servers
        3L, //numRows
        1L, //is_published
        1L, //is_available
        0L, //is_realtime
        0L, //is_overshadowed,
        null, //is_compacted
        0L  // replication_factor
    );

    //segment test3 is unpublished and has a NumberedShardSpec with partitionNum = 2
    verifyRow(
        rows.get(2),
        "test3_2012-01-01T00:00:00.000Z_2013-01-01T00:00:00.000Z_version3_2",
        100L,
        2L, //partition_num
        1L, //num_replicas
        2L, //numRows
        0L, //is_published
        1L, //is_available
        0L, //is_realtime
        0L, //is_overshadowed
        null, //is_compacted
        -1L   // replication_factor
    );

    verifyRow(
        rows.get(3),
        "test4_2014-01-01T00:00:00.000Z_2015-01-01T00:00:00.000Z_version4",
        100L,
        0L, //partition_num
        1L, //num_replicas
        0L, //numRows
        0L, //is_published
        1L, //is_available
        1L, //is_realtime
        0L, //is_overshadowed
        null, //is_compacted
        -1L  // replication_factor
    );

    verifyRow(
        rows.get(4),
        "test5_2015-01-01T00:00:00.000Z_2016-01-01T00:00:00.000Z_version5",
        100L,
        0L, //partition_num
        1L, //num_replicas
        0L, //numRows
        0L, //is_published
        1L, //is_available
        1L, //is_realtime
        0L, //is_overshadowed
        null, //is_compacted
        -1L  // replication_factor
    );

    // wikipedia segments are published and unavailable, num_replicas is 0
    // wikipedia segment 1 and 2 are compacted while 3 are not compacted
    verifyRow(
        rows.get(5),
        "wikipedia1_2007-01-01T00:00:00.000Z_2008-01-01T00:00:00.000Z_version1",
        53000L,
        0L, //partition_num
        0L, //num_replicas
        0L, //numRows
        1L, //is_published
        0L, //is_available
        0L, //is_realtime
        1L, //is_overshadowed
        expectedCompactionState, //is_compacted
        2L  // replication_factor
    );

    verifyRow(
        rows.get(6),
        "wikipedia2_2008-01-01T00:00:00.000Z_2009-01-01T00:00:00.000Z_version2",
        83000L,
        0L, //partition_num
        0L, //num_replicas
        0L, //numRows
        1L, //is_published
        0L, //is_available
        0L, //is_realtime
        0L, //is_overshadowed
        expectedCompactionState, //is_compacted
        0L  // replication_factor
    );

    verifyRow(
        rows.get(7),
        "wikipedia3_2009-01-01T00:00:00.000Z_2010-01-01T00:00:00.000Z_version3",
        47000L,
        0L, //partition_num
        0L, //num_replicas
        0L, //numRows
        1L, //is_published
        0L, //is_available
        0L, //is_realtime
        0L, //is_overshadowed
        null, //is_compacted
        2L  // replication_factor
    );

    // Verify value types.
    verifyTypes(rows, SystemSchema.SEGMENTS_SIGNATURE);
  }

  private void verifyRow(
      Object[] row,
      String segmentId,
      long size,
      long partitionNum,
      long numReplicas,
      long numRows,
      long isPublished,
      long isAvailable,
      long isRealtime,
      long isOvershadowed,
      CompactionState compactionState,
      long replicationFactor
  ) throws Exception
  {
    Assert.assertEquals(segmentId, row[0].toString());
    SegmentId id = Iterables.get(SegmentId.iterateAllPossibleParsings(segmentId), 0);
    Assert.assertEquals(id.getDataSource(), row[1]);
    Assert.assertEquals(id.getIntervalStart().toString(), row[2]);
    Assert.assertEquals(id.getIntervalEnd().toString(), row[3]);
    Assert.assertEquals(size, row[4]);
    Assert.assertEquals(id.getVersion(), row[5]);
    Assert.assertEquals(partitionNum, row[6]);
    Assert.assertEquals(numReplicas, row[7]);
    Assert.assertEquals(numRows, row[8]);
    Assert.assertEquals((((isPublished == 1) && (isOvershadowed == 0)) || (isRealtime == 1)) ? 1L : 0L, row[9]);
    Assert.assertEquals(isPublished, row[10]);
    Assert.assertEquals(isAvailable, row[11]);
    Assert.assertEquals(isRealtime, row[12]);
    Assert.assertEquals(isOvershadowed, row[13]);
    if (compactionState == null) {
      Assert.assertNull(row[17]);
    } else {
      Assert.assertEquals(mapper.writeValueAsString(compactionState), row[17]);
    }
    Assert.assertEquals(replicationFactor, row[18]);
  }

  @Test
  public void testServersTable() throws URISyntaxException
  {

    SystemSchema.ServersTable serversTable = EasyMock.createMockBuilder(SystemSchema.ServersTable.class)
                                                     .withConstructor(
                                                         druidNodeDiscoveryProvider,
                                                         serverInventoryView,
                                                         authMapper,
                                                         overlordClient,
                                                         coordinatorClient
                                                     )
                                                     .createMock();
    EasyMock.replay(serversTable);
    final DruidNodeDiscovery coordinatorNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    final DruidNodeDiscovery overlordNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    final DruidNodeDiscovery brokerNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    final DruidNodeDiscovery routerNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    final DruidNodeDiscovery historicalNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    final DruidNodeDiscovery mmNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    final DruidNodeDiscovery peonNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    final DruidNodeDiscovery indexerNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);


    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.COORDINATOR))
            .andReturn(coordinatorNodeDiscovery)
            .once();
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.OVERLORD))
            .andReturn(overlordNodeDiscovery)
            .once();
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER)).andReturn(brokerNodeDiscovery).once();
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.ROUTER)).andReturn(routerNodeDiscovery).once();
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.HISTORICAL))
            .andReturn(historicalNodeDiscovery)
            .once();
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.MIDDLE_MANAGER))
            .andReturn(mmNodeDiscovery)
            .once();
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.INDEXER))
            .andReturn(indexerNodeDiscovery)
            .once();
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(peonNodeDiscovery).once();

    EasyMock.expect(coordinatorNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(coordinator, coordinator2)).once();
    EasyMock.expect(overlordNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(overlord, overlord2)).once();
    EasyMock.expect(brokerNodeDiscovery.getAllNodes())
            .andReturn(ImmutableList.of(broker1, broker2, brokerWithBroadcastSegments))
            .once();
    EasyMock.expect(routerNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(router)).once();
    EasyMock.expect(historicalNodeDiscovery.getAllNodes())
            .andReturn(ImmutableList.of(historical1, historical2, lameHistorical))
            .once();
    EasyMock.expect(mmNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(middleManager)).once();
    EasyMock.expect(peonNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(peon1, peon2)).once();
    EasyMock.expect(indexerNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(indexer)).once();

    EasyMock.expect(coordinatorClient.findCurrentLeader()).andReturn(coordinator.getDruidNode().getHostAndPortToUse()).once();
    EasyMock.expect(overlordClient.findCurrentLeader())
            .andReturn(Futures.immediateFuture(new URI(overlord.getDruidNode().getHostAndPortToUse()))).once();

    final List<DruidServer> servers = new ArrayList<>();
    servers.add(mockDataServer(historical1.getDruidNode().getHostAndPortToUse(), 200L, 1000L, "tier"));
    servers.add(mockDataServer(historical2.getDruidNode().getHostAndPortToUse(), 400L, 1000L, "tier"));
    servers.add(mockDataServer(peon1.getDruidNode().getHostAndPortToUse(), 0L, 1000L, "tier"));
    servers.add(mockDataServer(peon2.getDruidNode().getHostAndPortToUse(), 0L, 1000L, "tier"));
    servers.add(mockDataServer(broker1.getDruidNode().getHostAndPortToUse(), 0L, 1000L, "tier"));
    servers.add(mockDataServer(broker2.getDruidNode().getHostAndPortToUse(), 0L, 1000L, "tier"));
    servers.add(mockDataServer(indexer.getDruidNode().getHostAndPortToUse(), 0L, 1000L, "tier"));
    servers.add(mockDataServer(brokerWithBroadcastSegments.getDruidNode().getHostAndPortToUse(), 0L, 1000L, "tier"));
    EasyMock.expect(serverInventoryView.getInventoryValue(lameHistorical.getDruidNode().getHostAndPortToUse()))
            .andReturn(null)
            .once();

    EasyMock.replay(druidNodeDiscoveryProvider, serverInventoryView, coordinatorClient, overlordClient);
    EasyMock.replay(servers.toArray(new Object[0]));
    EasyMock.replay(
        coordinatorNodeDiscovery,
        overlordNodeDiscovery,
        brokerNodeDiscovery,
        routerNodeDiscovery,
        historicalNodeDiscovery,
        mmNodeDiscovery,
        peonNodeDiscovery,
        indexerNodeDiscovery
    );

    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = serversTable.scan(dataContext).toList();
    rows.sort((Object[] row1, Object[] row2) -> ((Comparable) row1[0]).compareTo(row2[0]));

    final List<Object[]> expectedRows = new ArrayList<>();
    final Long nonLeader = NullHandling.defaultLongValue();
    final String startTimeStr = startTime.toString();
    expectedRows.add(
        createExpectedRow(
            "brokerHost:8082",
            "brokerHost",
            8082,
            -1,
            NodeRole.BROKER,
            null,
            0L,
            0L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "brokerHostWithBroadcastSegments:8282",
            "brokerHostWithBroadcastSegments",
            8082,
            8282,
            NodeRole.BROKER,
            "tier",
            0L,
            1000L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "histHost:8083",
            "histHost",
            8083,
            -1,
            NodeRole.HISTORICAL,
            "tier",
            400L,
            1000L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "indexerHost:8091",
            "indexerHost",
            8091,
            -1,
            NodeRole.INDEXER,
            "tier",
            0L,
            1000L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "lameHost:8083",
            "lameHost",
            8083,
            -1,
            NodeRole.HISTORICAL,
            "tier",
            0L,
            1000L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(createExpectedRow(
        "localhost:8080",
        "localhost",
        8080,
        -1,
        NodeRole.PEON,
        "tier",
        0L,
        1000L,
        nonLeader,
        startTimeStr
    ));
    expectedRows.add(
        createExpectedRow(
            "localhost:8081",
            "localhost",
            8081,
            -1,
            NodeRole.COORDINATOR,
            null,
            0L,
            0L,
            1L,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "localhost:8082",
            "localhost",
            8082,
            -1,
            NodeRole.BROKER,
            null,
            0L,
            0L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "localhost:8083",
            "localhost",
            8083,
            -1,
            NodeRole.HISTORICAL,
            "tier",
            200L,
            1000L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "localhost:8090",
            "localhost",
            8090,
            -1,
            NodeRole.OVERLORD,
            null,
            0L,
            0L,
            1L,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "localhost:8181",
            "localhost",
            8181,
            -1,
            NodeRole.COORDINATOR,
            null,
            0L,
            0L,
            0L,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "localhost:8190",
            "localhost",
            8190,
            -1,
            NodeRole.OVERLORD,
            null,
            0L,
            0L,
            0L,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "localhost:8888",
            "localhost",
            8888,
            -1,
            NodeRole.ROUTER,
            null,
            0L,
            0L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(
        createExpectedRow(
            "mmHost:8091",
            "mmHost",
            8091,
            -1,
            NodeRole.MIDDLE_MANAGER,
            null,
            0L,
            0L,
            nonLeader,
            startTimeStr
        )
    );
    expectedRows.add(createExpectedRow(
        "peonHost:8080",
        "peonHost",
        8080,
        -1,
        NodeRole.PEON,
        "tier",
        0L,
        1000L,
        nonLeader,
        startTimeStr
    ));
    Assert.assertEquals(expectedRows.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertArrayEquals(expectedRows.get(i), rows.get(i));
    }

    // Verify value types.
    verifyTypes(rows, SystemSchema.SERVERS_SIGNATURE);
  }

  private DruidServer mockDataServer(String name, long currentSize, long maxSize, String tier)
  {
    final DruidServer server = EasyMock.createMock(DruidServer.class);
    EasyMock.expect(serverInventoryView.getInventoryValue(name))
            .andReturn(server)
            .once();
    EasyMock.expect(server.getCurrSize()).andReturn(currentSize).once();
    EasyMock.expect(server.getMaxSize()).andReturn(maxSize).once();
    EasyMock.expect(server.getTier()).andReturn(tier).once();
    return server;
  }

  private Object[] createExpectedRow(
      String server,
      String host,
      int plaintextPort,
      int tlsPort,
      NodeRole nodeRole,
      @Nullable String tier,
      @Nullable Long currSize,
      @Nullable Long maxSize,
      @Nullable Long isLeader,
      String startTime
  )
  {
    return new Object[]{
        server,
        host,
        (long) plaintextPort,
        (long) tlsPort,
        StringUtils.toLowerCase(nodeRole.toString()),
        tier,
        currSize,
        maxSize,
        isLeader,
        startTime
    };
  }

  @Test
  public void testServerSegmentsTable()
  {
    SystemSchema.ServerSegmentsTable serverSegmentsTable = EasyMock
        .createMockBuilder(SystemSchema.ServerSegmentsTable.class)
        .withConstructor(serverView, authMapper)
        .createMock();
    EasyMock.replay(serverSegmentsTable);
    EasyMock.expect(serverView.getDruidServers())
            .andReturn(immutableDruidServers)
            .once();
    EasyMock.replay(serverView);
    DataContext dataContext = createDataContext(Users.SUPER);

    //server_segments table is the join of servers and segments table
    // it will have 5 rows as follows
    // localhost:0000 |  test1_2010-01-01T00:00:00.000Z_2011-01-01T00:00:00.000Z_version1(segment1)
    // localhost:0000 |  test2_2011-01-01T00:00:00.000Z_2012-01-01T00:00:00.000Z_version2(segment2)
    // server2:1234   |  test3_2012-01-01T00:00:00.000Z_2013-01-01T00:00:00.000Z_version3(segment3)
    // server2:1234   |  test4_2017-01-01T00:00:00.000Z_2018-01-01T00:00:00.000Z_version4(segment4)
    // server2:1234   |  test5_2017-01-01T00:00:00.000Z_2018-01-01T00:00:00.000Z_version5(segment5)

    final List<Object[]> rows = serverSegmentsTable.scan(dataContext).toList();
    Assert.assertEquals(5, rows.size());

    Object[] row0 = rows.get(0);
    Assert.assertEquals("localhost:0000", row0[0]);
    Assert.assertEquals("test1_2010-01-01T00:00:00.000Z_2011-01-01T00:00:00.000Z_version1", row0[1].toString());

    Object[] row1 = rows.get(1);
    Assert.assertEquals("localhost:0000", row1[0]);
    Assert.assertEquals("test2_2011-01-01T00:00:00.000Z_2012-01-01T00:00:00.000Z_version2", row1[1].toString());

    Object[] row2 = rows.get(2);
    Assert.assertEquals("server2:1234", row2[0]);
    Assert.assertEquals("test3_2012-01-01T00:00:00.000Z_2013-01-01T00:00:00.000Z_version3_2", row2[1].toString());

    Object[] row3 = rows.get(3);
    Assert.assertEquals("server2:1234", row3[0]);
    Assert.assertEquals("test4_2014-01-01T00:00:00.000Z_2015-01-01T00:00:00.000Z_version4", row3[1].toString());

    Object[] row4 = rows.get(4);
    Assert.assertEquals("server2:1234", row4[0]);
    Assert.assertEquals("test5_2015-01-01T00:00:00.000Z_2016-01-01T00:00:00.000Z_version5", row4[1].toString());

    // Verify value types.
    verifyTypes(rows, SystemSchema.SERVER_SEGMENTS_SIGNATURE);
  }

  @Test
  public void testTasksTable() throws Exception
  {

    SystemSchema.TasksTable tasksTable = EasyMock.createMockBuilder(SystemSchema.TasksTable.class)
                                                 .withConstructor(overlordClient, authMapper)
                                                 .createMock();

    EasyMock.replay(tasksTable);

    String json = "[{\n"
                  + "\t\"id\": \"index_wikipedia_2018-09-20T22:33:44.911Z\",\n"
                  + "\t\"groupId\": \"group_index_wikipedia_2018-09-20T22:33:44.911Z\",\n"
                  + "\t\"type\": \"index\",\n"
                  + "\t\"createdTime\": \"2018-09-20T22:33:44.922Z\",\n"
                  + "\t\"queueInsertionTime\": \"1970-01-01T00:00:00.000Z\",\n"
                  + "\t\"statusCode\": \"FAILED\",\n"
                  + "\t\"runnerStatusCode\": \"NONE\",\n"
                  + "\t\"duration\": -1,\n"
                  + "\t\"location\": {\n"
                  + "\t\t\"host\": \"testHost\",\n"
                  + "\t\t\"port\": 1234,\n"
                  + "\t\t\"tlsPort\": -1\n"
                  + "\t},\n"
                  + "\t\"dataSource\": \"wikipedia\",\n"
                  + "\t\"errorMsg\": null\n"
                  + "}, {\n"
                  + "\t\"id\": \"index_wikipedia_2018-09-21T18:38:47.773Z\",\n"
                  + "\t\"groupId\": \"group_index_wikipedia_2018-09-21T18:38:47.773Z\",\n"
                  + "\t\"type\": \"index\",\n"
                  + "\t\"createdTime\": \"2018-09-21T18:38:47.873Z\",\n"
                  + "\t\"queueInsertionTime\": \"2018-09-21T18:38:47.910Z\",\n"
                  + "\t\"statusCode\": \"RUNNING\",\n"
                  + "\t\"runnerStatusCode\": \"RUNNING\",\n"
                  + "\t\"duration\": null,\n"
                  + "\t\"location\": {\n"
                  + "\t\t\"host\": \"192.168.1.6\",\n"
                  + "\t\t\"port\": 8100,\n"
                  + "\t\t\"tlsPort\": -1\n"
                  + "\t},\n"
                  + "\t\"dataSource\": \"wikipedia\",\n"
                  + "\t\"errorMsg\": null\n"
                  + "}]";

    EasyMock.expect(overlordClient.taskStatuses(null, null, null)).andReturn(
        Futures.immediateFuture(
            CloseableIterators.withEmptyBaggage(
                mapper.readValue(json, new TypeReference<List<TaskStatusPlus>>() {}).iterator()
            )
        )
    );

    EasyMock.replay(overlordClient, request, responseHandler);
    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = tasksTable.scan(dataContext).toList();

    Object[] row0 = rows.get(0);
    Assert.assertEquals("index_wikipedia_2018-09-20T22:33:44.911Z", row0[0].toString());
    Assert.assertEquals("group_index_wikipedia_2018-09-20T22:33:44.911Z", row0[1].toString());
    Assert.assertEquals("index", row0[2].toString());
    Assert.assertEquals("wikipedia", row0[3].toString());
    Assert.assertEquals("2018-09-20T22:33:44.922Z", row0[4].toString());
    Assert.assertEquals("1970-01-01T00:00:00.000Z", row0[5].toString());
    Assert.assertEquals("FAILED", row0[6].toString());
    Assert.assertEquals("NONE", row0[7].toString());
    Assert.assertEquals(-1L, row0[8]);
    Assert.assertEquals("testHost:1234", row0[9]);
    Assert.assertEquals("testHost", row0[10]);
    Assert.assertEquals(1234L, row0[11]);
    Assert.assertEquals(-1L, row0[12]);
    Assert.assertEquals(null, row0[13]);

    Object[] row1 = rows.get(1);
    Assert.assertEquals("index_wikipedia_2018-09-21T18:38:47.773Z", row1[0].toString());
    Assert.assertEquals("group_index_wikipedia_2018-09-21T18:38:47.773Z", row1[1].toString());
    Assert.assertEquals("index", row1[2].toString());
    Assert.assertEquals("wikipedia", row1[3].toString());
    Assert.assertEquals("2018-09-21T18:38:47.873Z", row1[4].toString());
    Assert.assertEquals("2018-09-21T18:38:47.910Z", row1[5].toString());
    Assert.assertEquals("RUNNING", row1[6].toString());
    Assert.assertEquals("RUNNING", row1[7].toString());
    Assert.assertEquals(0L, row1[8]);
    Assert.assertEquals("192.168.1.6:8100", row1[9]);
    Assert.assertEquals("192.168.1.6", row1[10]);
    Assert.assertEquals(8100L, row1[11]);
    Assert.assertEquals(-1L, row1[12]);
    Assert.assertEquals(null, row1[13]);

    // Verify value types.
    verifyTypes(rows, SystemSchema.TASKS_SIGNATURE);
  }

  @Test
  public void testTasksTableAuth()
  {
    SystemSchema.TasksTable tasksTable = new SystemSchema.TasksTable(overlordClient, authMapper);

    String json = "[{\n"
                  + "\t\"id\": \"index_wikipedia_2018-09-20T22:33:44.911Z\",\n"
                  + "\t\"groupId\": \"group_index_wikipedia_2018-09-20T22:33:44.911Z\",\n"
                  + "\t\"type\": \"index\",\n"
                  + "\t\"createdTime\": \"2018-09-20T22:33:44.922Z\",\n"
                  + "\t\"queueInsertionTime\": \"1970-01-01T00:00:00.000Z\",\n"
                  + "\t\"statusCode\": \"FAILED\",\n"
                  + "\t\"runnerStatusCode\": \"NONE\",\n"
                  + "\t\"duration\": -1,\n"
                  + "\t\"location\": {\n"
                  + "\t\t\"host\": \"testHost\",\n"
                  + "\t\t\"port\": 1234,\n"
                  + "\t\t\"tlsPort\": -1\n"
                  + "\t},\n"
                  + "\t\"dataSource\": \"wikipedia\",\n"
                  + "\t\"errorMsg\": null\n"
                  + "}, {\n"
                  + "\t\"id\": \"index_wikipedia_2018-09-21T18:38:47.773Z\",\n"
                  + "\t\"groupId\": \"group_index_wikipedia_2018-09-21T18:38:47.773Z\",\n"
                  + "\t\"type\": \"index\",\n"
                  + "\t\"createdTime\": \"2018-09-21T18:38:47.873Z\",\n"
                  + "\t\"queueInsertionTime\": \"2018-09-21T18:38:47.910Z\",\n"
                  + "\t\"statusCode\": \"RUNNING\",\n"
                  + "\t\"runnerStatusCode\": \"RUNNING\",\n"
                  + "\t\"duration\": null,\n"
                  + "\t\"location\": {\n"
                  + "\t\t\"host\": \"192.168.1.6\",\n"
                  + "\t\t\"port\": 8100,\n"
                  + "\t\t\"tlsPort\": -1\n"
                  + "\t},\n"
                  + "\t\"dataSource\": \"wikipedia\",\n"
                  + "\t\"errorMsg\": null\n"
                  + "}]";

    EasyMock.expect(overlordClient.taskStatuses(null, null, null)).andAnswer(
        () -> Futures.immediateFuture(
            CloseableIterators.withEmptyBaggage(
                mapper.readValue(json, new TypeReference<List<TaskStatusPlus>>() {}).iterator()
            )
        )
    ).anyTimes();

    EasyMock.replay(overlordClient);

    // Verify that no row is returned for Datasource Write user
    List<Object[]> rows = tasksTable
        .scan(createDataContext(Users.DATASOURCE_WRITE))
        .toList();
    Assert.assertTrue(rows.isEmpty());

    // Verify that 2 rows are returned for Datasource Read user
    rows = tasksTable
        .scan(createDataContext(Users.DATASOURCE_READ))
        .toList();
    Assert.assertEquals(2, rows.size());

    // Verify that 2 rows are returned for Super user
    rows = tasksTable
        .scan(createDataContext(Users.SUPER))
        .toList();
    Assert.assertEquals(2, rows.size());
  }

  @Test
  public void testSupervisorTable() throws Exception
  {
    SystemSchema.SupervisorsTable supervisorTable =
        EasyMock.createMockBuilder(SystemSchema.SupervisorsTable.class)
                .withConstructor(overlordClient, authMapper)
                .createMock();
    EasyMock.replay(supervisorTable);

    String json = "[{\n"
                  + "\t\"id\": \"wikipedia\",\n"
                  + "\t\"state\": \"UNHEALTHY_SUPERVISOR\",\n"
                  + "\t\"detailedState\": \"UNABLE_TO_CONNECT_TO_STREAM\",\n"
                  + "\t\"healthy\": false,\n"
                  + "\t\"specString\": \"{\\\"type\\\":\\\"kafka\\\",\\\"dataSchema\\\":{\\\"dataSource\\\":\\\"wikipedia\\\"}"
                  + ",\\\"context\\\":null,\\\"suspended\\\":false}\",\n"
                  + "\t\"type\": \"kafka\",\n"
                  + "\t\"source\": \"wikipedia\",\n"
                  + "\t\"suspended\": false\n"
                  + "}]";

    EasyMock.expect(overlordClient.supervisorStatuses()).andReturn(
        Futures.immediateFuture(
            CloseableIterators.withEmptyBaggage(
                mapper.readValue(json, new TypeReference<List<SupervisorStatus>>() {}).iterator()
            )
        )
    );

    EasyMock.replay(overlordClient);
    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = supervisorTable.scan(dataContext).toList();

    Object[] row0 = rows.get(0);
    Assert.assertEquals("wikipedia", row0[0].toString());
    Assert.assertEquals("UNHEALTHY_SUPERVISOR", row0[1].toString());
    Assert.assertEquals("UNABLE_TO_CONNECT_TO_STREAM", row0[2].toString());
    Assert.assertEquals(0L, row0[3]);
    Assert.assertEquals("kafka", row0[4].toString());
    Assert.assertEquals("wikipedia", row0[5].toString());
    Assert.assertEquals(0L, row0[6]);
    Assert.assertEquals(
        "{\"type\":\"kafka\",\"dataSchema\":{\"dataSource\":\"wikipedia\"},\"context\":null,\"suspended\":false}",
        row0[7].toString()
    );

    // Verify value types.
    verifyTypes(rows, SystemSchema.SUPERVISOR_SIGNATURE);
  }

  @Test
  public void testSupervisorTableAuth()
  {
    SystemSchema.SupervisorsTable supervisorTable =
        new SystemSchema.SupervisorsTable(overlordClient, createAuthMapper());

    final String json = "[{\n"
                  + "\t\"id\": \"wikipedia\",\n"
                  + "\t\"state\": \"UNHEALTHY_SUPERVISOR\",\n"
                  + "\t\"detailedState\": \"UNABLE_TO_CONNECT_TO_STREAM\",\n"
                  + "\t\"healthy\": false,\n"
                  + "\t\"specString\": \"{\\\"type\\\":\\\"kafka\\\",\\\"dataSchema\\\":{\\\"dataSource\\\":\\\"wikipedia\\\"}"
                  + ",\\\"context\\\":null,\\\"suspended\\\":false}\",\n"
                  + "\t\"type\": \"kafka\",\n"
                  + "\t\"source\": \"wikipedia\",\n"
                  + "\t\"suspended\": false\n"
                  + "}]";

    EasyMock.expect(overlordClient.supervisorStatuses()).andAnswer(
        () -> Futures.immediateFuture(
            CloseableIterators.withEmptyBaggage(
                mapper.readValue(json, new TypeReference<List<SupervisorStatus>>() {}).iterator()
            )
        )
    ).anyTimes();

    EasyMock.replay(overlordClient);

    // Verify that no row is returned for Datasource Write user
    List<Object[]> rows = supervisorTable
        .scan(createDataContext(Users.DATASOURCE_WRITE))
        .toList();
    Assert.assertTrue(rows.isEmpty());

    // Verify that 1 row is returned for Datasource Write user
    rows = supervisorTable
        .scan(createDataContext(Users.DATASOURCE_READ))
        .toList();
    Assert.assertEquals(1, rows.size());

    // Verify that 1 row is returned for Super user
    rows = supervisorTable
        .scan(createDataContext(Users.SUPER))
        .toList();
    Assert.assertEquals(1, rows.size());

    // TODO: If needed, verify the first row here

    // TODO: Verify value types.
    // verifyTypes(rows, SystemSchema.SUPERVISOR_SIGNATURE);
  }

  /**
   * Creates a response holder that contains the given json.
   */
  private InputStreamFullResponseHolder createFullResponseHolder(
      HttpResponse httpResponse,
      String json
  )
  {
    InputStreamFullResponseHolder responseHolder = new InputStreamFullResponseHolder(httpResponse);

    byte[] bytesToWrite = json.getBytes(StandardCharsets.UTF_8);
    responseHolder.addChunk(bytesToWrite);
    responseHolder.done();

    return responseHolder;
  }

  /**
   * Creates a DataContext for the given username.
   */
  private DataContext createDataContext(String username)
  {
    return new DataContext()
    {
      @Override
      public SchemaPlus getRootSchema()
      {
        return null;
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return null;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        return null;
      }

      @Override
      public Object get(String authorizerName)
      {
        return CalciteTests.TEST_SUPERUSER_NAME.equals(username)
               ? CalciteTests.SUPER_USER_AUTH_RESULT
               : new AuthenticationResult(username, authorizerName, null, null);
      }
    };
  }

  private AuthorizerMapper createAuthMapper()
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return (authenticationResult, resource, action) -> {
          final String username = authenticationResult.getIdentity();

          // Allow access to a Datasource if
          // - Super User or Datasource Write User requests Write access
          // - Super User or Datasource Read User requests Read access
          if (resource.getType().equals(ResourceType.DATASOURCE)) {
            return new Access(
                username.equals(Users.SUPER)
                || (action == Action.READ && username.equals(Users.DATASOURCE_READ))
                || (action == Action.WRITE && username.equals(Users.DATASOURCE_WRITE))
            );
          }

          return new Access(true);
        };
      }
    };
  }

  private static void verifyTypes(final List<Object[]> rows, final RowSignature signature)
  {
    final RelDataType rowType = RowSignatures.toRelDataType(signature, new JavaTypeFactoryImpl());

    for (Object[] row : rows) {
      Assert.assertEquals(row.length, signature.size());

      for (int i = 0; i < row.length; i++) {
        final Class<?> expectedClass;

        final ColumnType columnType =
            signature.getColumnType(i)
                     .orElseThrow(() -> new ISE("Encountered null column type"));

        final boolean nullable = rowType.getFieldList().get(i).getType().isNullable();

        switch (columnType.getType()) {
          case LONG:
            expectedClass = Long.class;
            break;
          case FLOAT:
            expectedClass = Float.class;
            break;
          case DOUBLE:
            expectedClass = Double.class;
            break;
          case STRING:
            if (signature.getColumnName(i).equals("segment_id")) {
              expectedClass = SegmentId.class;
            } else {
              expectedClass = String.class;
            }
            break;
          default:
            throw new IAE("Don't know what class to expect for valueType[%s]", columnType);
        }

        if (nullable) {
          Assert.assertTrue(
              StringUtils.format(
                  "Column[%s] is a [%s] or null (was %s)",
                  signature.getColumnName(i),
                  expectedClass.getName(),
                  row[i] == null ? null : row[i].getClass().getName()
              ),
              row[i] == null || expectedClass.isAssignableFrom(row[i].getClass())
          );
        } else {
          Assert.assertTrue(
              StringUtils.format(
                  "Column[%s] is a [%s] (was %s)",
                  signature.getColumnName(i),
                  expectedClass.getName(),
                  row[i] == null ? null : row[i].getClass().getName()
              ),
              row[i] != null && expectedClass.isAssignableFrom(row[i].getClass())
          );
        }
      }
    }
  }

  /**
   * Usernames to be used in tests.
   */
  private static class Users
  {
    private static final String SUPER = CalciteTests.TEST_SUPERUSER_NAME;
    private static final String DATASOURCE_READ = "datasourceRead";
    private static final String DATASOURCE_WRITE = "datasourceWrite";
  }
}
