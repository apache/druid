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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
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
import org.apache.druid.timeline.partition.ShardSpec;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
  private static final ObjectMapper MAPPER = CalciteTests.getJsonMapper();

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

  private static final ShardSpec SHARD_SPEC = new NumberedShardSpec(0, 1);
  private static final IncrementalIndexSchema SCHEMA = new IncrementalIndexSchema.Builder()
      .withDimensionsSpec(new DimensionsSpec(ImmutableList.of(new StringDimensionSchema("dim1"))))
      .withMetrics(
          new CountAggregatorFactory("cnt"),
          new DoubleSumAggregatorFactory("m1", "m1"),
          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
      )
      .withRollup(false)
      .build();

  private SystemSchema schema;
  private SpecificSegmentsQuerySegmentWalker walker;
  private CoordinatorClient coordinatorClient;
  private OverlordClient overlordClient;
  private TimelineServerView serverView;
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
  private HttpClient httpClient;

  @BeforeAll
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
  }

  @AfterAll
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @BeforeEach
  public void setUp(@TempDir File tmpDir) throws Exception
  {
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    coordinatorClient = EasyMock.createMock(CoordinatorClient.class);
    overlordClient = EasyMock.createMock(OverlordClient.class);
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

    final QueryableIndex index1 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "1"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(SCHEMA)
                                              .rows(ROWS1)
                                              .buildMMappedIndex();

    final QueryableIndex index2 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "2"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(SCHEMA)
                                              .rows(ROWS2)
                                              .buildMMappedIndex();
    final QueryableIndex index3 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "3"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(SCHEMA)
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
            new SegmentManager(EasyMock.createMock(SegmentCacheManager.class))
        ),
        new NoopCoordinatorClient(),
        CentralizedDatasourceSchemaConfig.create()
    );
    cache.start();
    cache.awaitInitialization();
    druidSchema = new DruidSchema(cache, null, CatalogResolver.NULL_RESOLVER);
    metadataView = EasyMock.createMock(MetadataSegmentView.class);
    druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    serverInventoryView = EasyMock.createMock(FilteredServerInventoryView.class);
    httpClient = EasyMock.createMock(HttpClient.class);
    schema = new SystemSchema(
        druidSchema,
        metadataView,
        serverView,
        serverInventoryView,
        EasyMock.createStrictMock(AuthorizerMapper.class),
        coordinatorClient,
        overlordClient,
        druidNodeDiscoveryProvider,
        MAPPER,
        httpClient
    );
  }

  private final CompactionState expectedCompactionState = new CompactionState(
      new DynamicPartitionsSpec(null, null),
      null,
      null,
      null,
      MAPPER.convertValue(Collections.singletonMap("test", "map"), IndexSpec.class),
      MAPPER.convertValue(Collections.singletonMap("test2", "map2"), GranularitySpec.class),
      null
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
  private final DataSegment publishedUncompactedSegment3 = DataSegment.builder(SegmentId.of(
                                                                          "wikipedia3",
                                                                          Intervals.of("2009/2010"),
                                                                          "version3",
                                                                          null
                                                                      ))
                                                                      .dimensions(ImmutableList.of("dim1", "dim2"))
                                                                      .metrics(ImmutableList.of("met1", "met2"))
                                                                      .projections(ImmutableList.of())
                                                                      .binaryVersion(1)
                                                                      .size(47000L)
                                                                      .build();

  private final DataSegment segment1 = DataSegment.builder(SegmentId.of(
                                                      "test1",
                                                      Intervals.of("2010/2011"),
                                                      "version1",
                                                      null
                                                  ))
                                                  .dimensions(ImmutableList.of("dim1", "dim2"))
                                                  .metrics(ImmutableList.of("met1", "met2"))
                                                  .projections(ImmutableList.of("proj1", "proj2"))
                                                  .binaryVersion(1)
                                                  .size(100L)
                                                  .build();
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

  private final String version = GuavaUtils.firstNonNull(
          SystemSchemaTest.class.getPackage().getImplementationVersion(),
          DruidNode.UNKNOWN_VERSION
      );

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
      new DruidNode("s2", "localhost", false, 8090, null, null, true, false, ImmutableMap.of("overlordKey", "overlordValue")),
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
      new DruidNode("s3", "localhost", false, 8082, null, null, true, false, ImmutableMap.of("brokerKey", "brokerValue", "brokerKey2", "brokerValue2")),
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
        ImmutableSet.of("segments", "servers", "server_segments", "tasks", "supervisors", "server_properties"),
        schema.getTableNames()
    );

    final Map<String, Table> tableMap = schema.getTableMap();
    Assert.assertEquals(
        ImmutableSet.of("segments", "servers", "server_segments", "tasks", "supervisors", "server_properties"),
        tableMap.keySet()
    );
    final SystemSchema.SegmentsTable segmentsTable = (SystemSchema.SegmentsTable) schema.getTableMap().get("segments");
    final RelDataType rowType = segmentsTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(20, fields.size());

    final SystemSchema.TasksTable tasksTable = (SystemSchema.TasksTable) schema.getTableMap().get("tasks");
    final RelDataType sysRowType = tasksTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> sysFields = sysRowType.getFieldList();
    Assert.assertEquals(14, sysFields.size());

    Assert.assertEquals("task_id", sysFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, sysFields.get(0).getType().getSqlTypeName());

    final SystemSchema.ServersTable serversTable = (SystemSchema.ServersTable) schema.getTableMap().get("servers");
    final RelDataType serverRowType = serversTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> serverFields = serverRowType.getFieldList();
    Assert.assertEquals(12, serverFields.size());
    Assert.assertEquals("server", serverFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, serverFields.get(0).getType().getSqlTypeName());

    final SystemServerPropertiesTable propertiesTable = (SystemServerPropertiesTable) schema.getTableMap()
                                                                                            .get("server_properties");
    final RelDataType propertiesRowType = propertiesTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> propertiesFields = propertiesRowType.getFieldList();
    Assert.assertEquals(5, propertiesFields.size());
  }

  @Test
  public void testSegmentsTable() throws Exception
  {
    final SegmentsTable segmentsTable = new SegmentsTable(druidSchema, metadataView, MAPPER, authMapper);
    final Set<SegmentStatusInCluster> publishedSegments = new HashSet<>(Arrays.asList(
        new SegmentStatusInCluster(publishedCompactedSegment1, true, 2, null, false),
        new SegmentStatusInCluster(publishedCompactedSegment2, false, 0, null, false),
        new SegmentStatusInCluster(publishedUncompactedSegment3, false, 2, null, false),
        new SegmentStatusInCluster(segment1, true, 2, null, false),
        new SegmentStatusInCluster(segment2, false, 0, null, false)
    ));

    EasyMock.expect(metadataView.getSegments()).andReturn(publishedSegments.iterator()).once();

    EasyMock.replay(request, responseHolder, responseHandler, metadataView);
    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = segmentsTable.scan(dataContext, Collections.emptyList(), null).toList();
    rows.sort((Object[] row1, Object[] row2) -> ((Comparable) row1[0]).compareTo(row2[0]));

    // total segments = 8
    Assert.assertEquals(8, rows.size());
    // Verify value types.
    verifyTypes(rows, SystemSchema.SEGMENTS_SIGNATURE);

    // segments test1, test2  are published and available.
    Object[] segment1Expected = new Object[]{
        // segment_id, datasource
        "test1_2010-01-01T00:00:00.000Z_2011-01-01T00:00:00.000Z_version1", "test1",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2010-01-01T00:00:00.000Z", "2011-01-01T00:00:00.000Z", 100L, "version1", 0L, 1L, 3L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        0L, 1L, 1L, 0L, 1L, MAPPER.writeValueAsString(SHARD_SPEC),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", "[\"proj1\",\"proj2\"]", null, 2L
    };
    Assert.assertArrayEquals(segment1Expected, rows.get(0));
    Object[] segment2Expected = new Object[]{
        // segment_id, datasource
        "test2_2011-01-01T00:00:00.000Z_2012-01-01T00:00:00.000Z_version2", "test2",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2011-01-01T00:00:00.000Z", "2012-01-01T00:00:00.000Z", 100L, "version2", 0L, 2L, 3L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        1L, 1L, 1L, 0L, 0L, MAPPER.writeValueAsString(SHARD_SPEC),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", null, null, 0L
    };
    Assert.assertArrayEquals(segment2Expected, rows.get(1));
    //segment test3 is unpublished and has a NumberedShardSpec with partitionNum = 2, is served by historical but unpublished or unused
    Object[] segment3Expected = new Object[]{
        // segment_id, datasource
        "test3_2012-01-01T00:00:00.000Z_2013-01-01T00:00:00.000Z_version3_2", "test3",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2012-01-01T00:00:00.000Z", "2013-01-01T00:00:00.000Z", 100L, "version3", 2L, 1L, 2L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        0L, 0L, 1L, 0L, 0L, MAPPER.writeValueAsString(new NumberedShardSpec(2, 3)),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", null, null, -1L
    };
    Assert.assertArrayEquals(segment3Expected, rows.get(2));
    // segments test4, test5 are not published but available (realtime segments)
    Object[] segment4Expected = new Object[]{
        // segment_id, datasource
        "test4_2014-01-01T00:00:00.000Z_2015-01-01T00:00:00.000Z_version4", "test4",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2014-01-01T00:00:00.000Z", "2015-01-01T00:00:00.000Z", 100L, "version4", 0L, 1L, 0L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        1L, 0L, 1L, 1L, 0L, MAPPER.writeValueAsString(SHARD_SPEC),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", null, null, -1L
    };
    Assert.assertArrayEquals(segment4Expected, rows.get(3));
    Object[] segment5Expected = new Object[]{
        // segment_id, datasource
        "test5_2015-01-01T00:00:00.000Z_2016-01-01T00:00:00.000Z_version5", "test5",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2015-01-01T00:00:00.000Z", "2016-01-01T00:00:00.000Z", 100L, "version5", 0L, 1L, 0L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        1L, 0L, 1L, 1L, 0L, MAPPER.writeValueAsString(SHARD_SPEC),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", null, null, -1L
    };
    Assert.assertArrayEquals(segment5Expected, rows.get(4));

    // wikipedia segment 1 and segment 2 are published and unavailable and compacted, num_replicas is 0
    Object[] wikiSegment1Expected = new Object[]{
        // segment_id, datasource
        "wikipedia1_2007-01-01T00:00:00.000Z_2008-01-01T00:00:00.000Z_version1", "wikipedia1",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2007-01-01T00:00:00.000Z", "2008-01-01T00:00:00.000Z", 53_000L, "version1", 0L, 0L, 0L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        0L, 1L, 0L, 0L, 1L, MAPPER.writeValueAsString(SHARD_SPEC),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", null, MAPPER.writeValueAsString(expectedCompactionState), 2L
    };
    Assert.assertArrayEquals(wikiSegment1Expected, rows.get(5));
    Object[] wikiSegment2Expected = new Object[]{
        // segment_id, datasource
        "wikipedia2_2008-01-01T00:00:00.000Z_2009-01-01T00:00:00.000Z_version2", "wikipedia2",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2008-01-01T00:00:00.000Z", "2009-01-01T00:00:00.000Z", 83_000L, "version2", 0L, 0L, 0L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        1L, 1L, 0L, 0L, 0L, MAPPER.writeValueAsString(SHARD_SPEC),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", null, MAPPER.writeValueAsString(expectedCompactionState), 0L
    };
    Assert.assertArrayEquals(wikiSegment2Expected, rows.get(6));
    // wikipedia segment 3 are not compacted, and is projection aware.
    Object[] wikiSegment3Expected = new Object[]{
        // segment_id, datasource
        "wikipedia3_2009-01-01T00:00:00.000Z_2010-01-01T00:00:00.000Z_version3", "wikipedia3",
        // start, end, size, version, partition_num, num_replicas, numRows
        "2009-01-01T00:00:00.000Z", "2010-01-01T00:00:00.000Z", 47_000L, "version3", 0L, 0L, 0L,
        //  is_active, is_published, is_available, is_realtime, is_overshadowed, shard_spec
        1L, 1L, 0L, 0L, 0L, MAPPER.writeValueAsString(SHARD_SPEC),
        // dimensions, metrics, projections, last_compaction_state, replication_factor
        "[\"dim1\",\"dim2\"]", "[\"met1\",\"met2\"]", "[]", null, 2L
    };
    Assert.assertArrayEquals(wikiSegment3Expected, rows.get(7));
  }

  @Test
  public void testSegmentsTableWithProjection() throws JsonProcessingException
  {
    final SegmentsTable segmentsTable = new SegmentsTable(druidSchema, metadataView, MAPPER, authMapper);
    final Set<SegmentStatusInCluster> publishedSegments = new HashSet<>(Arrays.asList(
        new SegmentStatusInCluster(publishedCompactedSegment1, true, 2, null, false),
        new SegmentStatusInCluster(publishedCompactedSegment2, false, 0, null, false),
        new SegmentStatusInCluster(publishedUncompactedSegment3, false, 2, null, false),
        new SegmentStatusInCluster(segment1, true, 2, null, false),
        new SegmentStatusInCluster(segment2, false, 0, null, false)
    ));

    EasyMock.expect(metadataView.getSegments()).andReturn(publishedSegments.iterator()).once();

    EasyMock.replay(request, responseHolder, responseHandler, metadataView);
    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = segmentsTable.scan(
        dataContext,
        Collections.emptyList(),
        new int[]{
            SystemSchema.SEGMENTS_SIGNATURE.indexOf("last_compaction_state"),
            SystemSchema.SEGMENTS_SIGNATURE.indexOf("segment_id")
        }
    ).toList();
    rows.sort((Object[] row1, Object[] row2) -> ((Comparable) row1[1]).compareTo(row2[1]));

    // total segments = 8
    // segments test1, test2  are published and available
    // segment test3 is served by historical but unpublished or unused
    // segments test4, test5 are not published but available (realtime segments)
    // segment test2 is both published and served by a realtime server.

    Assert.assertEquals(8, rows.size());

    Assert.assertNull(null, rows.get(0)[0]);
    Assert.assertEquals("test1_2010-01-01T00:00:00.000Z_2011-01-01T00:00:00.000Z_version1", rows.get(0)[1]);

    Assert.assertNull(null, rows.get(1)[0]);
    Assert.assertEquals("test2_2011-01-01T00:00:00.000Z_2012-01-01T00:00:00.000Z_version2", rows.get(1)[1]);

    Assert.assertNull(null, rows.get(2)[0]);
    Assert.assertEquals("test3_2012-01-01T00:00:00.000Z_2013-01-01T00:00:00.000Z_version3_2", rows.get(2)[1]);

    Assert.assertNull(null, rows.get(3)[0]);
    Assert.assertEquals("test4_2014-01-01T00:00:00.000Z_2015-01-01T00:00:00.000Z_version4", rows.get(3)[1]);

    Assert.assertNull(null, rows.get(4)[0]);
    Assert.assertEquals("test5_2015-01-01T00:00:00.000Z_2016-01-01T00:00:00.000Z_version5", rows.get(4)[1]);

    Assert.assertEquals(MAPPER.writeValueAsString(expectedCompactionState), rows.get(5)[0]);
    Assert.assertEquals("wikipedia1_2007-01-01T00:00:00.000Z_2008-01-01T00:00:00.000Z_version1", rows.get(5)[1]);

    Assert.assertEquals(MAPPER.writeValueAsString(expectedCompactionState), rows.get(6)[0]);
    Assert.assertEquals("wikipedia2_2008-01-01T00:00:00.000Z_2009-01-01T00:00:00.000Z_version2", rows.get(6)[1]);

    Assert.assertNull(null, rows.get(7)[0]);
    Assert.assertEquals("wikipedia3_2009-01-01T00:00:00.000Z_2010-01-01T00:00:00.000Z_version3", rows.get(7)[1]);

    // Verify value types.
    verifyTypes(
        rows,
        RowSignature.builder()
                    .add("last_compaction_state", ColumnType.STRING)
                    .add("segment_id", ColumnType.STRING)
                    .build()
    );
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
                                                         coordinatorClient,
                                                         MAPPER
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

    EasyMock.expect(coordinatorClient.findCurrentLeader())
            .andReturn(Futures.immediateFuture(new URI(coordinator.getDruidNode().getHostAndPortToUse())))
            .once();
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
    final Long nonLeader = null;
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            null
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
        startTimeStr,
        version,
        null
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            "{\"brokerKey\":\"brokerValue\",\"brokerKey2\":\"brokerValue2\"}"
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            "{\"overlordKey\":\"overlordValue\"}"
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            null
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
            startTimeStr,
            version,
            null
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
        startTimeStr,
        version,
        null
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
      String startTime,
      String version,
      String labels
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
        startTime,
        version,
        labels
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
                MAPPER.readValue(json, new TypeReference<List<TaskStatusPlus>>() {}).iterator()
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
                MAPPER.readValue(json, new TypeReference<List<TaskStatusPlus>>() {}).iterator()
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
                  + "\t\"id\": \"wikipedia_supervisor\",\n"
                  + "\t\"dataSource\": \"wikipedia\",\n"
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
                MAPPER.readValue(json, new TypeReference<List<SupervisorStatus>>() {}).iterator()
            )
        )
    );

    EasyMock.replay(overlordClient);
    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = supervisorTable.scan(dataContext).toList();

    Object[] row0 = rows.get(0);
    Assert.assertEquals("wikipedia_supervisor", row0[0].toString());
    Assert.assertEquals("wikipedia", row0[1].toString());
    Assert.assertEquals("UNHEALTHY_SUPERVISOR", row0[2].toString());
    Assert.assertEquals("UNABLE_TO_CONNECT_TO_STREAM", row0[3].toString());
    Assert.assertEquals(0L, row0[4]);
    Assert.assertEquals("kafka", row0[5].toString());
    Assert.assertEquals("wikipedia", row0[6].toString());
    Assert.assertEquals(0L, row0[7]);
    Assert.assertEquals(
        "{\"type\":\"kafka\",\"dataSchema\":{\"dataSource\":\"wikipedia\"},\"context\":null,\"suspended\":false}",
        row0[8].toString()
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
                MAPPER.readValue(json, new TypeReference<List<SupervisorStatus>>() {}).iterator()
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

  @Test
  public void testPropertiesTable()
  {
    SystemServerPropertiesTable propertiesTable = EasyMock.createMockBuilder(SystemServerPropertiesTable.class)
                                                          .withConstructor(druidNodeDiscoveryProvider, authMapper, httpClient, MAPPER)
                                                          .createMock();

    EasyMock.replay(propertiesTable);

    List<Object[]> expectedRows = new ArrayList<>();

    mockNodeDiscovery(NodeRole.BROKER);
    mockNodeDiscovery(NodeRole.ROUTER);
    mockNodeDiscovery(NodeRole.HISTORICAL);
    mockNodeDiscovery(NodeRole.OVERLORD);
    mockNodeDiscovery(NodeRole.PEON);
    mockNodeDiscovery(NodeRole.INDEXER);

    mockNodeDiscovery(NodeRole.COORDINATOR, coordinator, coordinator2);
    HttpResponse coordinatorHttpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StringFullResponseHolder coordinatorResponseHolder = new StringFullResponseHolder(coordinatorHttpResponse, StandardCharsets.UTF_8);
    String coordinatorJson = "{\"druid.test-key\": \"test-value\"}";
    coordinatorResponseHolder.addChunk(coordinatorJson);
    expectedRows.add(new Object[]{
        coordinator.getDruidNode().getHostAndPortToUse(),
        coordinator.getDruidNode().getServiceName(),
        ImmutableList.of(coordinator.getNodeRole().getJsonName()).toString(),
        "druid.test-key", "test-value"
    });

    HttpResponse coordinator2HttpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StringFullResponseHolder coordinator2ResponseHolder = new StringFullResponseHolder(coordinator2HttpResponse, StandardCharsets.UTF_8);
    String coordinator2Json = "{\"druid.test-key3\": \"test-value3\"}";
    coordinator2ResponseHolder.addChunk(coordinator2Json);
    expectedRows
        .add(new Object[]{
            coordinator2.getDruidNode().getHostAndPortToUse(),
            coordinator2.getDruidNode().getServiceName(),
            ImmutableList.of(coordinator2.getNodeRole().getJsonName()).toString(),
            "druid.test-key3", "test-value3"
        });

    mockNodeDiscovery(NodeRole.MIDDLE_MANAGER, middleManager);
    HttpResponse middleManagerHttpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    StringFullResponseHolder middleManagerResponseHolder = new StringFullResponseHolder(middleManagerHttpResponse, StandardCharsets.UTF_8);
    String middleManagerJson = "{\n"
                               + "\"druid.test-key\": \"test-value\",\n"
                               + "\"druid.test-key2\": \"test-value2\"\n"
                               + "}";
    middleManagerResponseHolder.addChunk(middleManagerJson);
    expectedRows
        .add(new Object[]{
            middleManager.getDruidNode().getHostAndPortToUse(),
            middleManager.getDruidNode().getServiceName(),
            ImmutableList.of(middleManager.getNodeRole().getJsonName()).toString(),
            "druid.test-key", "test-value"
        });
    expectedRows
        .add(new Object[]{
            middleManager.getDruidNode().getHostAndPortToUse(),
            middleManager.getDruidNode().getServiceName(),  
            ImmutableList.of(middleManager.getNodeRole().getJsonName()).toString(),
            "druid.test-key2", "test-value2"
        });

    Map<String, ListenableFuture<StringFullResponseHolder>> urlToResponse = ImmutableMap.of(
        getStatusPropertiesUrl(coordinator), Futures.immediateFuture(coordinatorResponseHolder),
        getStatusPropertiesUrl(coordinator2), Futures.immediateFuture(coordinator2ResponseHolder),
        getStatusPropertiesUrl(middleManager), Futures.immediateFuture(middleManagerResponseHolder)
    );

    EasyMock.expect(
        httpClient.go(
            EasyMock.isA(Request.class),
            EasyMock.isA(StringFullResponseHandler.class)
        )
    ).andAnswer(() -> {
      Request req = (Request) EasyMock.getCurrentArguments()[0];
      String url = req.getUrl().toString();

      ListenableFuture<StringFullResponseHolder> future = urlToResponse.get(url);
      if (future != null) {
        return future;
      }
      return Futures.immediateFailedFuture(new AssertionError("Unexpected URL: " + url));
    }).times(3);

    EasyMock.replay(druidNodeDiscoveryProvider, responseHandler, httpClient);

    DataContext dataContext = createDataContext(Users.SUPER);
    final List<Object[]> rows = propertiesTable.scan(dataContext).toList();
    expectedRows.sort((Object[] row1, Object[] row2) -> ((Comparable) row1[0]).compareTo(row2[0]));
    rows.sort((Object[] row1, Object[] row2) -> ((Comparable) row1[0]).compareTo(row2[0]));
    Assert.assertEquals(expectedRows.size(), rows.size());
    for (int i = 0; i < expectedRows.size(); i++) {
      Assert.assertArrayEquals(expectedRows.get(i), rows.get(i));
    }

  }

  private String getStatusPropertiesUrl(DiscoveryDruidNode discoveryDruidNode)
  {
    return discoveryDruidNode.getDruidNode().getUriToUse().resolve("/status/properties").toString();
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
            expectedClass = String.class;
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

  private DruidNodeDiscovery mockNodeDiscovery(NodeRole nodeRole, DiscoveryDruidNode... discoveryDruidNodes)
  {
    final DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(nodeRole)).andReturn(druidNodeDiscovery).once();
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(ImmutableList.copyOf(discoveryDruidNodes)).once();
    EasyMock.replay(druidNodeDiscovery);
    return druidNodeDiscovery;
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
