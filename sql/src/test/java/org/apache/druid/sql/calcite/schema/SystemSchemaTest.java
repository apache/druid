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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.ReflectionQueryToolChestWarehouse;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestServerInventoryView;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class SystemSchemaTest extends CalciteTestBase
{
  private static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();

  private static final List<InputRow> ROWS1 = ImmutableList.of(
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  private static final List<InputRow> ROWS2 = ImmutableList.of(
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  private SystemSchema schema;
  private SpecificSegmentsQuerySegmentWalker walker;
  private DruidLeaderClient client;
  private TimelineServerView serverView;
  private ObjectMapper mapper;
  private FullResponseHolder responseHolder;
  private SystemSchema.BytesAccumulatingResponseHandler responseHandler;
  private Request request;
  private DruidSchema druidSchema;
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpClass()
  {
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    conglomerate = conglomerateCloserPair.lhs;
    resourceCloser = conglomerateCloserPair.rhs;
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
    mapper = TestHelper.makeJsonMapper();
    responseHolder = EasyMock.createMock(FullResponseHolder.class);
    responseHandler = EasyMock.createMockBuilder(SystemSchema.BytesAccumulatingResponseHandler.class)
                              .withConstructor()
                              .addMockedMethod(
                                      "handleResponse",
                                      HttpResponse.class,
                                      HttpResponseHandler.TrafficCop.class
                                  )
                              .addMockedMethod("getStatus")
                              .createMock();
    request = EasyMock.createMock(Request.class);

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
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE1)
                   .interval(Intervals.of("2001/P1Y"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index2
    );

    druidSchema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        new TestServerInventoryView(walker.getSegments()),
        PLANNER_CONFIG_DEFAULT,
        new NoopViewManager(),
        new NoopEscalator()
    );
    druidSchema.start();
    druidSchema.awaitInitialization();
    schema = new SystemSchema(
        druidSchema,
        serverView,
        EasyMock.createStrictMock(AuthorizerMapper.class),
        client,
        client,
        mapper
    );
  }

  private final DataSegment segment1 = new DataSegment(
      "test1",
      Intervals.of("2017/2018"),
      "version1",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L,
      DataSegment.PruneLoadSpecHolder.DEFAULT
  );
  private final DataSegment segment2 = new DataSegment(
      "test2",
      Intervals.of("2017/2018"),
      "version2",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L,
      DataSegment.PruneLoadSpecHolder.DEFAULT
  );
  private final DataSegment segment3 = new DataSegment(
      "test3",
      Intervals.of("2017/2018"),
      "version3",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L,
      DataSegment.PruneLoadSpecHolder.DEFAULT
  );
  private final DataSegment segment4 = new DataSegment(
      "test4",
      Intervals.of("2017/2018"),
      "version4",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L,
      DataSegment.PruneLoadSpecHolder.DEFAULT
  );
  private final DataSegment segment5 = new DataSegment(
      "test5",
      Intervals.of("2017/2018"),
      "version5",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L,
      DataSegment.PruneLoadSpecHolder.DEFAULT
  );
  private final ImmutableDruidDataSource dataSource1 = new ImmutableDruidDataSource(
      "ds1",
      ImmutableMap.of("prop1", "val1", "prop2", "val2"),
      ImmutableSortedMap.of(segment1.getIdentifier(), segment1)
  );
  private final ImmutableDruidDataSource dataSource2 = new ImmutableDruidDataSource(
      "ds2",
      ImmutableMap.of("prop1", "val1", "prop2", "val2"),
      ImmutableSortedMap.of(segment2.getIdentifier(), segment2)
  );
  private final ImmutableDruidDataSource dataSource3 = new ImmutableDruidDataSource(
      "ds3",
      ImmutableMap.of("prop1", "val1", "prop2", "val2"),
      ImmutableSortedMap.of(segment2.getIdentifier(), segment3)
  );

  private final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
  private final DirectDruidClient client1 = new DirectDruidClient(
      new ReflectionQueryToolChestWarehouse(),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER,
      new DefaultObjectMapper(),
      httpClient,
      "http",
      "foo",
      new NoopServiceEmitter()
  );
  private final DirectDruidClient client2 = new DirectDruidClient(
      new ReflectionQueryToolChestWarehouse(),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER,
      new DefaultObjectMapper(),
      httpClient,
      "http",
      "foo2",
      new NoopServiceEmitter()
  );
  private final QueryableDruidServer queryableDruidServer1 = new QueryableDruidServer(
      new DruidServer(
          "server1", "localhost", null, 0, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 0)
          .addDataSegment(segment1).addDataSegment(segment2), client1
  );

  private final QueryableDruidServer queryableDruidServer2 = new QueryableDruidServer(
      new DruidServer(
          "server2", "server2", null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
          .addDataSegment(segment2).addDataSegment(segment4).addDataSegment(segment5), client2
  );
  private final Map<String, QueryableDruidServer> serverViewClients = ImmutableMap.of(
      "server1",
      queryableDruidServer1,
      "server2",
      queryableDruidServer2
  );

  private final TaskStatusPlus task1 = new TaskStatusPlus(
      "task1",
      "index",
      DateTimes.nowUtc(),
      DateTimes.nowUtc(),
      TaskState.RUNNING,
      RunnerTaskState.RUNNING,
      -1L,
      TaskLocation.create("testHost", 1010, -1),
      "ds_test",
      null
  );

  private final TaskStatusPlus task2 = new TaskStatusPlus(
      "task2",
      "index",
      DateTimes.nowUtc(),
      DateTimes.nowUtc(),
      TaskState.SUCCESS,
      RunnerTaskState.NONE,
      1000L,
      TaskLocation.unknown(),
      "ds_test",
      null
  );

  private final TaskStatusPlus task3 = new TaskStatusPlus(
      "task3",
      "index",
      DateTimes.nowUtc(),
      DateTimes.nowUtc(),
      TaskState.FAILED,
      RunnerTaskState.NONE,
      1000L,
      TaskLocation.unknown(),
      "ds_test",
      null
  );

  @Test
  public void testGetTableMap()
  {
    Assert.assertEquals(ImmutableSet.of("segments", "servers", "segment_servers", "tasks"), schema.getTableNames());

    final Map<String, Table> tableMap = schema.getTableMap();
    Assert.assertEquals(ImmutableSet.of("segments", "servers", "segment_servers", "tasks"), tableMap.keySet());
    final SystemSchema.SegmentsTable segmentsTable = (SystemSchema.SegmentsTable) schema.getTableMap().get("segments");
    final RelDataType rowType = segmentsTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(13, fields.size());

    final SystemSchema.TasksTable tasksTable = (SystemSchema.TasksTable) schema.getTableMap().get("tasks");
    final RelDataType sysRowType = tasksTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> sysFields = sysRowType.getFieldList();
    Assert.assertEquals(10, sysFields.size());

    Assert.assertEquals("task_id", sysFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, sysFields.get(0).getType().getSqlTypeName());
  }

  @Test
  @Ignore
  public void testSegmentsTable() throws Exception
  {
    // total segments = 5
    // segments 1,2,3 are published
    // segments 1,2,4,5 are served
    // segment 3 is published but not served
    // segment 2 is served by 2 servers, so num_replicas=2

    final SystemSchema.SegmentsTable segmentsTable = EasyMock.createMockBuilder(SystemSchema.SegmentsTable.class).withConstructor(
        druidSchema, client, mapper, responseHandler).createMock();
    EasyMock.replay(segmentsTable);

    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/metadata/segments")).andReturn(request).anyTimes();
    SettableFuture<InputStream> future = SettableFuture.create();
    EasyMock.expect(client.goStream(request, responseHandler)).andReturn(future).once();
    final int ok = HttpServletResponse.SC_OK;
    EasyMock.expect(responseHandler.getStatus()).andReturn(ok).once();

    EasyMock.expect(request.getUrl()).andReturn(new URL("http://test-host:1234/druid/coordinator/v1/metadata/segments")).anyTimes();

    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    final String json = "[{\n"
                        + "\t\"dataSource\": \"wikipedia-kafka\",\n"
                        + "\t\"interval\": \"2018-08-07T23:00:00.000Z/2018-08-08T00:00:00.000Z\",\n"
                        + "\t\"version\": \"2018-08-07T23:00:00.059Z\",\n"
                        + "\t\"loadSpec\": {\n"
                        + "\t\t\"type\": \"local\",\n"
                        + "\t\t\"path\": \"/var/druid/segments/wikipedia-kafka/2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z/2018-08-07T23:00:00.059Z/51/1578eb79-0e44-4b41-a87b-65e40c52be53/index.zip\"\n"
                        + "\t},\n"
                        + "\t\"dimensions\": \"isRobot,channel,flags,isUnpatrolled,page,diffUrl,comment,isNew,isMinor,user,namespace,commentLength,deltaBucket,cityName,countryIsoCode,countryName,isAnonymous,regionIsoCode,regionName,added,deleted,delta\",\n"
                        + "\t\"metrics\": \"count,user_unique\",\n"
                        + "\t\"shardSpec\": {\n"
                        + "\t\t\"type\": \"numbered\",\n"
                        + "\t\t\"partitionNum\": 51,\n"
                        + "\t\t\"partitions\": 0\n"
                        + "\t},\n"
                        + "\t\"binaryVersion\": 9,\n"
                        + "\t\"size\": 47406,\n"
                        + "\t\"identifier\": \"wikipedia-kafka_2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z_2018-08-07T23:00:00.059Z_51\"\n"
                        + "}, {\n"
                        + "\t\"dataSource\": \"wikipedia-kafka\",\n"
                        + "\t\"interval\": \"2018-08-07T18:00:00.000Z/2018-08-07T19:00:00.000Z\",\n"
                        + "\t\"version\": \"2018-08-07T18:00:00.117Z\",\n"
                        + "\t\"loadSpec\": {\n"
                        + "\t\t\"type\": \"local\",\n"
                        + "\t\t\"path\": \"/var/druid/segments/wikipedia-kafka/2018-08-07T18:00:00.000Z_2018-08-07T19:00:00.000Z/2018-08-07T18:00:00.117Z/9/a2646827-b782-424c-9eed-e48aa448d2c5/index.zip\"\n"
                        + "\t},\n"
                        + "\t\"dimensions\": \"isRobot,channel,flags,isUnpatrolled,page,diffUrl,comment,isNew,isMinor,user,namespace,commentLength,deltaBucket,cityName,countryIsoCode,countryName,isAnonymous,metroCode,regionIsoCode,regionName,added,deleted,delta\",\n"
                        + "\t\"metrics\": \"count,user_unique\",\n"
                        + "\t\"shardSpec\": {\n"
                        + "\t\t\"type\": \"numbered\",\n"
                        + "\t\t\"partitionNum\": 9,\n"
                        + "\t\t\"partitions\": 0\n"
                        + "\t},\n"
                        + "\t\"binaryVersion\": 9,\n"
                        + "\t\"size\": 83846,\n"
                        + "\t\"identifier\": \"wikipedia-kafka_2018-08-07T18:00:00.000Z_2018-08-07T19:00:00.000Z_2018-08-07T18:00:00.117Z_9\"\n"
                        + "}, {\n"
                        + "\t\"dataSource\": \"wikipedia-kafka\",\n"
                        + "\t\"interval\": \"2018-08-07T23:00:00.000Z/2018-08-08T00:00:00.000Z\",\n"
                        + "\t\"version\": \"2018-08-07T23:00:00.059Z\",\n"
                        + "\t\"loadSpec\": {\n"
                        + "\t\t\"type\": \"local\",\n"
                        + "\t\t\"path\": \"/var/druid/segments/wikipedia-kafka/2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z/2018-08-07T23:00:00.059Z/50/87c5457e-c39b-4c03-9df8-e2b20b210dfc/index.zip\"\n"
                        + "\t},\n"
                        + "\t\"dimensions\": \"isRobot,channel,flags,isUnpatrolled,page,diffUrl,comment,isNew,isMinor,user,namespace,commentLength,deltaBucket,cityName,countryIsoCode,countryName,isAnonymous,metroCode,regionIsoCode,regionName,added,deleted,delta\",\n"
                        + "\t\"metrics\": \"count,user_unique\",\n"
                        + "\t\"shardSpec\": {\n"
                        + "\t\t\"type\": \"numbered\",\n"
                        + "\t\t\"partitionNum\": 50,\n"
                        + "\t\t\"partitions\": 0\n"
                        + "\t},\n"
                        + "\t\"binaryVersion\": 9,\n"
                        + "\t\"size\": 53527,\n"
                        + "\t\"identifier\": \"wikipedia-kafka_2018-08-07T23:00:00.000Z_2018-08-08T00:00:00.000Z_2018-08-07T23:00:00.059Z_50\"\n"
                        + "}]";
    byte[] bytesToWrite = json.getBytes(StandardCharsets.UTF_8);
    in.add(bytesToWrite);
    in.done();
    future.set(in);
    //String jsonValue = mapper.writeValueAsString(ImmutableList.of(dataSource1, dataSource2, dataSource3));
    //EasyMock.expect(responseHolder.getContent()).andReturn(jsonValue).once();

    /*EasyMock.expect(serverView.getClients())
            .andReturn(serverViewClients)
            .once();*/
    EasyMock.replay(client, request, responseHolder, responseHandler);
    DataContext dataContext = new DataContext()
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
      public Object get(String name)
      {
        return null;
      }
    };
    Enumerable<Object[]> rows = segmentsTable.scan(dataContext);
    Enumerator enumerator = rows.enumerator();
    while (enumerator.moveNext()) {
      System.out.println(enumerator.current());
    }

    Assert.assertEquals(5, rows.count());
    Enumerable<Object[]> distinctRows = rows.distinct();
    Assert.assertEquals(rows.count(), distinctRows.count());
    for (Object[] row : rows) {
      String ds = (String) row[1];
      int replicas = (int) row[7];
      long isAvailable = (long) row[9];
      if ("test3".equals(ds)) {
        //segment3 is published but not served
        Assert.assertEquals(0, isAvailable);
        Assert.assertEquals(0, replicas);
      } else if ("test2".equals(ds)) {
        Assert.assertEquals(1, isAvailable);
        Assert.assertEquals(2, replicas);
      } else {
        // all other segments are available with 1 replica
        Assert.assertEquals(1, isAvailable);
        Assert.assertEquals(1, replicas);
      }
    }

  }

  @Test
  public void testServersTable()
  {
    final SystemSchema.ServersTable serversTable = (SystemSchema.ServersTable) schema.getTableMap().get("servers");
    final RelDataType rowType = serversTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();
    Assert.assertEquals(7, fields.size());

    Assert.assertEquals("server", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(0).getType().getSqlTypeName());

    EasyMock.expect(serverView.getClients())
            .andReturn(serverViewClients)
            .once();
    EasyMock.replay(serverView);
    DataContext dataContext = new DataContext()
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
      public Object get(String name)
      {
        return null;
      }
    };
    Enumerable<Object[]> rows = serversTable.scan(dataContext);
    Assert.assertEquals(2, rows.count());
    Object[] row1 = rows.first();
    Assert.assertEquals("localhost", row1[0]);
    Assert.assertEquals("realtime", row1[3].toString());
    Object[] row2 = rows.last();
    Assert.assertEquals("server2", row2[0]);
    Assert.assertEquals("historical", row2[3].toString());
  }

  @Test
  public void testTasksTable() throws Exception
  {

    SystemSchema.TasksTable tasksTable = EasyMock.createMockBuilder(SystemSchema.TasksTable.class).withConstructor(client, mapper, responseHandler).createMock();
    EasyMock.replay(tasksTable);
    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/indexer/v1/tasks")).andReturn(request).anyTimes();
    SettableFuture<InputStream> future = SettableFuture.create();
    EasyMock.expect(client.goStream(request, responseHandler)).andReturn(future).once();
    final int ok = HttpServletResponse.SC_OK;
    EasyMock.expect(responseHandler.getStatus()).andReturn(ok).once();
    EasyMock.expect(request.getUrl()).andReturn(new URL("http://test-host:1234/druid/indexer/v1/tasks")).anyTimes();

    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();


    String json = "[{\n"
                  + "\t\"id\": \"index_wikipedia_2018-09-20T22:33:44.911Z\",\n"
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
    byte[] bytesToWrite = json.getBytes(StandardCharsets.UTF_8);
    in.add(bytesToWrite);
    in.done();
    future.set(in);

    EasyMock.replay(client, request, responseHandler);
    DataContext dataContext = new DataContext()
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
      public Object get(String name)
      {
        return null;
      }
    };
    Enumerable<Object[]> rows = tasksTable.scan(dataContext);
    Enumerator<Object[]> enumerator = rows.enumerator();

    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row1 = enumerator.current();
    Assert.assertEquals("index_wikipedia_2018-09-20T22:33:44.911Z", row1[0].toString());
    Assert.assertEquals("FAILED", row1[5].toString());
    Assert.assertEquals("NONE", row1[6].toString());
    Assert.assertEquals(-1L, row1[7]);
    Assert.assertEquals("testHost:1234", row1[8]);

    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row2 = enumerator.current();
    Assert.assertEquals("index_wikipedia_2018-09-21T18:38:47.773Z", row2[0].toString());
    Assert.assertEquals("RUNNING", row2[5].toString());
    Assert.assertEquals("RUNNING", row2[6].toString());
    Assert.assertEquals(null, row2[7]);
    Assert.assertEquals("192.168.1.6:8100", row2[8]);

    Assert.assertEquals(false, enumerator.moveNext());
  }

}
