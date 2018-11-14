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
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestServerInventoryView;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
  private BytesAccumulatingResponseHandler responseHandler;
  private Request request;
  private DruidSchema druidSchema;
  private AuthorizerMapper authMapper;
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
    authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return (authenticationResult, resource, action) -> new Access(true);
      }
    };

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

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate)
        .add(segment1, index1)
        .add(segment2, index2)
        .add(segment2, index2)
        .add(segment3, index2);

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
      Intervals.of("2010/2011"),
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
      Intervals.of("2011/2012"),
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
      Intervals.of("2012/2013"),
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

  private final ImmutableDruidServer druidServer1 = new ImmutableDruidServer(
      new DruidServerMetadata("server1", "localhost:0000", null, 5L, ServerType.REALTIME, DruidServer.DEFAULT_TIER, 0),
      1L,
      null,
      ImmutableMap.of("segment1", segment1, "segment2", segment2)
  );

  private final ImmutableDruidServer druidServer2 = new ImmutableDruidServer(
      new DruidServerMetadata("server2", "server2:1234", null, 5L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
      1L,
      null,
      ImmutableMap.of("segment3", segment3, "segment4", segment4, "segment5", segment5)
  );

  private final List<ImmutableDruidServer> immutableDruidServers = ImmutableList.of(druidServer1, druidServer2);

  @Test
  public void testGetTableMap()
  {
    Assert.assertEquals(ImmutableSet.of("segments", "servers", "server_segments", "tasks"), schema.getTableNames());

    final Map<String, Table> tableMap = schema.getTableMap();
    Assert.assertEquals(ImmutableSet.of("segments", "servers", "server_segments", "tasks"), tableMap.keySet());
    final SystemSchema.SegmentsTable segmentsTable = (SystemSchema.SegmentsTable) schema.getTableMap().get("segments");
    final RelDataType rowType = segmentsTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(13, fields.size());

    final SystemSchema.TasksTable tasksTable = (SystemSchema.TasksTable) schema.getTableMap().get("tasks");
    final RelDataType sysRowType = tasksTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> sysFields = sysRowType.getFieldList();
    Assert.assertEquals(13, sysFields.size());

    Assert.assertEquals("task_id", sysFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, sysFields.get(0).getType().getSqlTypeName());

    final SystemSchema.ServersTable serversTable = (SystemSchema.ServersTable) schema.getTableMap().get("servers");
    final RelDataType serverRowType = serversTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> serverFields = serverRowType.getFieldList();
    Assert.assertEquals(8, serverFields.size());
    Assert.assertEquals("server", serverFields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, serverFields.get(0).getType().getSqlTypeName());
  }

  @Test
  public void testSegmentsTable() throws Exception
  {
    // total segments = 6
    // segments 1,2,3 are published and available
    // segments 4,5,6  are published but unavailable
    // segment 3 is published but not served
    // segment 2 is served by 2 servers, so num_replicas=2

    final SystemSchema.SegmentsTable segmentsTable = EasyMock
        .createMockBuilder(SystemSchema.SegmentsTable.class)
        .withConstructor(druidSchema, client, mapper, responseHandler, authMapper)
        .createMock();
    EasyMock.replay(segmentsTable);

    EasyMock
        .expect(client.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/metadata/segments", false))
        .andReturn(request)
        .anyTimes();
    SettableFuture<InputStream> future = SettableFuture.create();
    EasyMock.expect(client.goAsync(request, responseHandler)).andReturn(future).once();
    final int ok = HttpServletResponse.SC_OK;
    EasyMock.expect(responseHandler.getStatus()).andReturn(ok).anyTimes();

    EasyMock
        .expect(request.getUrl())
        .andReturn(new URL("http://test-host:1234/druid/coordinator/v1/metadata/segments"))
        .anyTimes();

    AppendableByteArrayInputStream in = new AppendableByteArrayInputStream();
    //published but unavailable segments
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
                        + "\t\t\"type\": \"none\",\n"
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
                        + "\t\t\"type\": \"none\",\n"
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
                        + "\t\t\"type\": \"none\",\n"
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
        return CalciteTests.SUPER_USER_AUTH_RESULT;
      }
    };
    Enumerable<Object[]> rows = segmentsTable.scan(dataContext);
    Enumerator<Object[]> enumerator = rows.enumerator();

    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row1 = enumerator.current();
    //segment 6 is published and unavailable, num_replicas is 0
    Assert.assertEquals(1L, row1[9]);
    Assert.assertEquals(0L, row1[7]);
    Assert.assertEquals(0L, row1[8]); //numRows = 0

    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(true, enumerator.moveNext());

    Object[] row5 = enumerator.current();
    //segment 2 is published and has 2 replicas
    Assert.assertEquals(1L, row5[9]);
    Assert.assertEquals(2L, row5[7]);
    Assert.assertEquals(3L, row5[8]);  //numRows = 3
    Assert.assertEquals(true, enumerator.moveNext());
    Assert.assertEquals(false, enumerator.moveNext());

  }

  @Test
  public void testServersTable()
  {

    SystemSchema.ServersTable serversTable = EasyMock.createMockBuilder(SystemSchema.ServersTable.class).withConstructor(serverView, authMapper).createMock();
    EasyMock.replay(serversTable);

    EasyMock.expect(serverView.getDruidServers())
            .andReturn(immutableDruidServers)
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
        return CalciteTests.SUPER_USER_AUTH_RESULT;
      }
    };
    Enumerable<Object[]> rows = serversTable.scan(dataContext);
    Assert.assertEquals(2, rows.count());
    Object[] row1 = rows.first();
    Assert.assertEquals("localhost:0000", row1[0]);
    Assert.assertEquals("realtime", row1[4].toString());
    Object[] row2 = rows.last();
    Assert.assertEquals("server2:1234", row2[0]);
    Assert.assertEquals("historical", row2[4].toString());
  }

  @Test
  public void testServerSegmentsTable()
  {
    SystemSchema.ServerSegmentsTable serverSegmentsTable = EasyMock.createMockBuilder(SystemSchema.ServerSegmentsTable.class)
                                                                   .withConstructor(serverView, authMapper)
                                                                   .createMock();
    EasyMock.replay(serverSegmentsTable);
    EasyMock.expect(serverView.getDruidServers())
            .andReturn(immutableDruidServers)
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
        return CalciteTests.SUPER_USER_AUTH_RESULT;
      }
    };
    Enumerable<Object[]> rows = serverSegmentsTable.scan(dataContext);
    Assert.assertEquals(5, rows.count());

    //server_segments table is the join of servers and segments table
    // it will have 5 rows as follows
    // localhost:0000 |  test1_2010-01-01T00:00:00.000Z_2011-01-01T00:00:00.000Z_version1(segment1)
    // localhost:0000 |  test2_2011-01-01T00:00:00.000Z_2012-01-01T00:00:00.000Z_version2(segment2)
    // server2:1234   |  test3_2012-01-01T00:00:00.000Z_2013-01-01T00:00:00.000Z_version3(segment3)
    // server2:1234   |  test4_2017-01-01T00:00:00.000Z_2018-01-01T00:00:00.000Z_version4(segment4)
    // server2:1234   |  test5_2017-01-01T00:00:00.000Z_2018-01-01T00:00:00.000Z_version5(segment5)

    Enumerator<Object[]> enumerator = rows.enumerator();
    Assert.assertEquals(true, enumerator.moveNext());

    Object[] row1 = rows.first();
    Assert.assertEquals("localhost:0000", row1[0]);
    Assert.assertEquals("test1_2010-01-01T00:00:00.000Z_2011-01-01T00:00:00.000Z_version1", row1[1]);

    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row2 = enumerator.current();
    Assert.assertEquals("localhost:0000", row2[0]);
    Assert.assertEquals("test2_2011-01-01T00:00:00.000Z_2012-01-01T00:00:00.000Z_version2", row2[1]);

    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row3 = enumerator.current();
    Assert.assertEquals("server2:1234", row3[0]);
    Assert.assertEquals("test3_2012-01-01T00:00:00.000Z_2013-01-01T00:00:00.000Z_version3", row3[1]);

    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row4 = enumerator.current();
    Assert.assertEquals("server2:1234", row4[0]);
    Assert.assertEquals("test4_2017-01-01T00:00:00.000Z_2018-01-01T00:00:00.000Z_version4", row4[1]);

    Assert.assertEquals(true, enumerator.moveNext());
    Object[] row5 = rows.last();
    Assert.assertEquals("server2:1234", row5[0]);
    Assert.assertEquals("test5_2017-01-01T00:00:00.000Z_2018-01-01T00:00:00.000Z_version5", row5[1]);

    Assert.assertEquals(false, enumerator.moveNext());
  }

  @Test
  public void testTasksTable() throws Exception
  {

    SystemSchema.TasksTable tasksTable = EasyMock.createMockBuilder(SystemSchema.TasksTable.class)
                                                 .withConstructor(client, mapper, responseHandler, authMapper)
                                                 .createMock();
    EasyMock.replay(tasksTable);
    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/indexer/v1/tasks", false)).andReturn(request).anyTimes();
    SettableFuture<InputStream> future = SettableFuture.create();
    EasyMock.expect(client.goAsync(request, responseHandler)).andReturn(future).once();
    final int ok = HttpServletResponse.SC_OK;
    EasyMock.expect(responseHandler.getStatus()).andReturn(ok).anyTimes();
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
        return CalciteTests.SUPER_USER_AUTH_RESULT;
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
