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
package io.druid.sql.calcite.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
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
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.ReflectionQueryToolChestWarehouse;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;

public class SystemSchemaTest extends CalciteTestBase
{

  private SystemSchema schema;
  private SpecificSegmentsQuerySegmentWalker walker;
  private DruidLeaderClient client;
  private TimelineServerView serverView;
  private ObjectMapper mapper;
  private FullResponseHolder responseHolder;
  private Request request;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp()
  {
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    client = EasyMock.createMock(DruidLeaderClient.class);
    mapper = TestHelper.makeJsonMapper();
    responseHolder = EasyMock.createMock(FullResponseHolder.class);
    request = EasyMock.createMock(Request.class);
    schema = new SystemSchema(
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
  }

  @Test
  public void testSegmentsTable() throws Exception
  {
    // total segments = 5
    // segments 1,2,3 are published
    // segments 1,2,4,5 are served
    // segment 3 is published but not served
    // segment 2 is served by 2 servers, so num_replicas=2

    final SystemSchema.SegmentsTable segmentsTable = (SystemSchema.SegmentsTable) schema.getTableMap().get("segments");
    final RelDataType rowType = segmentsTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();

    Assert.assertEquals(12, fields.size());

    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/coordinator/v1/datasources?full"))
            .andReturn(request)
            .once();
    EasyMock.expect(client.go(request)).andReturn(responseHolder).once();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).once();
    String jsonValue = mapper.writeValueAsString(ImmutableList.of(dataSource1, dataSource2, dataSource3));
    EasyMock.expect(responseHolder.getContent()).andReturn(jsonValue).once();

    EasyMock.expect(serverView.getClients())
            .andReturn(serverViewClients)
            .once();
    EasyMock.replay(client, request, responseHolder, serverView);
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
    Assert.assertEquals(6, fields.size());

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
    Assert.assertEquals("realtime", row1[2].toString());
    Object[] row2 = rows.last();
    Assert.assertEquals("server2", row2[0]);
    Assert.assertEquals("historical", row2[2].toString());
  }

  @Test
  public void testTasksTable() throws Exception
  {
    final SystemSchema.TasksTable tasksTable = (SystemSchema.TasksTable) schema.getTableMap().get("tasks");
    final RelDataType rowType = tasksTable.getRowType(new JavaTypeFactoryImpl());
    final List<RelDataTypeField> fields = rowType.getFieldList();
    Assert.assertEquals(10, fields.size());

    Assert.assertEquals("task_id", fields.get(0).getName());
    Assert.assertEquals(SqlTypeName.VARCHAR, fields.get(0).getType().getSqlTypeName());

    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/indexer/v1/tasks")).andReturn(request).once();
    EasyMock.expect(client.go(request)).andReturn(responseHolder).once();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).once();
    String jsonValue = mapper.writeValueAsString(ImmutableList.of(task1, task2, task3));
    EasyMock.expect(responseHolder.getContent()).andReturn(jsonValue).once();
    EasyMock.replay(client, request, responseHolder);
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
    Assert.assertEquals(3, rows.count());
    //task1 is running
    //task2 and task3 are completed
    for (Object[] row : rows) {
      String id = (String) row[0];
      if ("task1".equals(id)) {
        Assert.assertEquals("RUNNING", row[5].toString());
        Assert.assertEquals("RUNNING", row[6].toString());
        Assert.assertEquals(-1L, row[7]);
        Assert.assertEquals("testHost:1010", row[8]);
      } else {
        Assert.assertEquals("NONE", row[6].toString());
        Assert.assertEquals(1000L, row[7]);
        Assert.assertEquals("null:-1", row[8]);
      }
    }
  }

}
