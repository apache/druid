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

package org.apache.druid.msq.sql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.indexing.report.MSQTaskReportTest;
import org.apache.druid.msq.sql.entity.ColNameAndType;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.sql.resources.SqlStatementResource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlResourceTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlStatementResourceTest extends CalciteTestBase
{

  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static SqlStatementResource resource;

  private static final String ACCEPTED_SELECT_MSQ_QUERY = "QUERY_ID_1";
  private static final String RUNNING_SELECT_MSQ_QUERY = "QUERY_ID_2";
  private static final String FINISHED_SELECT_MSQ_QUERY = "QUERY_ID_3";

  private static final String ERRORED_SELECT_MSQ_QUERY = "QUERY_ID_4";


  private static final String RUNNING_NON_MSQ_TASK = "QUERY_ID_5";

  private static final String FAILED_NON_MSQ_TASK = "QUERY_ID_6";

  private static final String FINISHED_NON_MSQ_TASK = "QUERY_ID_7";


  private static final String ACCEPTED_INSERT_MSQ_TASK = "QUERY_ID_8";

  private static final String RUNNING_INSERT_MSQ_QUERY = "QUERY_ID_9";
  private static final String FINISHED_INSERT_MSQ_QUERY = "QUERY_ID_10";
  private static final String ERRORED_INSERT_MSQ_QUERY = "QUERY_ID_11";


  private static final Query<?> QUERY =
      new Druids.ScanQueryBuilder()
          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
          .legacy(false)
          .intervals(new MultipleIntervalSegmentSpec(
              Collections.singletonList(Intervals.of(
                  "2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))))
          .dataSource("target")
          .context(ImmutableMap.of(MSQTaskQueryMaker.USER_KEY, AuthConfig.ALLOW_ALL_NAME))
          .build();


  private static final MSQControllerTask MSQ_CONTROLLER_SELECT_PAYLOAD = new MSQControllerTask(
      ACCEPTED_SELECT_MSQ_QUERY,
      MSQSpec.builder()
             .query(QUERY)
             .columnMappings(
                 ColumnMappings.identity(
                     RowSignature.builder()
                                 .add(
                                     "_time",
                                     ColumnType.LONG
                                 )
                                 .add(
                                     "alias",
                                     ColumnType.STRING
                                 )
                                 .add(
                                     "market",
                                     ColumnType.STRING
                                 )
                                 .build()))
             .destination(
                 TaskReportMSQDestination.INSTANCE)
             .tuningConfig(
                 MSQTuningConfig.defaultConfig())
             .build(),
      "select _time,alias,market from test",
      Maps.newHashMap(),
      null,
      ImmutableList.of(SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR),
      null
  );

  private static final MSQControllerTask MSQ_CONTROLLER_INSERT_PAYLOAD = new MSQControllerTask(
      ACCEPTED_SELECT_MSQ_QUERY,
      MSQSpec.builder()
             .query(QUERY)
             .columnMappings(
                 ColumnMappings.identity(
                     RowSignature.builder()
                                 .add(
                                     "_time",
                                     ColumnType.LONG
                                 )
                                 .add(
                                     "alias",
                                     ColumnType.STRING
                                 )
                                 .add(
                                     "market",
                                     ColumnType.STRING
                                 )
                                 .build()))
             .destination(
                 new DataSourceMSQDestination("test", Granularities.DAY, null, null))
             .tuningConfig(
                 MSQTuningConfig.defaultConfig())
             .build(),
      "insert into test select _time,alias,market from test",
      Maps.newHashMap(),
      null,
      ImmutableList.of(SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR),
      null
  );

  @Mock
  private static IndexingServiceClient indexingServiceClient;

  private static final DateTime CREATED_TIME = DateTimes.of("2023-05-31T12:00Z");
  private static final DateTime QUEUE_INSERTION_TIME = DateTimes.of("2023-05-31T12:01Z");

  private static final List<Object[]> RESULT_ROWS = ImmutableList.of(
      new Object[]{123, "foo", "bar"},
      new Object[]{234, "foo1", "bar1"}
  );

  private static final Map<String, Object> ROW1 = ImmutableMap.of("_time", 123, "alias", "foo", "market", "bar");
  private static final Map<String, Object> ROW2 = ImmutableMap.of("_time", 234, "alias", "foo1", "market", "bar1");

  private static final String FAILURE_MSG = "failure msg";

  @BeforeClass
  public static void init() throws Exception
  {
    indexingServiceClient = Mockito.mock(IndexingServiceClient.class);
    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(ACCEPTED_SELECT_MSQ_QUERY)))
           .thenReturn(new TaskStatusResponse(
               ACCEPTED_SELECT_MSQ_QUERY,
               new TaskStatusPlus(
                   ACCEPTED_SELECT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   null,
                   null,
                   null,
                   TaskLocation.unknown(),
                   null,
                   null
               )
           ));

    Mockito.when(indexingServiceClient.getTaskPayload(ArgumentMatchers.eq(ACCEPTED_SELECT_MSQ_QUERY)))
           .thenReturn(new TaskPayloadResponse(ACCEPTED_SELECT_MSQ_QUERY, MSQ_CONTROLLER_SELECT_PAYLOAD));

    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(RUNNING_SELECT_MSQ_QUERY)))
           .thenReturn(new TaskStatusResponse(
               RUNNING_SELECT_MSQ_QUERY,
               new TaskStatusPlus(
                   RUNNING_SELECT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.RUNNING,
                   null,
                   null,
                   new TaskLocation("test", 0, 0),
                   null,
                   null
               )
           ));

    Mockito.when(indexingServiceClient.getTaskPayload(RUNNING_SELECT_MSQ_QUERY))
           .thenReturn(new TaskPayloadResponse(RUNNING_SELECT_MSQ_QUERY, MSQ_CONTROLLER_SELECT_PAYLOAD));


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(FINISHED_SELECT_MSQ_QUERY)))
           .thenReturn(new TaskStatusResponse(
               FINISHED_SELECT_MSQ_QUERY,
               new TaskStatusPlus(
                   FINISHED_SELECT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.SUCCESS,
                   null,
                   100L,
                   new TaskLocation("test", 0, 0),
                   null,
                   null
               )
           ));

    Mockito.when(indexingServiceClient.getTaskPayload(FINISHED_SELECT_MSQ_QUERY))
           .thenReturn(new TaskPayloadResponse(FINISHED_SELECT_MSQ_QUERY, MSQ_CONTROLLER_SELECT_PAYLOAD));

    final MSQTaskReport report = new MSQTaskReport(
        FINISHED_SELECT_MSQ_QUERY,
        new MSQTaskReportPayload(
            new MSQStatusReport(TaskState.SUCCESS, null, new ArrayDeque<>(), null, 0, 1, 2),
            MSQStagesReport.create(
                MSQTaskReportTest.QUERY_DEFINITION,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of()
            ),
            new CounterSnapshotsTree(),
            new MSQResultsReport(
                ImmutableList.of(
                    new MSQResultsReport.ColumnAndType("_time", ColumnType.LONG),
                    new MSQResultsReport.ColumnAndType("alias", ColumnType.STRING),
                    new MSQResultsReport.ColumnAndType("market", ColumnType.STRING)
                ),
                ImmutableList.of(SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR, SqlTypeName.VARCHAR),
                Yielders.each(Sequences.simple(RESULT_ROWS))
            )
        )
    );

    final ObjectMapper mapper = TestHelper.makeJsonMapper()
                                          .registerModules(new MSQIndexingModule().getJacksonModules());

    Map<String, Object> reportPayload = mapper.readValue(
        mapper.writeValueAsString(TaskReport.buildTaskReports(report)),
        new TypeReference<Map<String, Object>>()
        {
        }
    );

    Mockito.when(indexingServiceClient.getTaskReport(FINISHED_SELECT_MSQ_QUERY))
           .thenReturn(reportPayload);


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(ERRORED_SELECT_MSQ_QUERY)))
           .thenReturn(new TaskStatusResponse(
               ERRORED_SELECT_MSQ_QUERY,
               new TaskStatusPlus(
                   ERRORED_SELECT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.FAILED,
                   null,
                   -1L,
                   TaskLocation.unknown(),
                   null,
                   FAILURE_MSG
               )
           ));


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(RUNNING_NON_MSQ_TASK)))
           .thenReturn(new TaskStatusResponse(
               RUNNING_NON_MSQ_TASK,
               new TaskStatusPlus(
                   RUNNING_NON_MSQ_TASK,
                   null,
                   null,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.RUNNING,
                   null,
                   -1L,
                   TaskLocation.unknown(),
                   null,
                   null
               )
           ));


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(FAILED_NON_MSQ_TASK)))
           .thenReturn(new TaskStatusResponse(
               FAILED_NON_MSQ_TASK,
               new TaskStatusPlus(
                   FAILED_NON_MSQ_TASK,
                   null,
                   null,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.FAILED,
                   null,
                   -1L,
                   TaskLocation.unknown(),
                   null,
                   FAILURE_MSG
               )
           ));


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(FINISHED_NON_MSQ_TASK)))
           .thenReturn(new TaskStatusResponse(
               FINISHED_NON_MSQ_TASK,
               new TaskStatusPlus(
                   FINISHED_NON_MSQ_TASK,
                   null,
                   IndexTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.SUCCESS,
                   null,
                   -1L,
                   TaskLocation.unknown(),
                   null,
                   null
               )
           ));


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(ACCEPTED_INSERT_MSQ_TASK)))
           .thenReturn(new TaskStatusResponse(
               ACCEPTED_SELECT_MSQ_QUERY,
               new TaskStatusPlus(
                   ACCEPTED_SELECT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   null,
                   null,
                   null,
                   TaskLocation.unknown(),
                   null,
                   null
               )
           ));

    Mockito.when(indexingServiceClient.getTaskPayload(ArgumentMatchers.eq(ACCEPTED_INSERT_MSQ_TASK)))
           .thenReturn(new TaskPayloadResponse(ACCEPTED_INSERT_MSQ_TASK, MSQ_CONTROLLER_INSERT_PAYLOAD));

    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(RUNNING_INSERT_MSQ_QUERY)))
           .thenReturn(new TaskStatusResponse(
               RUNNING_INSERT_MSQ_QUERY,
               new TaskStatusPlus(
                   RUNNING_INSERT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.RUNNING,
                   null,
                   null,
                   new TaskLocation("test", 0, 0),
                   null,
                   null
               )
           ));

    Mockito.when(indexingServiceClient.getTaskPayload(RUNNING_INSERT_MSQ_QUERY))
           .thenReturn(new TaskPayloadResponse(RUNNING_INSERT_MSQ_QUERY, MSQ_CONTROLLER_INSERT_PAYLOAD));


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(FINISHED_INSERT_MSQ_QUERY)))
           .thenReturn(new TaskStatusResponse(
               FINISHED_INSERT_MSQ_QUERY,
               new TaskStatusPlus(
                   FINISHED_INSERT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.SUCCESS,
                   null,
                   100L,
                   new TaskLocation("test", 0, 0),
                   null,
                   null
               )
           ));

    Mockito.when(indexingServiceClient.getTaskPayload(FINISHED_INSERT_MSQ_QUERY))
           .thenReturn(new TaskPayloadResponse(FINISHED_INSERT_MSQ_QUERY, MSQ_CONTROLLER_INSERT_PAYLOAD));


    Mockito.when(indexingServiceClient.getTaskStatus(ArgumentMatchers.eq(ERRORED_INSERT_MSQ_QUERY)))
           .thenReturn(new TaskStatusResponse(
               ERRORED_INSERT_MSQ_QUERY,
               new TaskStatusPlus(
                   ERRORED_INSERT_MSQ_QUERY,
                   null,
                   MSQControllerTask.TYPE,
                   CREATED_TIME,
                   QUEUE_INSERTION_TIME,
                   TaskState.FAILED,
                   null,
                   -1L,
                   TaskLocation.unknown(),
                   null,
                   FAILURE_MSG
               )
           ));

    resource = new SqlStatementResource(
        new SqlStatementFactory(null),
        new ServerConfig(),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        JSON_MAPPER,
        indexingServiceClient
    );
  }


  @Test
  public void testMSQSelectAcceptedQuery()
  {
    Response response = resource.doGetStatus(ACCEPTED_SELECT_MSQ_QUERY, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(new SqlStatementResult(
        ACCEPTED_SELECT_MSQ_QUERY,
        SqlStatementState.ACCEPTED,
        CREATED_TIME,
        ImmutableList.of(
            new ColNameAndType("_time", SqlTypeName.TIMESTAMP.getName()),
            new ColNameAndType("alias", SqlTypeName.VARCHAR.getName()),
            new ColNameAndType("market", SqlTypeName.VARCHAR.getName())
        ),
        null,
        null,
        null
    ), response.getEntity());

    assertExceptionMessage(
        resource.doGetResults(ACCEPTED_SELECT_MSQ_QUERY, null, null, null, null, makeOkRequest()),
        StringUtils.format("Query is [%s]. Please wait for it to complete.", SqlStatementState.ACCEPTED),
        Response.Status.NOT_FOUND
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(ACCEPTED_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }

  @Test

  public void testMSQSelectRunningQuery()
  {

    Response response = resource.doGetStatus(RUNNING_SELECT_MSQ_QUERY, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(new SqlStatementResult(
        RUNNING_SELECT_MSQ_QUERY,
        SqlStatementState.RUNNING,
        CREATED_TIME,
        ImmutableList.of(
            new ColNameAndType("_time", SqlTypeName.TIMESTAMP.getName()),
            new ColNameAndType("alias", SqlTypeName.VARCHAR.getName()),
            new ColNameAndType("market", SqlTypeName.VARCHAR.getName())
        ),
        null,
        null,
        null
    ), response.getEntity());

    assertExceptionMessage(
        resource.doGetResults(RUNNING_SELECT_MSQ_QUERY, null, null, null, null, makeOkRequest()),
        StringUtils.format("Query is [%s]. Please wait for it to complete.", SqlStatementState.RUNNING),
        Response.Status.NOT_FOUND
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(RUNNING_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }


  @Test
  public void testFinishedSelectMSQQuery() throws Exception
  {
    Response response = resource.doGetStatus(FINISHED_SELECT_MSQ_QUERY, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(new SqlStatementResult(
        FINISHED_SELECT_MSQ_QUERY,
        SqlStatementState.SUCCESS,
        CREATED_TIME,
        ImmutableList.of(
            new ColNameAndType("_time", SqlTypeName.TIMESTAMP.getName()),
            new ColNameAndType("alias", SqlTypeName.VARCHAR.getName()),
            new ColNameAndType("market", SqlTypeName.VARCHAR.getName())
        ),
        100L,
        new ResultSetInformation(
            null,
            false,
            2L,
            null,
            RESULT_ROWS.stream().map(Arrays::asList).collect(Collectors.toList())
        ),
        null
    ), response.getEntity());

    Response resultsResponse = resource.doGetResults(
        FINISHED_SELECT_MSQ_QUERY,
        null,
        null,
        null,
        null,
        makeOkRequest()
    );
    Assert.assertEquals(
        Response.Status.OK.getStatusCode(), resultsResponse.getStatus());

    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(ROW1);
    rows.add(ROW2);

    Assert.assertEquals(rows, getResultRowsFromResponse(resultsResponse));

    Assert.assertEquals(
        Response.Status.OK.getStatusCode(),
        resource.deleteQuery(FINISHED_SELECT_MSQ_QUERY, makeOkRequest()).getStatus()
    );

    Assert.assertEquals(
        rows.subList(1, 2),
        getResultRowsFromResponse(resource.doGetResults(
            FINISHED_SELECT_MSQ_QUERY,
            1L,
            null,
            null,
            null,
            makeOkRequest()
        ))
    );
    Assert.assertEquals(
        rows.subList(0, 1),
        getResultRowsFromResponse(resource.doGetResults(
            FINISHED_SELECT_MSQ_QUERY,
            0L,
            1L,
            null,
            null,
            makeOkRequest()
        ))
    );
    Assert.assertEquals(
        rows,
        getResultRowsFromResponse(resource.doGetResults(
            FINISHED_SELECT_MSQ_QUERY,
            0L,
            3L,
            null,
            null,
            makeOkRequest()
        ))
    );

    Assert.assertEquals(
        Response.Status.PRECONDITION_FAILED.getStatusCode(),
        resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, -1L, 3L, null, null, makeOkRequest())
                .getStatus()
    );
    Assert.assertEquals(
        Response.Status.PRECONDITION_FAILED.getStatusCode(),
        resource.doGetResults(FINISHED_SELECT_MSQ_QUERY, null, -1L, null, null, makeOkRequest())
                .getStatus()
    );

  }


  @Test
  public void testFailedMSQQuery()
  {
    for (String queryID : ImmutableList.of(ERRORED_SELECT_MSQ_QUERY, ERRORED_INSERT_MSQ_QUERY)) {
      assertExceptionMessage(
          resource.doGetStatus(queryID, makeOkRequest()),
          FAILURE_MSG,
          Response.Status.OK
      );
      assertExceptionMessage(
          resource.doGetResults(queryID, null, null, null, null, makeOkRequest()),
          FAILURE_MSG,
          Response.Status.NOT_FOUND
      );

      Assert.assertEquals(
          Response.Status.OK.getStatusCode(),
          resource.deleteQuery(queryID, makeOkRequest()).getStatus()
      );
    }
  }


  @Test
  public void testNonMSQTasks()
  {
    for (String queryID : ImmutableList.of(RUNNING_NON_MSQ_TASK, FAILED_NON_MSQ_TASK, FINISHED_NON_MSQ_TASK)) {
      assertNullResponse(
          resource.doGetStatus(queryID, makeOkRequest()),
          Response.Status.NOT_FOUND
      );
      assertNullResponse(
          resource.doGetResults(queryID, null, null, null, null, makeOkRequest()),
          Response.Status.NOT_FOUND
      );
      assertNullResponse(
          resource.deleteQuery(queryID, makeOkRequest()),
          Response.Status.NOT_FOUND
      );
    }
  }

  @Test
  public void testMSQInsertAcceptedQuery()
  {
    Response response = resource.doGetStatus(ACCEPTED_INSERT_MSQ_TASK, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(new SqlStatementResult(
        ACCEPTED_INSERT_MSQ_TASK,
        SqlStatementState.ACCEPTED,
        CREATED_TIME,
        null,
        null,
        null,
        null
    ), response.getEntity());

    assertExceptionMessage(
        resource.doGetResults(ACCEPTED_INSERT_MSQ_TASK, null, null, null, null, makeOkRequest()),
        StringUtils.format("Query is [%s]. Please wait for it to complete.", SqlStatementState.ACCEPTED),
        Response.Status.NOT_FOUND
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(ACCEPTED_INSERT_MSQ_TASK, makeOkRequest()).getStatus()
    );
  }

  @Test
  public void testMSQInsertRunningQuery()
  {
    Response response = resource.doGetStatus(RUNNING_INSERT_MSQ_QUERY, makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(new SqlStatementResult(
        RUNNING_INSERT_MSQ_QUERY,
        SqlStatementState.RUNNING,
        CREATED_TIME,
        null,
        null,
        null,
        null
    ), response.getEntity());

    assertExceptionMessage(
        resource.doGetResults(RUNNING_INSERT_MSQ_QUERY, null, null, null, null, makeOkRequest()),
        StringUtils.format("Query is [%s]. Please wait for it to complete.", SqlStatementState.RUNNING),
        Response.Status.NOT_FOUND
    );
    Assert.assertEquals(
        Response.Status.ACCEPTED.getStatusCode(),
        resource.deleteQuery(RUNNING_INSERT_MSQ_QUERY, makeOkRequest()).getStatus()
    );
  }


  private static void assertNullResponse(Response response, Response.Status expectectedStatus)
  {
    Assert.assertEquals(expectectedStatus.getStatusCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
  }

  private static void assertExceptionMessage(
      Response response,
      String exceptionMessage,
      Response.Status expectectedStatus
  )
  {
    Assert.assertEquals(expectectedStatus.getStatusCode(), response.getStatus());
    Assert.assertEquals(exceptionMessage, getQueryExceptionFromResponse(response).getMessage());
  }

  private static List getResultRowsFromResponse(Response resultsResponse) throws IOException
  {
    return JSON_MAPPER.readValue(SqlResourceTest.responseToByteArray(resultsResponse), List.class);
  }

  private static QueryException getQueryExceptionFromResponse(Response response)
  {
    if (response.getEntity() instanceof SqlStatementResult) {
      return ((SqlStatementResult) response.getEntity()).getQueryException();
    } else {
      return (QueryException) response.getEntity();
    }
  }

  private MockHttpServletRequest makeOkRequest()
  {
    return makeExpectedReq(CalciteTests.REGULAR_USER_AUTH_RESULT);
  }

  private MockHttpServletRequest makeOtherUserRequest()
  {
    return makeExpectedReq(CalciteTests.SUPER_USER_AUTH_RESULT);
  }

  private MockHttpServletRequest makeExpectedReq(AuthenticationResult authenticationResult)
  {
    MockHttpServletRequest req = new MockHttpServletRequest();
    req.attributes.put(AuthConfig.DRUID_AUTHENTICATION_RESULT, authenticationResult);
    req.remoteAddr = "1.2.3.4";
    return req;
  }
}
