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

package org.apache.druid.msq.sql.resources;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestFileUtils;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.storage.NilStorageConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlMSQStatementResourcePostTest extends MSQTestBase
{
  private SqlStatementResource resource;


  @Before
  public void init()
  {
    resource = new SqlStatementResource(
        sqlStatementFactory,
        objectMapper,
        indexingServiceClient,
        localFileStorageConnector,
        authorizerMapper
    );
  }

  @Test
  public void testMSQSelectQueryTest() throws IOException
  {
    List<Object[]> results = ImmutableList.of(
        new Object[]{1L, ""},
        new Object[]{
            1L,
            "10.1"
        },
        new Object[]{1L, "2"},
        new Object[]{1L, "1"},
        new Object[]{1L, "def"},
        new Object[]{1L, "abc"}
    );

    Response response = resource.doPost(new SqlQuery(
        "select cnt,dim1 from foo",
        null,
        false,
        false,
        false,
        defaultAsyncContext(),
        null
    ), SqlStatementResourceTest.makeOkRequest());


    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    String taskId = ((SqlStatementResult) response.getEntity()).getQueryId();

    SqlStatementResult expected =
        new SqlStatementResult(taskId, SqlStatementState.SUCCESS,
                               MSQTestOverlordServiceClient.CREATED_TIME,
                               ImmutableList.of(
                                   new ColumnNameAndTypes(
                                       "cnt",
                                       SqlTypeName.BIGINT.getName(),
                                       ValueType.LONG.name()
                                   ),
                                   new ColumnNameAndTypes(
                                       "dim1",
                                       SqlTypeName.VARCHAR.getName(),
                                       ValueType.STRING.name()
                                   )
                               ),
                               MSQTestOverlordServiceClient.DURATION,
                               new ResultSetInformation(
                                   6L,
                                   316L,
                                   null,
                                   MSQControllerTask.DUMMY_DATASOURCE_FOR_SELECT,
                                   results,
                                   ImmutableList.of(new PageInformation(0, 6L, 316L))
                               ),
                               null
        );

    Assert.assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(response.getEntity())
    );
  }


  @Test
  public void nonSupportedModes()
  {
    SqlStatementResourceTest.assertExceptionMessage(
        resource.doPost(new SqlQuery(
            "select * from foo",
            null,
            false,
            false,
            false,
            ImmutableMap.of(),
            null
        ), SqlStatementResourceTest.makeOkRequest()),
        "Execution mode is not provided to the sql statement api. "
        + "Please set [executionMode] to [ASYNC] in the query context",
        Response.Status.BAD_REQUEST
    );

    SqlStatementResourceTest.assertExceptionMessage(
        resource.doPost(new SqlQuery(
            "select * from foo",
            null,
            false,
            false,
            false,
            ImmutableMap.of(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.SYNC.name()),
            null
        ), SqlStatementResourceTest.makeOkRequest()),
        "The sql statement api currently does not support the provided execution mode [SYNC]. "
        + "Please set [executionMode] to [ASYNC] in the query context",
        Response.Status.BAD_REQUEST
    );
  }

  @Test
  public void emptyInsert()
  {
    Response response = resource.doPost(new SqlQuery(
        "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null and __time < TIMESTAMP '1971-01-01 00:00:00' group by 1, 2 PARTITIONED by day clustered by dim1",
        null,
        false,
        false,
        false,
        ImmutableMap.<String, Object>builder()
                    .putAll(defaultAsyncContext())
                    .build(),
        null
    ), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    SqlStatementResult actual = (SqlStatementResult) response.getEntity();

    SqlStatementResult expected = new SqlStatementResult(
        actual.getQueryId(),
        SqlStatementState.SUCCESS,
        MSQTestOverlordServiceClient.CREATED_TIME,
        null,
        MSQTestOverlordServiceClient.DURATION,
        new ResultSetInformation(0L, 0L, null, "foo1", null, null),
        null
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void emptyReplace()
  {
    Response response = resource.doPost(new SqlQuery(
        "replace into foo1 overwrite all select  __time, dim1 , count(*) as cnt from foo where dim1 is not null and __time < TIMESTAMP '1971-01-01 00:00:00' group by 1, 2 PARTITIONED by day clustered by dim1",
        null,
        false,
        false,
        false,
        ImmutableMap.<String, Object>builder()
                    .putAll(defaultAsyncContext())
                    .build(),
        null
    ), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    SqlStatementResult actual = (SqlStatementResult) response.getEntity();

    SqlStatementResult expected = new SqlStatementResult(
        actual.getQueryId(),
        SqlStatementState.SUCCESS,
        MSQTestOverlordServiceClient.CREATED_TIME,
        null,
        MSQTestOverlordServiceClient.DURATION,
        new ResultSetInformation(0L, 0L, null, "foo1", null, null),
        null
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void insertCannotBeEmptyFaultTest()
  {
    Response response = resource.doPost(new SqlQuery(
        "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null and __time < TIMESTAMP '1971-01-01 00:00:00' group by 1, 2 PARTITIONED by day clustered by dim1",
        null,
        false,
        false,
        false,
        ImmutableMap.<String, Object>builder()
                    .putAll(defaultAsyncContext())
                    .put(MultiStageQueryContext.CTX_FAIL_ON_EMPTY_INSERT, true)
                    .build(),
        null
    ), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    SqlStatementResult actual = (SqlStatementResult) response.getEntity();

    InsertCannotBeEmptyFault insertCannotBeEmptyFault = new InsertCannotBeEmptyFault("foo1");

    MSQException insertCannotBeEmpty = new MSQException(insertCannotBeEmptyFault);

    SqlStatementResult expected = new SqlStatementResult(
        actual.getQueryId(),
        SqlStatementState.FAILED,
        MSQTestOverlordServiceClient.CREATED_TIME,
        null,
        MSQTestOverlordServiceClient.DURATION,
        null,
        DruidException.fromFailure(new DruidException.Failure(InsertCannotBeEmptyFault.CODE)
        {
          @Override
          protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
          {
            DruidException e = bob.forPersona(DruidException.Persona.USER)
                                  .ofCategory(DruidException.Category.UNCATEGORIZED)
                                  .build(insertCannotBeEmpty.getFault().getErrorMessage());
            e.withContext("dataSource", insertCannotBeEmptyFault.getDataSource());
            return e;
          }
        }).toErrorResponse()
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testExplain() throws IOException
  {
    Map<String, Object> context = defaultAsyncContext();
    context.put("sqlQueryId", "queryId");
    Response response = resource.doPost(new SqlQuery(
        "explain plan for select * from foo",
        null,
        false,
        false,
        false,
        context,
        null
    ), SqlStatementResourceTest.makeOkRequest());

    Assert.assertEquals(
        "{PLAN=[{\"query\":"
        + "{\"queryType\":\"scan\","
        + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
        + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
        + "\"resultFormat\":\"compactedList\","
        + "\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],"
        + "\"legacy\":false,"
        + "\"context\":{\"__resultFormat\":\"object\",\"executionMode\":\"ASYNC\",\"scanSignature\":\"[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"cnt\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"dim1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"dim2\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"dim3\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"m1\\\",\\\"type\\\":\\\"FLOAT\\\"},{\\\"name\\\":\\\"m2\\\",\\\"type\\\":\\\"DOUBLE\\\"},{\\\"name\\\":\\\"unique_dim1\\\",\\\"type\\\":\\\"COMPLEX<hyperUnique>\\\"}]\",\"sqlQueryId\":\"queryId\"},\"columnTypes\":[\"LONG\",\"LONG\",\"STRING\",\"STRING\",\"STRING\",\"FLOAT\",\"DOUBLE\",\"COMPLEX<hyperUnique>\"],\"granularity\":{\"type\":\"all\"}},\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"dim1\",\"type\":\"STRING\"},{\"name\":\"dim2\",\"type\":\"STRING\"},{\"name\":\"dim3\",\"type\":\"STRING\"},{\"name\":\"cnt\",\"type\":\"LONG\"},{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"m2\",\"type\":\"DOUBLE\"},{\"name\":\"unique_dim1\",\"type\":\"COMPLEX<hyperUnique>\"}],\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"},{\"queryColumn\":\"dim2\",\"outputColumn\":\"dim2\"},{\"queryColumn\":\"dim3\",\"outputColumn\":\"dim3\"},{\"queryColumn\":\"cnt\",\"outputColumn\":\"cnt\"},{\"queryColumn\":\"m1\",\"outputColumn\":\"m1\"},{\"queryColumn\":\"m2\",\"outputColumn\":\"m2\"},{\"queryColumn\":\"unique_dim1\",\"outputColumn\":\"unique_dim1\"}]}],"
        + " RESOURCES=[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}],"
        + " ATTRIBUTES={\"statementType\":\"SELECT\"}}",
        String.valueOf(SqlStatementResourceTest.getResultRowsFromResponse(response).get(0))
    );
  }

  @Test
  public void forbiddenTest()
  {
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resource.doPost(
        new SqlQuery(
            StringUtils.format("select * from %s", CalciteTests.FORBIDDEN_DATASOURCE),
            null,
            false,
            false,
            false,
            defaultAsyncContext(),
            null
        ),
        SqlStatementResourceTest.makeOkRequest()
    ).getStatus());
  }

  @Test
  public void durableStorageDisabledTest()
  {
    SqlStatementResource resourceWithDurableStorage = new SqlStatementResource(
        sqlStatementFactory,
        objectMapper,
        indexingServiceClient,
        NilStorageConnector.getInstance(),
        authorizerMapper
    );

    String errorMessage = "The sql statement api cannot read from the select destination [durableStorage] provided in "
                          + "the query context [selectDestination] since it is not configured on the broker. It is recommended to "
                          + "configure durable storage as it allows the user to fetch large result sets. "
                          + "Please contact your cluster admin to configure durable storage.";
    Map<String, Object> context = defaultAsyncContext();
    context.put(MultiStageQueryContext.CTX_SELECT_DESTINATION, MSQSelectDestination.DURABLESTORAGE.getName());

    SqlStatementResourceTest.assertExceptionMessage(resourceWithDurableStorage.doPost(
                                                        new SqlQuery(
                                                            "select * from foo",
                                                            null,
                                                            false,
                                                            false,
                                                            false,
                                                            context,
                                                            null
                                                        ),
                                                        SqlStatementResourceTest.makeOkRequest()
                                                    ), errorMessage,
                                                    Response.Status.BAD_REQUEST
    );
  }

  @Test
  public void testWithDurableStorage() throws IOException
  {
    Map<String, Object> context = defaultAsyncContext();
    context.put(MultiStageQueryContext.CTX_SELECT_DESTINATION, MSQSelectDestination.DURABLESTORAGE.getName());
    context.put(MultiStageQueryContext.CTX_ROWS_PER_PAGE, 2);

    SqlStatementResult sqlStatementResult = (SqlStatementResult) resource.doPost(
        new SqlQuery(
            "select cnt,dim1 from foo",
            null,
            false,
            false,
            false,
            context,
            null
        ),
        SqlStatementResourceTest.makeOkRequest()
    ).getEntity();

    Assert.assertEquals(ImmutableList.of(
        new PageInformation(0, 2L, 120L, 0, 0),
        new PageInformation(1, 2L, 118L, 0, 1),
        new PageInformation(2, 2L, 122L, 0, 2)
    ), sqlStatementResult.getResultSetInformation().getPages());

    assertExpectedResults(
        "{\"cnt\":1,\"dim1\":\"\"}\n"
        + "{\"cnt\":1,\"dim1\":\"10.1\"}\n"
        + "{\"cnt\":1,\"dim1\":\"2\"}\n"
        + "{\"cnt\":1,\"dim1\":\"1\"}\n"
        + "{\"cnt\":1,\"dim1\":\"def\"}\n"
        + "{\"cnt\":1,\"dim1\":\"abc\"}\n"
        + "\n",
        resource.doGetResults(
            sqlStatementResult.getQueryId(),
            null,
            ResultFormat.OBJECTLINES.name(),
            SqlStatementResourceTest.makeOkRequest()
        ),
        objectMapper
    );

    assertExpectedResults(
        "{\"cnt\":1,\"dim1\":\"\"}\n"
        + "{\"cnt\":1,\"dim1\":\"10.1\"}\n"
        + "\n",
        resource.doGetResults(
            sqlStatementResult.getQueryId(),
            0L,
            ResultFormat.OBJECTLINES.name(),
            SqlStatementResourceTest.makeOkRequest()
        ),
        objectMapper
    );

    assertExpectedResults(
        "{\"cnt\":1,\"dim1\":\"def\"}\n"
        + "{\"cnt\":1,\"dim1\":\"abc\"}\n"
        + "\n",
        resource.doGetResults(
            sqlStatementResult.getQueryId(),
            2L,
            ResultFormat.OBJECTLINES.name(),
            SqlStatementResourceTest.makeOkRequest()
        ),
        objectMapper
    );
  }


  @Test
  public void testMultipleWorkersWithPageSizeLimiting() throws IOException
  {
    Map<String, Object> context = defaultAsyncContext();
    context.put(MultiStageQueryContext.CTX_SELECT_DESTINATION, MSQSelectDestination.DURABLESTORAGE.getName());
    context.put(MultiStageQueryContext.CTX_ROWS_PER_PAGE, 2);
    context.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 3);

    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());


    SqlStatementResult sqlStatementResult = (SqlStatementResult) resource.doPost(
        new SqlQuery(
            "SELECT\n"
            + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
            + "  user\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{ \"files\": [" + toReadAsJson + "," + toReadAsJson + "],\"type\":\"local\"}',\n"
            + "    '{\"type\": \"json\"}',\n"
            + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
            + "  )\n"
            + ") where user like '%ot%'",
            null,
            false,
            false,
            false,
            context,
            null
        ),
        SqlStatementResourceTest.makeOkRequest()
    ).getEntity();

    Assert.assertEquals(ImmutableList.of(
        new PageInformation(0, 2L, 128L, 0, 0),
        new PageInformation(1, 2L, 132L, 1, 1),
        new PageInformation(2, 2L, 128L, 0, 2),
        new PageInformation(3, 2L, 132L, 1, 3),
        new PageInformation(4, 2L, 130L, 0, 4)
    ), sqlStatementResult.getResultSetInformation().getPages());


    List<List<Object>> rows = new ArrayList<>();
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "Beau.bot"));
    rows.add(ImmutableList.of(1466985600000L, "Beau.bot"));
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "Lsjbot"));
    rows.add(ImmutableList.of(1466985600000L, "TaxonBot"));
    rows.add(ImmutableList.of(1466985600000L, "TaxonBot"));
    rows.add(ImmutableList.of(1466985600000L, "GiftBot"));
    rows.add(ImmutableList.of(1466985600000L, "GiftBot"));

    Assert.assertEquals(rows, SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        null,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(0, 2), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        0L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(2, 4), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        1L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(4, 6), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        2L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(6, 8), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        3L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows.subList(8, 10), SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        4L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));
  }

  @Test
  public void testResultFormat() throws Exception
  {
    Map<String, Object> context = defaultAsyncContext();
    context.put(MultiStageQueryContext.CTX_SELECT_DESTINATION, MSQSelectDestination.DURABLESTORAGE.name());

    SqlStatementResult sqlStatementResult = (SqlStatementResult) resource.doPost(
        new SqlQuery(
            "select cnt,dim1 from foo",
            null,
            false,
            false,
            false,
            context,
            null
        ),
        SqlStatementResourceTest.makeOkRequest()
    ).getEntity();

    final List<ColumnNameAndTypes> columnNameAndTypes = ImmutableList.of(
        new ColumnNameAndTypes("cnt", "cnt", "cnt"),
        new ColumnNameAndTypes("dim1", "dim1", "dim1")
    );

    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{1, ""},
        new Object[]{1, "10.1"},
        new Object[]{1, "2"},
        new Object[]{1, "1"},
        new Object[]{1, "def"},
        new Object[]{1, "abc"}
    );

    for (ResultFormat resultFormat : ResultFormat.values()) {
      Assert.assertArrayEquals(
          createExpectedResultsInFormat(resultFormat, expectedRows, columnNameAndTypes, objectMapper),
          responseToByteArray(
              resource.doGetResults(
                  sqlStatementResult.getQueryId(),
                  null,
                  resultFormat.name(),
                  SqlStatementResourceTest.makeOkRequest()
              ), objectMapper
          )
      );

      Assert.assertArrayEquals(
          createExpectedResultsInFormat(resultFormat, expectedRows, columnNameAndTypes, objectMapper),
          responseToByteArray(
              resource.doGetResults(
                  sqlStatementResult.getQueryId(),
                  0L,
                  resultFormat.name(),
                  SqlStatementResourceTest.makeOkRequest()
              ), objectMapper
          )
      );
    }
  }

  @Test
  public void testResultFormatWithParamInSelect() throws IOException
  {
    Map<String, Object> context = defaultAsyncContext();
    context.put(MultiStageQueryContext.CTX_SELECT_DESTINATION, MSQSelectDestination.DURABLESTORAGE.name());

    SqlStatementResult sqlStatementResult = (SqlStatementResult) resource.doPost(
        new SqlQuery(
            "select cnt,dim1 from foo",
            ResultFormat.OBJECTLINES,
            false,
            false,
            false,
            context,
            null
        ),
        SqlStatementResourceTest.makeOkRequest()
    ).getEntity();


    List<List<Object>> rows = new ArrayList<>();
    rows.add(ImmutableList.of(1, ""));
    rows.add(ImmutableList.of(1, "10.1"));
    rows.add(ImmutableList.of(1, "2"));
    rows.add(ImmutableList.of(1, "1"));
    rows.add(ImmutableList.of(1, "def"));
    rows.add(ImmutableList.of(1, "abc"));

    Assert.assertEquals(rows, SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        null,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));

    Assert.assertEquals(rows, SqlStatementResourceTest.getResultRowsFromResponse(resource.doGetResults(
        sqlStatementResult.getQueryId(),
        0L,
        ResultFormat.ARRAY.name(),
        SqlStatementResourceTest.makeOkRequest()
    )));
  }

  private byte[] createExpectedResultsInFormat(
      ResultFormat resultFormat,
      List<Object[]> resultsList,
      List<ColumnNameAndTypes> rowSignature,
      ObjectMapper jsonMapper
  ) throws Exception
  {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (final ResultFormat.Writer writer = resultFormat.createFormatter(os, jsonMapper)) {
      SqlStatementResource.resultPusherInternal(writer, Yielders.each(Sequences.simple(resultsList)), rowSignature);
    }
    return os.toByteArray();
  }

  private void assertExpectedResults(String expectedResult, Response resultsResponse, ObjectMapper objectMapper)
      throws IOException
  {
    byte[] bytes = responseToByteArray(resultsResponse, objectMapper);
    Assert.assertEquals(expectedResult, new String(bytes, StandardCharsets.UTF_8));
  }

  public static byte[] responseToByteArray(Response resp, ObjectMapper objectMapper) throws IOException
  {
    if (resp.getEntity() instanceof StreamingOutput) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ((StreamingOutput) resp.getEntity()).write(baos);
      return baos.toByteArray();
    } else {
      return objectMapper.writeValueAsBytes(resp.getEntity());
    }
  }

  @Test
  public void testInsert()
  {
    Response response = resource.doPost(new SqlQuery(
        "insert into foo1 select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1",
        null,
        false,
        false,
        false,
        defaultAsyncContext(),
        null
    ), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    SqlStatementResult actual = (SqlStatementResult) response.getEntity();


    SqlStatementResult expected = new SqlStatementResult(
        actual.getQueryId(),
        SqlStatementState.SUCCESS,
        MSQTestOverlordServiceClient.CREATED_TIME,
        null,
        MSQTestOverlordServiceClient.DURATION,
        new ResultSetInformation(NullHandling.sqlCompatible() ? 6L : 5L, 0L, null, "foo1", null, null),
        null
    );
    Assert.assertEquals(expected, actual);

    Response getResponse = resource.doGetStatus(actual.getQueryId(), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), getResponse.getStatus());
    Assert.assertEquals(expected, getResponse.getEntity());

    Response resultsResponse = resource.doGetResults(
        actual.getQueryId(),
        0L,
        null,
        SqlStatementResourceTest.makeOkRequest()
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), resultsResponse.getStatus());
    Assert.assertNull(resultsResponse.getEntity());
  }


  @Test
  public void testReplaceAll()
  {
    Response response = resource.doPost(new SqlQuery(
        "replace into foo1 overwrite all select  __time, dim1 , count(*) as cnt from foo where dim1 is not null group by 1, 2 PARTITIONED by day clustered by dim1",
        null,
        false,
        false,
        false,
        defaultAsyncContext(),
        null
    ), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    SqlStatementResult actual = (SqlStatementResult) response.getEntity();


    SqlStatementResult expected = new SqlStatementResult(
        actual.getQueryId(),
        SqlStatementState.SUCCESS,
        MSQTestOverlordServiceClient.CREATED_TIME,
        null,
        MSQTestOverlordServiceClient.DURATION,
        new ResultSetInformation(NullHandling.sqlCompatible() ? 6L : 5L, 0L, null, "foo1", null, null),
        null
    );
    Assert.assertEquals(expected, actual);

    Response getResponse = resource.doGetStatus(actual.getQueryId(), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), getResponse.getStatus());
    Assert.assertEquals(expected, getResponse.getEntity());

    Response resultsResponse = resource.doGetResults(
        actual.getQueryId(),
        0L,
        null,
        SqlStatementResourceTest.makeOkRequest()
    );
    Assert.assertEquals(Response.Status.OK.getStatusCode(), resultsResponse.getStatus());
    Assert.assertNull(resultsResponse.getEntity());
  }


  private static Map<String, Object> defaultAsyncContext()
  {
    Map<String, Object> context = new HashMap<>();
    context.put(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.ASYNC.name());
    return context;
  }

}
