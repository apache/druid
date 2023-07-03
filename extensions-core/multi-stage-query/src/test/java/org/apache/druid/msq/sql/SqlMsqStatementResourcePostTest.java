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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.sql.resources.SqlStatementResource;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlMsqStatementResourcePostTest extends MSQTestBase
{
  private SqlStatementResource resource;

  @Before
  public void init()
  {
    resource = new SqlStatementResource(
        sqlStatementFactory,
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        objectMapper,
        indexingServiceClient
    );
  }

  @Test
  public void testMSQSelectQueryTest() throws IOException
  {
    List<Object> results = ImmutableList.of(
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
                                   objectMapper.readValue(
                                       objectMapper.writeValueAsString(
                                           results),
                                       new TypeReference<List<Object>>()
                                       {
                                       }
                                   ),
                                   ImmutableList.of(new PageInformation(6L, 316L, 0))
                               ),
                               null
        );

    Assert.assertEquals(expected, response.getEntity());
  }


  @Test
  public void nonSupportedModes()
  {
    for (ImmutableMap<?, ?> context : ImmutableList.of(ImmutableMap.of(
        QueryContexts.CTX_EXECUTION_MODE,
        ExecutionMode.SYNC.name()
    ), ImmutableMap.of())) {
      SqlStatementResourceTest.assertExceptionMessage(
          resource.doPost(new SqlQuery(
              "select * from foo",
              null,
              false,
              false,
              false,
              (Map<String, Object>) context,
              null
          ), SqlStatementResourceTest.makeOkRequest()),
          "The statement sql api only supports sync mode[ASYNC]. Please set context parameter [executionMode=ASYNC] in the context payload",
          Response.Status.BAD_REQUEST
      );
    }
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
        defaultAsyncContext(),
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
        + "\"context\":{\"executionMode\":\"ASYNC\",\"scanSignature\":\"[{\\\"name\\\":\\\"__time\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"cnt\\\",\\\"type\\\":\\\"LONG\\\"},{\\\"name\\\":\\\"dim1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"dim2\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"dim3\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"m1\\\",\\\"type\\\":\\\"FLOAT\\\"},{\\\"name\\\":\\\"m2\\\",\\\"type\\\":\\\"DOUBLE\\\"},{\\\"name\\\":\\\"unique_dim1\\\",\\\"type\\\":\\\"COMPLEX<hyperUnique>\\\"}]\",\"sqlQueryId\":\"queryId\"},\"granularity\":{\"type\":\"all\"}},\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"dim1\",\"type\":\"STRING\"},{\"name\":\"dim2\",\"type\":\"STRING\"},{\"name\":\"dim3\",\"type\":\"STRING\"},{\"name\":\"cnt\",\"type\":\"LONG\"},{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"m2\",\"type\":\"DOUBLE\"},{\"name\":\"unique_dim1\",\"type\":\"COMPLEX<hyperUnique>\"}],\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"},{\"queryColumn\":\"dim2\",\"outputColumn\":\"dim2\"},{\"queryColumn\":\"dim3\",\"outputColumn\":\"dim3\"},{\"queryColumn\":\"cnt\",\"outputColumn\":\"cnt\"},{\"queryColumn\":\"m1\",\"outputColumn\":\"m1\"},{\"queryColumn\":\"m2\",\"outputColumn\":\"m2\"},{\"queryColumn\":\"unique_dim1\",\"outputColumn\":\"unique_dim1\"}]}],"
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


  private static Map<String, Object> defaultAsyncContext()
  {
    Map<String, Object> context = new HashMap<String, Object>();
    context.put(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.ASYNC.name());
    return context;
  }

}
