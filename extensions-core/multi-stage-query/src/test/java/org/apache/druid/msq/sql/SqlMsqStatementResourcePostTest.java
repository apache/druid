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
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.druid.msq.sql.entity.ColNameAndType;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.sql.resources.SqlStatementResource;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

public class SqlMsqStatementResourcePostTest extends MSQTestBase
{

  private SqlStatementResource resource;

  @Before
  public void init()
  {
    resource = new SqlStatementResource(
        sqlStatementFactory,
        new ServerConfig(),
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
        ImmutableMap.of(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.ASYNC.name()),
        null
    ), SqlStatementResourceTest.makeOkRequest());


    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    String taskId = ((SqlStatementResult) response.getEntity()).getQueryId();

    SqlStatementResult expected =
        new SqlStatementResult(taskId, SqlStatementState.SUCCESS,
                               MSQTestOverlordServiceClient.CREATED_TIME,
                               ImmutableList.of(
                                   new ColNameAndType(
                                       "cnt",
                                       SqlTypeName.BIGINT.getName(),
                                       ValueType.LONG.name()
                                   ),
                                   new ColNameAndType(
                                       "dim1",
                                       SqlTypeName.VARCHAR.getName(),
                                       ValueType.STRING.name()
                                   )
                               ),
                               MSQTestOverlordServiceClient.DURATION,
                               new ResultSetInformation(
                                   null,
                                   6L,
                                   316L,
                                   MSQControllerTask.DUMMY_DATASOURCE_FOR_SELECT,
                                   objectMapper.readValue(
                                       objectMapper.writeValueAsString(
                                           results),
                                       new TypeReference<List<Object>>()
                                       {
                                       }
                                   )
                               ),
                               null
        );

    Assert.assertEquals(expected, response.getEntity());
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
        ImmutableMap.of(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.ASYNC.name()),
        null
    ), SqlStatementResourceTest.makeOkRequest());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    SqlStatementResult actual = (SqlStatementResult) response.getEntity();

    InsertCannotBeEmptyFault insertCannotBeEmptyFault = new InsertCannotBeEmptyFault("foo1");

    SqlStatementResult expected = new SqlStatementResult(
        actual.getQueryId(),
        SqlStatementState.FAILED,
        MSQTestOverlordServiceClient.CREATED_TIME,
        null,
        MSQTestOverlordServiceClient.DURATION,
        null,
        new QueryException(
            null,
            insertCannotBeEmptyFault.getErrorMessage(),
            null,
            "localhost:8080",
            ImmutableMap.of(
                "errorCode",
                InsertCannotBeEmptyFault.CODE,
                "dataSource",
                insertCannotBeEmptyFault.getDataSource()
            )
        )
    );
    Assert.assertEquals(expected, actual);
  }


}
