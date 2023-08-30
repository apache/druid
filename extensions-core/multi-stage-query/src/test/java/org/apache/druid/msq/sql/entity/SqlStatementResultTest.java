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


package org.apache.druid.msq.sql.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.resources.SqlStatementResourceTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlStatementResultTest
{
  public static final MSQException MSQ_EXCEPTION = new MSQException(
      QueryNotSupportedFault.instance());

  public static final ObjectMapper MAPPER = DefaultObjectMapper.INSTANCE;

  public static final String JSON_STRING = "{\"queryId\":\"q1\","
                                           + "\"state\":\"RUNNING\","
                                           + "\"createdAt\":\"2023-05-31T12:00:00.000Z\","
                                           + "\"schema\":[{\"name\":\"_time\",\"type\":\"TIMESTAMP\",\"nativeType\":\"LONG\"},{\"name\":\"alias\",\"type\":\"VARCHAR\",\"nativeType\":\"STRING\"},{\"name\":\"market\",\"type\":\"VARCHAR\",\"nativeType\":\"STRING\"}],"
                                           + "\"durationMs\":100,"
                                           + "\"result\":{\"numTotalRows\":1,\"totalSizeInBytes\":1,\"resultFormat\":\"object\",\"dataSource\":\"ds\",\"pages\":[{\"id\":0,\"sizeInBytes\":1}]},"
                                           + "\"errorDetails\":{\"error\":\"druidException\",\"errorCode\":\"QueryNotSupported\",\"persona\":\"USER\",\"category\":\"UNCATEGORIZED\",\"errorMessage\":\"QueryNotSupported\",\"context\":{}}}";

  public static final SqlStatementResult SQL_STATEMENT_RESULT = new SqlStatementResult(
      "q1",
      SqlStatementState.RUNNING,
      SqlStatementResourceTest.CREATED_TIME,
      SqlStatementResourceTest.COL_NAME_AND_TYPES,
      100L,
      ResultSetInformationTest.RESULTS,
      DruidException.fromFailure(new DruidException.Failure(MSQ_EXCEPTION.getFault().getErrorCode())
      {
        @Override
        protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
        {
          return bob.forPersona(DruidException.Persona.USER)
                    .ofCategory(DruidException.Category.UNCATEGORIZED)
                    .build(MSQ_EXCEPTION.getMessage());
        }
      }).toErrorResponse()
  );


  @Test
  public void sanityTest() throws JsonProcessingException
  {

    Assert.assertEquals(JSON_STRING, MAPPER.writeValueAsString(SQL_STATEMENT_RESULT));
    Assert.assertEquals(
        SQL_STATEMENT_RESULT,
        MAPPER.readValue(MAPPER.writeValueAsString(SQL_STATEMENT_RESULT), SqlStatementResult.class)
    );
    Assert.assertEquals(
        SQL_STATEMENT_RESULT.hashCode(),
        MAPPER.readValue(MAPPER.writeValueAsString(SQL_STATEMENT_RESULT), SqlStatementResult.class).hashCode()
    );
    Assert.assertEquals(
        "SqlStatementResult{"
        + "queryId='q1',"
        + " state=RUNNING,"
        + " createdAt=2023-05-31T12:00:00.000Z,"
        + " sqlRowSignature=[ColumnNameAndTypes{colName='_time', sqlTypeName='TIMESTAMP', nativeTypeName='LONG'}, ColumnNameAndTypes{colName='alias', sqlTypeName='VARCHAR', nativeTypeName='STRING'}, ColumnNameAndTypes{colName='market', sqlTypeName='VARCHAR', nativeTypeName='STRING'}],"
        + " durationInMs=100,"
        + " resultSetInformation=ResultSetInformation{numTotalRows=1, totalSizeInBytes=1, resultFormat=object, records=null, dataSource='ds', pages=[PageInformation{id=0, numRows=null, sizeInBytes=1}]},"
        + " errorResponse={error=druidException, errorCode=QueryNotSupported, persona=USER, category=UNCATEGORIZED, errorMessage=QueryNotSupported, context={}}}",
        SQL_STATEMENT_RESULT.toString()
    );
  }
}
