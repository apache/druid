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

package org.apache.druid.sql.calcite.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.query.http.ClientSqlParameter;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class SqlQueryTest extends CalciteTestBase
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    final SqlQuery query = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );
    Assert.assertEquals(query, JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(query), SqlQuery.class));
  }

  @Test
  public void testClientSqlQueryToSqlQueryConversion() throws JsonProcessingException
  {
    final ClientSqlQuery givenClientSqlQuery = new ClientSqlQuery(
        "SELECT ?",
        "array",
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        null
    );

    final SqlQuery expectedSqlQuery = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        null
    );

    final SqlQuery observedSqlQuery =
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(givenClientSqlQuery), SqlQuery.class);
    Assert.assertEquals(expectedSqlQuery, observedSqlQuery);
  }

  @Test
  public void testClientSqlQueryToSqlQueryConversion2() throws JsonProcessingException
  {
    final ClientSqlQuery givenClientSqlQuery = new ClientSqlQuery(
        "SELECT ?",
        "arrayLines",
        false,
        false,
        false,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new ClientSqlParameter("INTEGER", 1), new ClientSqlParameter("VARCHAR", "foo"))
    );

    final SqlQuery expectedSqlQuery = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAYLINES,
        false,
        false,
        false,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1), new SqlParameter(SqlType.VARCHAR, "foo"))
    );

    final SqlQuery observedSqlQuery =
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(givenClientSqlQuery), SqlQuery.class);
    Assert.assertEquals(expectedSqlQuery, observedSqlQuery);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SqlQuery.class).withNonnullFields("query").usingGetClass().verify();
    EqualsVerifier.forClass(SqlParameter.class).withNonnullFields("type").usingGetClass().verify();
  }
}
