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

package org.apache.druid.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SqlRequestTest
{
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testDefaults()
  {
    final String query = "SELECT COUNT(*) FROM datasource";
    final SqlRequest request = SqlRequest.of(query, null, null);

    Assertions.assertEquals(query, request.query());
    Assertions.assertEquals("array", request.resultFormat());
    Assertions.assertTrue(request.header());
    Assertions.assertTrue(request.typesHeader());
    Assertions.assertTrue(request.sqlTypesHeader());
    Assertions.assertEquals(Map.of("sqlStringifyArrays", false), request.context());
    Assertions.assertEquals(List.of(), request.parameters());
  }

  @Test
  public void testContext()
  {
    final Map<String, Object> context = new HashMap<>();
    context.put("timeout", 30000);
    context.put("useApproximateCountDistinct", false);

    final SqlRequest request = SqlRequest.of("SELECT * FROM datasource", context, null);

    Assertions.assertEquals(30000, request.context().get("timeout"));
    Assertions.assertEquals(false, request.context().get("useApproximateCountDistinct"));
    Assertions.assertEquals(false, request.context().get("sqlStringifyArrays"));
  }

  @Test
  public void testSerde() throws IOException
  {
    final SqlRequest original = SqlRequest.of(
        "SELECT * FROM table WHERE col1 = ? AND col2 = ?",
        Map.of("timeout", 30000),
        List.of(new SqlParameter("VARCHAR", "test_value"), new SqlParameter("INTEGER", 42))
    );

    final String json = objectMapper.writeValueAsString(original);
    Assertions.assertTrue(json.contains("\"resultFormat\":\"array\""), json);
    Assertions.assertTrue(json.contains("\"header\":true"), json);
    Assertions.assertTrue(json.contains("\"typesHeader\":true"), json);
    Assertions.assertTrue(json.contains("\"sqlTypesHeader\":true"), json);

    Assertions.assertEquals(original, objectMapper.readValue(json, SqlRequest.class));
  }
}
