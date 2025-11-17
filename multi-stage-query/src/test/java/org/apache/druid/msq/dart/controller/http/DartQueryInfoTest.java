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

package org.apache.druid.msq.dart.controller.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DartQueryInfoTest
{
  @Test
  void test_serde() throws Exception
  {
    DartQueryInfo dartQueryInfo = new DartQueryInfo(
        "sid",
        "did",
        "SELECT 1",
        "localhost:1001",
        "",
        "",
        DateTimes.of("2000"),
        ControllerHolder.State.RUNNING.toString()
    );
    ObjectMapper jsonMapper = new DefaultObjectMapper().registerModules(new DartWorkerModule().getJacksonModules());
    byte[] bytes = jsonMapper.writeValueAsBytes(dartQueryInfo);
    DartQueryInfo deserialized = jsonMapper.readValue(bytes, DartQueryInfo.class);
    Assertions.assertEquals(dartQueryInfo, deserialized);

    // Assert that the engine is present.
    Map<String, Object> map = jsonMapper.readValue(bytes, Map.class);
    Assertions.assertEquals(DartSqlEngine.NAME, map.get("engine"));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(DartQueryInfo.class).usingGetClass().verify();
  }
}
