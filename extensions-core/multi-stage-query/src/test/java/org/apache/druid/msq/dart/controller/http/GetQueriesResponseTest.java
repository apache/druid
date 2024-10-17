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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class GetQueriesResponseTest
{
  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;
    final GetQueriesResponse response = new GetQueriesResponse(
        Collections.singletonList(
            new DartQueryInfo(
                "xyz",
                "abc",
                "SELECT 1",
                "localhost:1001",
                "auth",
                "anon",
                DateTimes.of("2000"),
                ControllerHolder.State.RUNNING.toString()
            )
        )
    );
    final GetQueriesResponse response2 =
        jsonMapper.readValue(jsonMapper.writeValueAsBytes(response), GetQueriesResponse.class);
    Assertions.assertEquals(response, response2);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(GetQueriesResponse.class).usingGetClass().verify();
  }
}
