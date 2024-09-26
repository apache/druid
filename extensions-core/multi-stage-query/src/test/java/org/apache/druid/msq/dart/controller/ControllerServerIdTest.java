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

package org.apache.druid.msq.dart.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ControllerServerIdTest
{
  @Test
  public void test_fromString()
  {
    Assertions.assertEquals(
        new ControllerServerId("local-host:8100", 123),
        ControllerServerId.fromString("local-host:8100:123")
    );
  }

  @Test
  public void test_toString()
  {
    Assertions.assertEquals(
        "local-host:8100:123",
        new ControllerServerId("local-host:8100", 123).toString()
    );
  }

  @Test
  public void test_getters()
  {
    final ControllerServerId id = new ControllerServerId("local-host:8100", 123);
    Assertions.assertEquals("local-host:8100", id.getHost());
    Assertions.assertEquals(123, id.getEpoch());
  }

  @Test
  public void test_serde() throws IOException
  {
    final ObjectMapper objectMapper = TestHelper.JSON_MAPPER;
    final ControllerServerId id = new ControllerServerId("localhost:8100", 123);
    final ControllerServerId id2 = objectMapper.readValue(objectMapper.writeValueAsBytes(id), ControllerServerId.class);
    Assertions.assertEquals(id, id2);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ControllerServerId.class)
                  .usingGetClass()
                  .withNonnullFields("fullString")
                  .withIgnoredFields("host", "epoch")
                  .verify();
  }
}
