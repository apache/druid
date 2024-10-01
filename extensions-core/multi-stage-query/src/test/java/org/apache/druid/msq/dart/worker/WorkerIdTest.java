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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class WorkerIdTest
{
  @Test
  public void test_fromString()
  {
    Assertions.assertEquals(
        new WorkerId("https", "local-host:8100", "xyz"),
        WorkerId.fromString("https:local-host:8100:xyz")
    );
  }

  @Test
  public void test_fromDruidNode()
  {
    Assertions.assertEquals(
        new WorkerId("https", "local-host:8100", "xyz"),
        WorkerId.fromDruidNode(new DruidNode("none", "local-host", false, 8200, 8100, true, true), "xyz")
    );
  }

  @Test
  public void test_fromDruidServerMetadata()
  {
    Assertions.assertEquals(
        new WorkerId("https", "local-host:8100", "xyz"),
        WorkerId.fromDruidServerMetadata(
            new DruidServerMetadata("none", "local-host:8200", "local-host:8100", 1, ServerType.HISTORICAL, "none", 0),
            "xyz"
        )
    );
  }

  @Test
  public void test_toString()
  {
    Assertions.assertEquals(
        "https:local-host:8100:xyz",
        new WorkerId("https", "local-host:8100", "xyz").toString()
    );
  }

  @Test
  public void test_getters()
  {
    final WorkerId workerId = new WorkerId("https", "local-host:8100", "xyz");
    Assertions.assertEquals("https", workerId.getScheme());
    Assertions.assertEquals("local-host:8100", workerId.getHostAndPort());
    Assertions.assertEquals("xyz", workerId.getQueryId());
    Assertions.assertEquals("https://local-host:8100/druid/dart-worker/workers/xyz", workerId.toUri().toString());
  }

  @Test
  public void test_serde() throws IOException
  {
    final ObjectMapper objectMapper = TestHelper.JSON_MAPPER;
    final WorkerId workerId = new WorkerId("https", "localhost:8100", "xyz");
    final WorkerId workerId2 = objectMapper.readValue(objectMapper.writeValueAsBytes(workerId), WorkerId.class);
    Assertions.assertEquals(workerId, workerId2);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(WorkerId.class)
                  .usingGetClass()
                  .withNonnullFields("fullString")
                  .withIgnoredFields("scheme", "hostAndPort", "queryId")
                  .verify();
  }
}
