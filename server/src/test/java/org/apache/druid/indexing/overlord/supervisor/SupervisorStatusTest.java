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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SupervisorStatusTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final SupervisorStatus.Builder builder = new SupervisorStatus.Builder();
    final SupervisorStatus supervisorStatus = builder.withId("wikipedia")
                                                     .withState("RUNNING")
                                                     .withDetailedState("RUNNING")
                                                     .withHealthy(true)
                                                     .withType("kafka")
                                                     .withSource("wikipedia")
                                                     .withSuspended(false)
                                                     .build();
    final String serialized = mapper.writeValueAsString(supervisorStatus);
    final SupervisorStatus deserialized = mapper.readValue(serialized, SupervisorStatus.class);
    Assert.assertEquals(supervisorStatus, deserialized);
  }

  @Test
  public void testJsonAttr() throws IOException
  {
    String json = "{"
                  + "\"id\":\"wikipedia\","
                  + "\"state\":\"UNHEALTHY_SUPERVISOR\","
                  + "\"detailedState\":\"UNHEALTHY_SUPERVISOR\","
                  + "\"healthy\":false,"
                  + "\"type\":\"kafka\","
                  + "\"source\":\"wikipedia\","
                  + "\"suspended\":false"
                  + "}";
    final ObjectMapper mapper = new ObjectMapper();
    final SupervisorStatus deserialized = mapper.readValue(json, SupervisorStatus.class);
    Assert.assertNotNull(deserialized);
    Assert.assertEquals("wikipedia", deserialized.getId());
    final String serialized = mapper.writeValueAsString(deserialized);
    Assert.assertTrue(serialized.contains("\"source\""));
    Assert.assertEquals(json, serialized);
  }
}
