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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.client.indexing.ClientKillUnusedSegmentsTaskQuery;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ClientKillUnusedSegmentsTaskQuerySerdeTest
{
  private ObjectMapper objectMapper;

  @Before
  public void setup()
  {
    objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(
        new NamedType(ClientKillUnusedSegmentsTaskQuery.class, ClientKillUnusedSegmentsTaskQuery.TYPE)
    );
  }

  @Test
  public void testClientKillUnusedSegmentsTaskQueryToKillUnusedSegmentsTask() throws IOException
  {
    final ClientKillUnusedSegmentsTaskQuery taskQuery = new ClientKillUnusedSegmentsTaskQuery(
        "killTaskId",
        "datasource",
        Intervals.of("2020-01-01/P1D")
    );
    final byte[] json = objectMapper.writeValueAsBytes(taskQuery);
    final KillUnusedSegmentsTask fromJson = (KillUnusedSegmentsTask) objectMapper.readValue(json, Task.class);
    Assert.assertEquals(taskQuery.getId(), fromJson.getId());
    Assert.assertEquals(taskQuery.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(taskQuery.getInterval(), fromJson.getInterval());
  }

  @Test
  public void testKillUnusedSegmentsTaskToClientKillUnusedSegmentsTaskQuery() throws IOException
  {
    final KillUnusedSegmentsTask task = new KillUnusedSegmentsTask(
        null,
        "datasource",
        Intervals.of("2020-01-01/P1D"),
        null
    );
    final byte[] json = objectMapper.writeValueAsBytes(task);
    final ClientKillUnusedSegmentsTaskQuery taskQuery = (ClientKillUnusedSegmentsTaskQuery) objectMapper.readValue(
        json,
        ClientTaskQuery.class
    );
    Assert.assertEquals(task.getId(), taskQuery.getId());
    Assert.assertEquals(task.getDataSource(), taskQuery.getDataSource());
    Assert.assertEquals(task.getInterval(), taskQuery.getInterval());
  }
}
