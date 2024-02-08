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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TaskStatusPlusTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    final TaskStatusPlus status = new TaskStatusPlus(
        "testId",
        "testGroupId",
        "testType",
        DateTimes.nowUtc(),
        DateTimes.nowUtc(),
        TaskState.RUNNING,
        RunnerTaskState.RUNNING,
        1000L,
        TaskLocation.create("testHost", 1010, -1),
        "ds_test",
        null
    );
    final String json = jsonMapper.writeValueAsString(status);
    Assert.assertEquals(status, jsonMapper.readValue(json, TaskStatusPlus.class));
  }

  @Test
  public void testJsonAttributes() throws IOException
  {
    final String json = "{\n"
                        + "\"id\": \"testId\",\n"
                        + "\"groupId\": \"testGroupId\",\n"
                        + "\"type\": \"testType\",\n"
                        + "\"createdTime\": \"2018-09-17T06:35:17.392Z\",\n"
                        + "\"queueInsertionTime\": \"2018-09-17T06:35:17.392Z\",\n"
                        + "\"statusCode\": \"RUNNING\",\n"
                        + "\"status\": \"RUNNING\",\n"
                        + "\"runnerStatusCode\": \"RUNNING\",\n"
                        + "\"duration\": 1000,\n"
                        + "\"location\": {\n"
                        + "\"host\": \"testHost\",\n"
                        + "\"port\": 1010,\n"
                        + "\"tlsPort\": -1\n"
                        + "},\n"
                        + "\"dataSource\": \"ds_test\",\n"
                        + "\"errorMsg\": null\n"
                        + "}";
    TaskStatusPlus taskStatusPlus = jsonMapper.readValue(json, TaskStatusPlus.class);
    Assert.assertNotNull(taskStatusPlus);
    Assert.assertNotNull(taskStatusPlus.getStatusCode());
    Assert.assertTrue(taskStatusPlus.getStatusCode().isRunnable());
    Assert.assertNotNull(taskStatusPlus.getRunnerStatusCode());

    String serialized = jsonMapper.writeValueAsString(taskStatusPlus);

    Assert.assertTrue(serialized.contains("\"status\":"));
    Assert.assertTrue(serialized.contains("\"statusCode\":"));
    Assert.assertTrue(serialized.contains("\"runnerStatusCode\":"));
  }
}
