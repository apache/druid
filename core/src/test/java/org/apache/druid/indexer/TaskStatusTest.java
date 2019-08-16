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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TaskStatusTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();

    final TaskStatus status = new TaskStatus(
        "testId",
        TaskState.RUNNING,
        1000L,
        "an error message",
        TaskLocation.create("testHost", 1010, -1)
    );

    final String json = mapper.writeValueAsString(status);
    Assert.assertEquals(status, mapper.readValue(json, TaskStatus.class));

    final String jsonNoLocation = "{\n"
                                  + "\"id\": \"testId\",\n"
                                  + "\"status\": \"SUCCESS\",\n"
                                  + "\"duration\": 3000,\n"
                                  + "\"errorMsg\": \"hello\"\n"
                                  + "}";

    final TaskStatus statusNoLocation = new TaskStatus(
        "testId",
        TaskState.SUCCESS,
        3000L,
        "hello",
        null
    );
    Assert.assertEquals(statusNoLocation, mapper.readValue(jsonNoLocation, TaskStatus.class));
  }
}
