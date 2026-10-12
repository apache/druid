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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.seekablestream.supervisor.TaskReportData;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class KafkaSupervisorReportPayloadTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void serde_roundTripsActiveAndPublishingTasks() throws Exception
  {
    final TaskReportData active = new TaskReportData<>(
        "active-task",
        null,
        null,
        null,
        null,
        TaskReportData.TaskType.ACTIVE,
        null,
        null
    );
    final TaskReportData publishing = new TaskReportData<>(
        "publishing-task",
        null,
        null,
        null,
        null,
        TaskReportData.TaskType.PUBLISHING,
        null,
        null
    );

    final KafkaSupervisorReportPayload payload = new KafkaSupervisorReportPayload(
        "id",
        "dataSource",
        "stream",
        1,
        1,
        1L,
        null,
        null,
        null,
        null,
        null,
        false,
        true,
        SupervisorStateManager.BasicState.RUNNING,
        SupervisorStateManager.BasicState.RUNNING,
        List.of(),
        List.of(active),
        List.of(publishing)
    );

    final String json = mapper.writeValueAsString(payload);
    final KafkaSupervisorReportPayload deserialized = mapper.readValue(json, KafkaSupervisorReportPayload.class);

    Assertions.assertEquals(List.of(active), deserialized.getActiveTasks());
    Assertions.assertEquals(List.of(publishing), deserialized.getPublishingTasks());
  }
}
