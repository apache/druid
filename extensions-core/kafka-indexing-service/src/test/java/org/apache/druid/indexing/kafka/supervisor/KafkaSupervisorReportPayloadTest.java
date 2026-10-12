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

import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.seekablestream.supervisor.TaskReportData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class KafkaSupervisorReportPayloadTest
{
  @Test
  public void constructor_withoutTaskLists_startsWithEmptyActiveAndPublishingTasks()
  {
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
        null,
        null
    );

    Assertions.assertEquals(List.of(), payload.getActiveTasks());
    Assertions.assertEquals(List.of(), payload.getPublishingTasks());
  }

  @Test
  public void constructor_withTaskLists_populatesActiveAndPublishingTasks()
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

    Assertions.assertEquals(List.of(active), payload.getActiveTasks());
    Assertions.assertEquals(List.of(publishing), payload.getPublishingTasks());
  }
}
