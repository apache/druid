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

package org.apache.druid.indexing.seekablestream.supervisor;

import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SeekableStreamSupervisorReportPayloadTest
{
  private static SeekableStreamSupervisorReportPayload<String, String> createPayload(
      List<TaskReportData> activeTasks,
      List<TaskReportData> publishingTasks
  )
  {
    return new SeekableStreamSupervisorReportPayload<String, String>(
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
        null,
        false,
        true,
        SupervisorStateManager.BasicState.RUNNING,
        SupervisorStateManager.BasicState.RUNNING,
        List.of(),
        activeTasks,
        publishingTasks
    )
    {
    };
  }

  @Test
  public void testConstructorPopulatesActiveAndPublishingTasks()
  {
    final TaskReportData<String, String> active = new TaskReportData<>(
        "active-task",
        null,
        null,
        DateTimes.nowUtc(),
        30L,
        TaskReportData.TaskType.ACTIVE,
        null,
        null
    );
    final TaskReportData<String, String> publishing = new TaskReportData<>(
        "publishing-task",
        null,
        null,
        DateTimes.nowUtc(),
        60L,
        TaskReportData.TaskType.PUBLISHING,
        null,
        null
    );

    final SeekableStreamSupervisorReportPayload<String, String> payload =
        createPayload(List.of(active), List.of(publishing));

    Assertions.assertEquals(List.of(active), payload.getActiveTasks());
    Assertions.assertEquals(List.of(publishing), payload.getPublishingTasks());
  }

  @Test
  public void testConstructorDefaultsNullTaskListsToEmpty()
  {
    final SeekableStreamSupervisorReportPayload<String, String> payload = createPayload(null, null);

    Assertions.assertTrue(payload.getActiveTasks().isEmpty());
    Assertions.assertTrue(payload.getPublishingTasks().isEmpty());
  }
}
