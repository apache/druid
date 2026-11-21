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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SupervisorManagerSubmitHandoffNoticesTest
{
  @Test
  public void testGroupsBySupervisorIdNotDataSource()
  {
    final Task primary = streamTask("index_kafka", "KubeState", "KubeState", 0);
    final Task haCopy = streamTask("index_kafka", "KubeState_gcp_moncevents", "KubeState", 1);
    final SupervisorManager manager = mockManager();
    EasyMock.expect(manager.handoffTaskGroupsEarly("KubeState", ImmutableList.of(0))).andReturn(true);
    EasyMock.expect(manager.handoffTaskGroupsEarly("KubeState_gcp_moncevents", ImmutableList.of(1))).andReturn(true);
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("primary", "ha"),
        lookup("primary", primary, "ha", haCopy)
    );

    Assertions.assertEquals(ImmutableList.of(0), result.taskGroups().get("KubeState"));
    Assertions.assertEquals(ImmutableList.of(1), result.taskGroups().get("KubeState_gcp_moncevents"));
    Assertions.assertEquals(2, result.taskGroups().size());
    Assertions.assertEquals(Collections.emptyList(), result.skipped());
    EasyMock.verify(manager);
  }

  @Test
  public void testIncludesRabbit()
  {
    final Task rabbit = streamTask("index_rabbit", "orders", "orders", 2);
    final SupervisorManager manager = mockManager();
    EasyMock.expect(manager.handoffTaskGroupsEarly("orders", ImmutableList.of(2))).andReturn(true);
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("r1"),
        id -> Optional.of(rabbit)
    );
    Assertions.assertEquals(ImmutableList.of(2), result.taskGroups().get("orders"));
    Assertions.assertEquals(Collections.emptyList(), result.skipped());
    EasyMock.verify(manager);
  }

  @Test
  public void testSkipsEmptySupervisorId()
  {
    final Task task = streamTask("index_kafka", "", "cAdvisor", 3);
    final SupervisorManager manager = mockManager();
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("t1"),
        id -> Optional.of(task)
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(
        ImmutableList.of(new WorkerHandoffResult.SkippedTask("t1", WorkerHandoffResult.REASON_MISSING_SUPERVISOR_ID)),
        result.skipped()
    );
    EasyMock.verify(manager);
  }

  @Test
  public void testSkipsNullTaskGroupId()
  {
    final Task task = streamTask("index_kafka", "wikipedia", "wikipedia", null);
    final SupervisorManager manager = mockManager();
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("index_kafka_legacy_abc"),
        id -> Optional.of(task)
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(
        ImmutableList.of(
            new WorkerHandoffResult.SkippedTask(
                "index_kafka_legacy_abc",
                WorkerHandoffResult.REASON_MISSING_TASK_GROUP_ID
            )
        ),
        result.skipped()
    );
    EasyMock.verify(manager);
  }

  @Test
  public void testOmitsUnknownTask()
  {
    final SupervisorManager manager = mockManager();
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("missing-task"),
        id -> Optional.absent()
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(Collections.emptyList(), result.skipped());
    EasyMock.verify(manager);
  }

  @Test
  public void testOmitsBlankTaskId()
  {
    final SupervisorManager manager = mockManager();
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        Collections.singletonList(""),
        id -> Optional.absent()
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(Collections.emptyList(), result.skipped());
    EasyMock.verify(manager);
  }

  @Test
  public void testOmitsNonStreamTask()
  {
    final Task compact = EasyMock.mock(Task.class);
    EasyMock.replay(compact);
    final SupervisorManager manager = mockManager();
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("c1"),
        id -> Optional.of(compact)
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(Collections.emptyList(), result.skipped());
    EasyMock.verify(compact, manager);
  }

  @Test
  public void testOmitsNoop()
  {
    final SupervisorManager manager = mockManager();
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("noop"),
        id -> Optional.of(NoopTask.create())
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(Collections.emptyList(), result.skipped());
    EasyMock.verify(manager);
  }

  @Test
  public void testSkipsMissingSupervisor()
  {
    final Task task = streamTask("index_kafka", "gone", "gone", 0);
    final SupervisorManager manager = mockManager();
    EasyMock.expect(manager.handoffTaskGroupsEarly("gone", ImmutableList.of(0))).andReturn(false);
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("t1"),
        id -> Optional.of(task)
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(
        ImmutableList.of(new WorkerHandoffResult.SkippedTask("t1", WorkerHandoffResult.REASON_SUPERVISOR_NOT_FOUND)),
        result.skipped()
    );
    EasyMock.verify(manager);
  }

  @Test
  public void testSkipsUnsupportedSupervisor()
  {
    final Task task = streamTask("index_kafka", "batch", "batch", 0);
    final SupervisorManager manager = mockManager();
    EasyMock.expect(manager.handoffTaskGroupsEarly("batch", ImmutableList.of(0)))
            .andThrow(new UnsupportedOperationException("no early handoff"));
    EasyMock.replay(manager);

    final WorkerHandoffResult result = manager.submitHandoffNotices(
        ImmutableList.of("t1"),
        id -> Optional.of(task)
    );
    Assertions.assertTrue(result.taskGroups().isEmpty());
    Assertions.assertEquals(
        ImmutableList.of(new WorkerHandoffResult.SkippedTask("t1", WorkerHandoffResult.REASON_HANDOFF_UNSUPPORTED)),
        result.skipped()
    );
    EasyMock.verify(manager);
  }

  private static SupervisorManager mockManager()
  {
    return EasyMock.partialMockBuilder(SupervisorManager.class)
                   .withConstructor(ObjectMapper.class, MetadataSupervisorManager.class)
                   .withArgs(new ObjectMapper(), EasyMock.createNiceMock(MetadataSupervisorManager.class))
                   .addMockedMethod("handoffTaskGroupsEarly", String.class, List.class)
                   .createMock();
  }

  private static Function<String, Optional<Task>> lookup(String id1, Task task1, String id2, Task task2)
  {
    return id -> {
      if (id1.equals(id)) {
        return Optional.of(task1);
      }
      if (id2.equals(id)) {
        return Optional.of(task2);
      }
      return Optional.absent();
    };
  }

  private static SeekableStreamIndexTask<?, ?, ?> streamTask(
      String type,
      String supervisorId,
      String dataSource,
      Integer taskGroupId
  )
  {
    @SuppressWarnings({"rawtypes", "unchecked"})
    final SeekableStreamIndexTaskIOConfig ioConfig = EasyMock.mock(SeekableStreamIndexTaskIOConfig.class);
    EasyMock.expect(ioConfig.getTaskGroupId()).andReturn(taskGroupId).anyTimes();
    @SuppressWarnings({"rawtypes", "unchecked"})
    final SeekableStreamIndexTask task = EasyMock.mock(SeekableStreamIndexTask.class);
    EasyMock.expect(task.getType()).andReturn(type).anyTimes();
    EasyMock.expect(task.getSupervisorId()).andReturn(supervisorId).anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn(dataSource).anyTimes();
    EasyMock.expect(task.getIOConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.replay(ioConfig, task);
    return task;
  }
}
