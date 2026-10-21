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

package org.apache.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class WorkerHandoffHelperTest
{
  private static final String HOST = "mm-0.svc.ns.svc.cluster.local:8091";

  @Test
  public void testFindWorkerMatchesRegisteredHost()
  {
    final ImmutableWorkerInfo worker = workerInfo(HOST, ImmutableSet.of("t1"));
    final Optional<ImmutableWorkerInfo> found = WorkerHandoffHelper.findWorker(
        ImmutableList.of(worker),
        HOST
    );
    Assertions.assertTrue(found.isPresent());
    Assertions.assertEquals(HOST, found.get().getWorker().getHost());
  }

  @Test
  public void testFindWorkerUnknownHostIsAbsent()
  {
    final ImmutableWorkerInfo worker = workerInfo(HOST, ImmutableSet.of("t1"));
    Assertions.assertFalse(
        WorkerHandoffHelper.findWorker(ImmutableList.of(worker), "other-host:8091").isPresent()
    );
  }

  @Test
  public void testGroupTaskGroupsUsesSupervisorIdNotDataSource()
  {
    final Task primary = streamTask("index_kafka", "KubeState", "KubeState", 0);
    final Task haCopy = streamTask("index_kafka", "KubeState_gcp_moncevents", "KubeState", 1);
    final Function<String, Optional<Task>> lookup = id -> {
      if ("primary".equals(id)) {
        return Optional.of(primary);
      }
      if ("ha".equals(id)) {
        return Optional.of(haCopy);
      }
      return Optional.absent();
    };

    final Map<String, List<Integer>> groups = WorkerHandoffHelper.groupTaskGroups(
        ImmutableList.of("primary", "ha"),
        lookup
    );

    Assertions.assertEquals(ImmutableList.of(0), groups.get("KubeState"));
    Assertions.assertEquals(ImmutableList.of(1), groups.get("KubeState_gcp_moncevents"));
    Assertions.assertEquals(2, groups.size());
  }

  @Test
  public void testGroupTaskGroupsIncludesRabbit()
  {
    final Task rabbit = streamTask("index_rabbit", "orders", "orders", 2);
    final Map<String, List<Integer>> groups = WorkerHandoffHelper.groupTaskGroups(
        ImmutableList.of("r1"),
        id -> Optional.of(rabbit)
    );
    Assertions.assertEquals(ImmutableList.of(2), groups.get("orders"));
    Assertions.assertEquals(1, groups.size());
  }

  @Test
  public void testGroupTaskGroupsSkipsEmptySupervisorId()
  {
    final Task task = streamTask("index_kafka", "", "cAdvisor", 3);
    final Map<String, List<Integer>> groups = WorkerHandoffHelper.groupTaskGroups(
        ImmutableList.of("t1"),
        id -> Optional.of(task)
    );
    Assertions.assertTrue(groups.isEmpty());
  }

  @Test
  public void testGroupTaskGroupsSkipsCompact()
  {
    final Task compact = EasyMock.mock(Task.class);
    EasyMock.replay(compact);

    final Map<String, List<Integer>> groups = WorkerHandoffHelper.groupTaskGroups(
        ImmutableList.of("c1"),
        id -> Optional.of(compact)
    );
    Assertions.assertTrue(groups.isEmpty());
    EasyMock.verify(compact);
  }

  @Test
  public void testGroupTaskGroupsSkipsNoop()
  {
    final Map<String, List<Integer>> groups = WorkerHandoffHelper.groupTaskGroups(
        ImmutableList.of("noop"),
        id -> Optional.of(NoopTask.create())
    );
    Assertions.assertTrue(groups.isEmpty());
  }

  @Test
  public void testSubmitHandoffsInvokesEachSupervisor()
  {
    final SupervisorManager manager = EasyMock.mock(SupervisorManager.class);
    EasyMock.expect(manager.handoffTaskGroupsEarly("KubeState", ImmutableList.of(0))).andReturn(true);
    EasyMock.expect(manager.handoffTaskGroupsEarly("KubeState_gcp_moncevents", ImmutableList.of(1))).andReturn(true);
    EasyMock.replay(manager);

    final Map<String, List<Integer>> accepted = WorkerHandoffHelper.submitHandoffs(
        manager,
        ImmutableMap.of(
            "KubeState", ImmutableList.of(0),
            "KubeState_gcp_moncevents", ImmutableList.of(1)
        )
    );

    Assertions.assertEquals(ImmutableList.of(0), accepted.get("KubeState"));
    Assertions.assertEquals(ImmutableList.of(1), accepted.get("KubeState_gcp_moncevents"));
    EasyMock.verify(manager);
  }

  @Test
  public void testSubmitHandoffsSkipsMissingSupervisor()
  {
    final SupervisorManager manager = EasyMock.mock(SupervisorManager.class);
    EasyMock.expect(manager.handoffTaskGroupsEarly("gone", ImmutableList.of(0))).andReturn(false);
    EasyMock.replay(manager);

    final Map<String, List<Integer>> accepted = WorkerHandoffHelper.submitHandoffs(
        manager,
        ImmutableMap.of("gone", ImmutableList.of(0))
    );
    Assertions.assertTrue(accepted.isEmpty());
    EasyMock.verify(manager);
  }

  private static ImmutableWorkerInfo workerInfo(String host, java.util.Set<String> runningTasks)
  {
    return new ImmutableWorkerInfo(
        new Worker("http", host, "127.0.0.1", 4, "v1", "default"),
        runningTasks.size(),
        ImmutableSet.of(),
        runningTasks,
        DateTimes.nowUtc()
    );
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static SeekableStreamIndexTask streamTask(
      String type,
      String supervisorId,
      String dataSource,
      int taskGroupId
  )
  {
    final SeekableStreamIndexTaskIOConfig ioConfig = EasyMock.mock(SeekableStreamIndexTaskIOConfig.class);
    EasyMock.expect(ioConfig.getTaskGroupId()).andReturn(taskGroupId).anyTimes();
    final SeekableStreamIndexTask task = EasyMock.mock(SeekableStreamIndexTask.class);
    EasyMock.expect(task.getType()).andReturn(type).anyTimes();
    EasyMock.expect(task.getSupervisorId()).andReturn(supervisorId).anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn(dataSource).anyTimes();
    EasyMock.expect(task.getIOConfig()).andReturn(ioConfig).anyTimes();
    EasyMock.replay(ioConfig, task);
    return task;
  }
}
