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

package org.apache.druid.k8s.overlord.runnerstrategy;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerFactory;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(EasyMockExtension.class)
public class TaskTypeRunnerStrategyTest extends EasyMockSupport
{
  @Mock
  Task task;

  @Test
  public void test_taskTypeRunnerStrategy_returnsCorrectRunnerType()
  {
    TaskTypeRunnerStrategy runnerStrategy = new TaskTypeRunnerStrategy("k8s", ImmutableMap.of("index_kafka", "worker"));
    EasyMock.expect(task.getType()).andReturn("index_kafka");
    EasyMock.expectLastCall().once();
    EasyMock.expect(task.getType()).andReturn("compact");
    EasyMock.expectLastCall().once();
    replayAll();
    Assertions.assertEquals(RunnerStrategy.WORKER_NAME, runnerStrategy.getRunnerTypeForTask(task).getType());
    Assertions.assertEquals(KubernetesTaskRunnerFactory.TYPE_NAME, runnerStrategy.getRunnerTypeForTask(task).getType());
    verifyAll();
  }

  @Test
  public void test_invalidOverridesConfig_shouldThrowException()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () ->
        new TaskTypeRunnerStrategy(
            "k8s",
            ImmutableMap.of(
                "index_kafka",
                "non_exist_runner"
            )
        )
    );
  }
}
