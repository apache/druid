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

package org.apache.druid.k8s.overlord;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.task.Task;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class RunnerStrategyTest extends EasyMockSupport
{
  @Mock Task task;

  @Test
  public void test_mixRunnerStrategy_returnsCorrectRunnerType()
  {
    MixRunnerStrategy runnerStrategy = new MixRunnerStrategy("k8s", null, ImmutableMap.of("index_kafka", "worker"));
    EasyMock.expect(task.getType()).andReturn("index_kafka");
    EasyMock.expectLastCall().once();
    EasyMock.expect(task.getType()).andReturn("compact");
    EasyMock.expectLastCall().once();
    replayAll();
    Assert.assertEquals("httpRemote", runnerStrategy.getRunnerTypeForTask(task).getType());
    Assert.assertEquals("k8s", runnerStrategy.getRunnerTypeForTask(task).getType());
    verifyAll();
  }

  @Test
  public void test_kubernetesRunnerStrategy_returnsCorrectRunnerType()
  {
    KubernetesRunnerStrategy runnerStrategy = new KubernetesRunnerStrategy();

    Assert.assertEquals("k8s", runnerStrategy.getRunnerTypeForTask(task).getType());
  }

  @Test
  public void test_workerRunnerStrategy_returnsCorrectRunnerType()
  {
    WorkerRunnerStrategy runnerStrategy = new WorkerRunnerStrategy("remote");
    Assert.assertEquals("remote", runnerStrategy.getRunnerTypeForTask(task).getType());

    runnerStrategy = new WorkerRunnerStrategy(null);
    Assert.assertEquals("httpRemote", runnerStrategy.getRunnerTypeForTask(task).getType());
  }

}
