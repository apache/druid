/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.indexing.overlord.setup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.worker.Worker;
import io.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;



public class EqualDistributionWithLimitWorkerSelectStrategyTest
{
  private final EqualDistributionWithLimitWorkerSelectStrategy DEMO = new EqualDistributionWithLimitWorkerSelectStrategy(
      new LimitConfig(ImmutableMap.of("index_realtime", 2, "index_hadoop", 1, "index_realtime_foo", 1))
  );
  @Test
  public void testFindWorkerForTaskWithLimit() throws Exception
  {
    RemoteTaskRunnerConfig config = new RemoteTaskRunnerConfig();
    ImmutableWorkerInfo workerInfo = new ImmutableWorkerInfo(
        new Worker("http", "localhost0", "localhost0", 3, "v1"), 2,
        Sets.<String>newHashSet(),
        Sets.<String>newHashSet(
          "index_realtime_foo_0",
          "index_hadoop_foo_1"
        ),
        DateTimes.nowUtc()
    );
    ImmutableWorkerInfo worker = DEMO.findWorkerForTask(
        config,
        ImmutableMap.of(
          "localhost0",
          workerInfo
        ),
        new NoopTask("index_hadoop", 1, 0, null, null, null)
    );
    Assert.assertNull(worker);
    worker = DEMO.findWorkerForTask(
      config,
      ImmutableMap.of(
        "localhost0",
        workerInfo
      ),
      new NoopTask("index_realtime_foo", 1, 0, null, null, null)
    );
    Assert.assertNull(worker);
    worker = DEMO.findWorkerForTask(
      config,
      ImmutableMap.of(
        "localhost0",
        workerInfo
      ),
      new NoopTask("index_realtime", 1, 0, null, null, null)
    );
    Assert.assertEquals("localhost0", worker.getWorker().getHost());
  }
  @Test
  public void testFindWorkerForTaskWithoutLimit() throws Exception
  {
    ImmutableWorkerInfo worker = DEMO.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
          "localhost0",
          new ImmutableWorkerInfo(
            new Worker("http", "localhost0", "localhost0", 3, "v1"), 2,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(
              "index_realtime_foo_0",
              "index_realtime_foo_1"
            ),
            DateTimes.nowUtc()
          )
        ),
        new NoopTask("kill", 1, 0, null, null, null)
    );
    Assert.assertEquals("localhost0", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithNull() throws Exception
  {
    ImmutableWorkerInfo worker = DEMO.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
          "localhost0",
          new ImmutableWorkerInfo(
            new Worker("http", "localhost0", "localhost0", 3, "v1"), 2,
            Sets.<String>newHashSet(),
            Sets.<String>newHashSet(
              "index_realtime_foo_0",
              "index_realtime_foo_1"
            ),
            DateTimes.nowUtc()
          )
        ),
        new NoopTask(null, 1, 0, null, null, null)
    );
    Assert.assertEquals("localhost0", worker.getWorker().getHost());
  }
}

