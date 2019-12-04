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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class EqualDistributionWithAffinityWorkerSelectStrategyTest
{
  @Test
  public void testFindWorkerForTask()
  {
    EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWithAffinityWorkerSelectStrategy(
        new AffinityConfig(ImmutableMap.of("foo", ImmutableSet.of("localhost1", "localhost2", "localhost3")), false)
    );

    NoopTask noopTask = new NoopTask(null, null, null, 1, 0, null, null, null)
    {
      @Override
      public String getDataSource()
      {
        return "foo";
      }
    };
    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
            new RemoteTaskRunnerConfig(),
            ImmutableMap.of(
                    "localhost0",
                    new ImmutableWorkerInfo(
                        new Worker("http", "localhost0", "localhost0", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                        new HashSet<>(),
                        new HashSet<>(),
                        DateTimes.nowUtc()
                    ),
                    "localhost1",
                    new ImmutableWorkerInfo(
                        new Worker("http", "localhost1", "localhost1", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                        new HashSet<>(),
                        new HashSet<>(),
                        DateTimes.nowUtc()
                    ),
                    "localhost2",
                    new ImmutableWorkerInfo(
                        new Worker("http", "localhost2", "localhost2", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 1,
                        new HashSet<>(),
                        new HashSet<>(),
                        DateTimes.nowUtc()
                    ),
                    "localhost3",
                    new ImmutableWorkerInfo(
                        new Worker("http", "localhost3", "localhost3", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 1,
                        new HashSet<>(),
                        new HashSet<>(),
                        DateTimes.nowUtc()
                    )
            ),
            noopTask
    );
    Assert.assertEquals("localhost1", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithNulls()
  {
    EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWithAffinityWorkerSelectStrategy(
            new AffinityConfig(ImmutableMap.of("foo", ImmutableSet.of("localhost")), false)
    );

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
            new RemoteTaskRunnerConfig(),
            ImmutableMap.of(
                    "lhost",
                    new ImmutableWorkerInfo(
                        new Worker("http", "lhost", "lhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                        new HashSet<>(),
                        new HashSet<>(),
                        DateTimes.nowUtc()
                    ),
                    "localhost",
                    new ImmutableWorkerInfo(
                        new Worker("http", "localhost", "localhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                        new HashSet<>(),
                        new HashSet<>(),
                        DateTimes.nowUtc()
                    )
            ),
            new NoopTask(null, null, null, 1, 0, null, null, null)
    );
    Assert.assertEquals("lhost", worker.getWorker().getHost());
  }

  @Test
  public void testIsolation()
  {
    EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWithAffinityWorkerSelectStrategy(
            new AffinityConfig(ImmutableMap.of("foo", ImmutableSet.of("localhost")), false)
    );

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
            new RemoteTaskRunnerConfig(),
            ImmutableMap.of(
                    "localhost",
                    new ImmutableWorkerInfo(
                        new Worker("http", "localhost", "localhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                        new HashSet<>(),
                        new HashSet<>(),
                        DateTimes.nowUtc()
                    )
            ),
            new NoopTask(null, null, null, 1, 0, null, null, null)
    );
    Assert.assertNull(worker);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    final EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWithAffinityWorkerSelectStrategy(
        new AffinityConfig(ImmutableMap.of("foo", ImmutableSet.of("localhost")), false)
    );
    final WorkerSelectStrategy strategy2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(strategy),
        WorkerSelectStrategy.class
    );
    Assert.assertEquals(strategy, strategy2);
  }
}
