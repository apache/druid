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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.ImmutableZkWorker;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.worker.Worker;
import org.junit.Assert;
import org.junit.Test;

public class EqualDistributionWorkerSelectStrategyTest
{

  @Test
  public void testFindWorkerForTask() throws Exception
  {
    final EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWorkerSelectStrategy();

    Optional<ImmutableZkWorker> optional = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableZkWorker(
                new Worker("lhost", "lhost", 1, "v1"), 0,
                Sets.<String>newHashSet()
            ),
            "localhost",
            new ImmutableZkWorker(
                new Worker("localhost", "localhost", 1, "v1"), 1,
                Sets.<String>newHashSet()
            )
        ),
        new NoopTask(null, 1, 0, null, null, null)
        {
          @Override
          public String getDataSource()
          {
            return "foo";
          }
        }
    );
    ImmutableZkWorker worker = optional.get();
    Assert.assertEquals("lhost", worker.getWorker().getHost());
  }
}
