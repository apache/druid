/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.util.Modules;
import io.airlift.airline.Command;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TierLocalTaskRunner;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.worker.Worker;
import io.druid.indexing.worker.WorkerCuratorCoordinator;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;

@Command(
    name = "local",
    description = "Runs a task as part of a tier. This command creates and monitors a 'fork' tier task locally"
)
public class CliTierLocal extends CliMiddleManager
{
  private static final Logger LOG = new Logger(CliTierLocal.class);

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(Modules.override(super.getModules()).with(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TaskRunner.class).to(TierLocalTaskRunner.class);
            binder.bind(TierLocalTaskRunner.class).in(LazySingleton.class);
          }

          @Provides
          @ManageLifecycle
          public WorkerCuratorCoordinator disabledWorkerCuratorCoordinator(
              ObjectMapper jsonMapper,
              IndexerZkConfig indexerZkConfig,
              RemoteTaskRunnerConfig config,
              CuratorFramework curatorFramework,
              Worker worker
          )
          {
            return new WorkerCuratorCoordinator(jsonMapper, indexerZkConfig, config, curatorFramework, worker)
            {
              @Override
              public void start() throws Exception
              {
                LOG.debug("WorkerCuratorCoordinator Start NOOP");
              }

              @Override
              public void stop() throws Exception
              {
                LOG.debug("WorkerCuratorCoordinator Stop NOOP");
              }
            };
          }
        }
    ));
  }
}
