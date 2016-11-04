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
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import io.airlift.airline.Command;
import io.druid.guice.Jerseys;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.ManageLifecycleLast;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.RemoteChatHandler;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.PortWriter;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import io.druid.indexing.overlord.resources.DeadhandMonitor;
import io.druid.indexing.overlord.resources.DeadhandResource;
import io.druid.indexing.overlord.resources.ShutdownCleanlyResource;
import io.druid.indexing.overlord.resources.TaskLogResource;
import io.druid.indexing.overlord.resources.TierRunningCheckResource;
import io.druid.indexing.worker.executor.ExecutorLifecycle;
import io.druid.indexing.worker.executor.ExecutorLifecycleConfig;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.jetty.ChatHandlerServerModule;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Command(
    name = "fork",
    description = "Runs a task as part of a tier. This command forks / joins the task in the same JVM. "
                  + "It expects the current working path to contain the files of interest"
)
public class CliTierFork extends CliPeon
{
  private static final Logger LOG = new Logger(CliTierFork.class);

  @Override
  protected List<? extends Module> getModules()
  {
    final List<Module> modules = new ArrayList<>(super.getModules());
    final Iterator<Module> moduleIterator = modules.iterator();
    while (moduleIterator.hasNext()) {
      if (moduleIterator.next() instanceof ChatHandlerServerModule) {
        // ChatHandlerServerModule injection is all screwed up
        moduleIterator.remove();
        break;
      }
    }
    modules.add(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(PortWriter.class).in(ManageLifecycleLast.class);
            LifecycleModule.register(binder, PortWriter.class);

            binder.bind(ParentMonitorInputStreamFaker.class)
                  .toProvider(ParentMonitorInputStreamFakerProvider.class)
                  .in(LazySingleton.class);

            Jerseys.addResource(binder, ShutdownCleanlyResource.class);
            Jerseys.addResource(binder, DeadhandResource.class);
            Jerseys.addResource(binder, TierRunningCheckResource.class);
            Jerseys.addResource(binder, TaskLogResource.class);

            binder.bind(DeadhandMonitor.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, DeadhandMonitor.class);

            binder.bind(ForkAnnouncer.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, ForkAnnouncer.class);

            // Chat handler not used
            binder.bind(DruidNode.class)
                  .annotatedWith(RemoteChatHandler.class)
                  .to(Key.get(DruidNode.class, Self.class));
            binder.bind(ServerConfig.class).annotatedWith(RemoteChatHandler.class).to(Key.get(ServerConfig.class));
          }
        }
    );
    return modules;
  }

  @Provides
  @ManageLifecycle
  public static ExecutorLifecycle executorLifecycleProvider(
      final TaskActionClientFactory taskActionClientFactory,
      final TaskRunner taskRunner,
      final TaskConfig taskConfig,
      final ParentMonitorInputStreamFaker parentStream,
      final @Json ObjectMapper jsonMapper,
      final ExecutorLifecycleConfig config
  )
  {
    final File taskFile = config.getTaskFile();
    final File statusFile = config.getStatusFile();
    try {
      if (!statusFile.exists() && !statusFile.createNewFile()) {
        throw new IOException(String.format("Could not create file [%s]", statusFile));
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    LOG.info("Using status and task files: [%s] [%s]", statusFile, taskFile);
    return new ExecutorLifecycle(
        new ExecutorLifecycleConfig()
        {
          // This stream closes whenever the parent dies.
          @Override
          public InputStream getParentStream()
          {
            return parentStream;
          }
        }
            .setStatusFile(statusFile)
            .setTaskFile(taskFile),
        taskConfig,
        taskActionClientFactory,
        taskRunner,
        jsonMapper
    );
  }
}

class ParentMonitorInputStreamFakerProvider implements Provider<ParentMonitorInputStreamFaker>
{
  final DeadhandResource deadhandResource;
  final TierLocalTaskRunnerConfig tierLocalTaskRunnerConfig;

  @Inject
  ParentMonitorInputStreamFakerProvider(
      final DeadhandResource deadhandResource,
      final TierLocalTaskRunnerConfig tierLocalTaskRunnerConfig
  )
  {
    this.deadhandResource = deadhandResource;
    this.tierLocalTaskRunnerConfig = tierLocalTaskRunnerConfig;
  }

  @Override
  public ParentMonitorInputStreamFaker get()
  {
    return new ParentMonitorInputStreamFaker()
    {
      @Override
      public int read() throws IOException
      {
        try {
          deadhandResource.waitForHeartbeat(tierLocalTaskRunnerConfig.getHeartbeatTimeLimit());
          return 0;
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
        catch (TimeoutException e) {
          // fake EOS
          return -1;
        }
      }
    };
  }
}

abstract class ParentMonitorInputStreamFaker extends InputStream
{

}
