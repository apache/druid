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

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;

import io.airlift.airline.Command;
import io.druid.guice.IndexingServiceFirehoseModule;
import io.druid.guice.IndexingServiceModuleHelper;
import io.druid.guice.IndexingServiceTaskLogsModule;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.ForkingTaskRunner;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.worker.Worker;
import io.druid.indexing.worker.WorkerCuratorCoordinator;
import io.druid.indexing.worker.WorkerTaskMonitor;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.indexing.worker.http.WorkerResource;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.server.DruidNode;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

import java.util.List;

/**
 */
@Command(
    name = "middleManager",
    description = "Runs a Middle Manager, this is a \"task\" node used as part of the remote indexing service."
)
public class CliMiddleManager extends ServerRunnable
{
  private static final Logger log = new Logger(CliMiddleManager.class);

  public CliMiddleManager()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/middlemanager");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8091);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8291);

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);

            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);
            JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);

            binder.bind(TaskRunner.class).to(ForkingTaskRunner.class);
            binder.bind(ForkingTaskRunner.class).in(LazySingleton.class);

            binder.bind(ChatHandlerProvider.class).toProvider(Providers.<ChatHandlerProvider>of(null));

            binder.bind(WorkerTaskMonitor.class).in(ManageLifecycle.class);
            binder.bind(WorkerCuratorCoordinator.class).in(ManageLifecycle.class);

            LifecycleModule.register(binder, WorkerTaskMonitor.class);
            binder.bind(JettyServerInitializer.class).toInstance(new MiddleManagerJettyServerInitializer());
            Jerseys.addResource(binder, WorkerResource.class);

            LifecycleModule.register(binder, Server.class);
          }

          @Provides
          @LazySingleton
          public Worker getWorker(@Self DruidNode node, WorkerConfig config)
          {
            return new Worker(
                node.getServiceScheme(),
                node.getHostAndPortToUse(),
                config.getIp(),
                config.getCapacity(),
                config.getVersion()
            );
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceTaskLogsModule()
    );
  }
}
