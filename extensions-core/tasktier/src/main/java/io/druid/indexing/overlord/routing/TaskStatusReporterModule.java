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

package io.druid.indexing.overlord.routing;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import io.druid.client.indexing.IndexingServiceSelectorConfig;
import io.druid.guice.JacksonConfigProvider;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerFactory;
import io.druid.indexing.overlord.TierRoutingTaskRunnerFactory;
import io.druid.initialization.DruidModule;
import org.apache.curator.x.discovery.ServiceDiscovery;

import java.util.List;

public class TaskStatusReporterModule implements DruidModule
{
  private static final Logger LOG = new Logger(TaskStatusReporterModule.class);
  public static final String UPSTREAM_SERVICE_NAME_CONSTANT_KEY = "upstreamServiceName";
  public static final String UPSTREAM_TASK_REPORTER_NAME = "upstream";
  public static final String UPSTREAM_PROPERTY_KEY = "io.druid.index.tier.upstreamServiceName";
  public static final String POLYBIND_ROUTING_KEY = "routing";
  public static final String ROUTING_CONFIG_KEY = "druid.tier.routing.config";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.optionBinder(
        binder,
        Key.get(TaskRunnerFactory.class)
    ).addBinding(POLYBIND_ROUTING_KEY).to(TierRoutingTaskRunnerFactory.class);

    Jerseys.addResource(binder, TaskStatusPostToLeaderListenerResource.class);

    binder.bind(Key.get(TaskStatusReporter.class, Names.named(UPSTREAM_TASK_REPORTER_NAME)))
          .to(DynamicUpstreamReporter.class)
          .in(LazySingleton.class);
    JsonConfigProvider.bind(binder, UPSTREAM_PROPERTY_KEY, UpstreamNameHolder.class);
    binder.bind(Key.get(String.class, Names.named(UPSTREAM_SERVICE_NAME_CONSTANT_KEY)))
          .toProvider(UpstreamNameHolderProvider.class);
    JacksonConfigProvider.bind(binder, ROUTING_CONFIG_KEY, TierRouteConfig.class, null);
  }

  public static class UpstreamNameHolder
  {
    @JsonProperty
    String upstreamServiceName = IndexingServiceSelectorConfig.DEFAULT_SERVICE_NAME;

    public String getUpstreamServiceName()
    {
      return upstreamServiceName;
    }
  }

  public static class UpstreamNameHolderProvider implements Provider<String>
  {
    private final UpstreamNameHolder upstreamNameHolder;

    @Inject
    public UpstreamNameHolderProvider(
        UpstreamNameHolder upstreamNameHolder
    )
    {
      this.upstreamNameHolder = upstreamNameHolder;
    }

    @Override
    public String get()
    {
      return upstreamNameHolder.getUpstreamServiceName();
    }
  }

  public static class DynamicUpstreamReporter implements TaskStatusReporter
  {
    @Inject(optional = true)
    private TaskMaster taskMaster = null;
    @Inject
    private
    @Global
    HttpClient httpClient = null;
    @Inject
    private ServiceDiscovery<Void> discovery = null;
    @Inject
    private
    @Named(UPSTREAM_SERVICE_NAME_CONSTANT_KEY)
    String upstreamService = null;

    public DynamicUpstreamReporter() {}

    public DynamicUpstreamReporter(
        TaskMaster taskMaster,
        @Global HttpClient httpClient,
        ServiceDiscovery<Void> discovery,
        @Named(UPSTREAM_SERVICE_NAME_CONSTANT_KEY) String upstreamService
    )
    {
      this.taskMaster = taskMaster;
      this.httpClient = httpClient;
      this.discovery = discovery;
      this.upstreamService = upstreamService;
    }

    @Override
    public boolean reportStatus(TaskStatus status)
    {
      TaskStatusReporter reporter = new TaskStatusPostToLeaderReporter(httpClient, discovery, upstreamService);
      if (taskMaster != null) {
        final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
        if (taskRunnerOptional.isPresent()) {
          final TaskRunner runner = taskRunnerOptional.get();
          if (runner instanceof TaskStatusReporter) {
            reporter = (TaskStatusReporter) runner;
          } else {
            LOG.debug(
                "Expected [%s] but was [%s]. Trying POST",
                TaskStatusReporter.class,
                runner.getClass().getCanonicalName()
            );
          }
        } else {
          LOG.debug("No task runner. Trying POST");
        }
      } else {
        LOG.debug("No task master. Trying POST");
      }
      return reporter.reportStatus(status);
    }
  }
}
