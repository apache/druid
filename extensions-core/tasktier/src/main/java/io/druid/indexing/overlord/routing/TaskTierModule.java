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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.name.Names;
import io.druid.client.indexing.IndexingServiceSelectorConfig;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.indexing.overlord.TaskRunnerFactory;
import io.druid.indexing.overlord.TierRoutingTaskRunnerFactory;
import io.druid.initialization.DruidModule;
import io.druid.server.initialization.jetty.JettyBindings;

import java.util.List;

public class TaskTierModule implements DruidModule
{
  public static final String UPSTREAM_SERVICE_NAME_CONSTANT_KEY = "upstreamServiceName";
  public static final String UPSTREAM_TASK_REPORTER_NAME = "upstream";
  public static final String UPSTREAM_PROPERTY_KEY = "io.druid.index.tier.upstreamServiceName";
  public static final String POLYBIND_ROUTING_KEY = "routing";

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
    binder.bind(TierRoutingTaskRunnerFactory.class).in(LazySingleton.class);

    Jerseys.addResource(binder, TaskStatusPostToLeaderListenerResource.class);
    JettyBindings.addQosFilter(binder, TaskStatusPostToLeaderListenerResource.PATH, 2);

    binder.bind(Key.get(TaskStatusReporter.class, Names.named(UPSTREAM_TASK_REPORTER_NAME)))
          .to(DynamicUpstreamReporter.class)
          .in(LazySingleton.class);
    JsonConfigProvider.bind(binder, UPSTREAM_PROPERTY_KEY, UpstreamNameHolder.class);
    binder.bind(Key.get(String.class, Names.named(UPSTREAM_SERVICE_NAME_CONSTANT_KEY)))
          .toProvider(UpstreamNameHolderProvider.class);
  }
}

class UpstreamNameHolder
{
  @JsonProperty
  String upstreamServiceName = IndexingServiceSelectorConfig.DEFAULT_SERVICE_NAME;

  public String getUpstreamServiceName()
  {
    return upstreamServiceName;
  }
}

class UpstreamNameHolderProvider implements Provider<String>
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
