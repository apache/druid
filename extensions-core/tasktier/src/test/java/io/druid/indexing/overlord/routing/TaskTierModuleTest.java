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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.cli.CliOverlord;
import io.druid.common.config.JacksonConfigManager;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.annotations.Self;
import io.druid.indexing.overlord.TaskRunnerFactory;
import io.druid.indexing.overlord.TierRoutingTaskRunner;
import io.druid.indexing.overlord.TierRoutingTaskRunnerFactory;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

public class TaskTierModuleTest
{
  @Test
  public void testUpstreamNameHolder() throws Exception
  {
    final String name = "foo";
    final UpstreamNameHolder upstreamNameHolder = new UpstreamNameHolder();
    upstreamNameHolder.upstreamServiceName = name;
    Assert.assertEquals(name, upstreamNameHolder.getUpstreamServiceName());
    final ObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        name,
        mapper.readValue(
            mapper.writeValueAsString(upstreamNameHolder),
            UpstreamNameHolder.class
        ).getUpstreamServiceName()
    );
    final UpstreamNameHolderProvider provider = new UpstreamNameHolderProvider(
        upstreamNameHolder);
    Assert.assertEquals(name, provider.get());
  }

  @Test
  public void testTaskRunnerFactoryBinding()
  {
    final String propertyPrefix = "druid.indexer.runner.type";
    final Collection<Module> modules = new ArrayList<>();
    modules.addAll(new CliOverlord().getModules());
    final Injector injector = Initialization.makeInjectorWithModules(GuiceInjectors.makeStartupInjector(), modules);
    final Properties properties = injector.getInstance(Properties.class);
    properties.setProperty(propertyPrefix, TaskTierModule.POLYBIND_ROUTING_KEY);
    final TaskRunnerFactory factory = injector.getInstance(TaskRunnerFactory.class);
    final TierRoutingTaskRunnerFactory tierRoutingTaskRunnerFactory = (TierRoutingTaskRunnerFactory)factory;
    final TierRoutingTaskRunner runner = tierRoutingTaskRunnerFactory.build();
    Assert.assertEquals(ImmutableList.of(), runner.getKnownTasks());
    Assert.assertEquals(ImmutableList.of(), runner.getPendingTasks());
    Assert.assertEquals(ImmutableList.of(), runner.getRunningTasks());
  }

  @Test
  public void testTierRouteConfigBinding()
  {
    final Injector injector = Initialization.makeInjectorWithModules(GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null)
            );
          }
        }
    ));
    final JacksonConfigManager jacksonConfigManager = injector.getInstance(JacksonConfigManager.class);
    final TierRouteConfig routeConfig;
  }
}
