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

package io.druid.client.cache;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import org.junit.Assert;
import org.junit.Test;

public class CacheMonitorTest
{
  @Test
  public void testOptionalInject() throws Exception
  {
    Injector injector = Initialization.makeInjectorWithModules(GuiceInjectors.makeStartupInjector(), ImmutableList.of(
        new Module() {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null, null, new ServerConfig())
            );
          }
        }
    ));

    CacheMonitor monitor = injector.getInstance(CacheMonitor.class);
    Assert.assertNull(monitor.cache);
  }

  @Test
  public void testInject() throws Exception
  {
    Injector injector = Initialization.makeInjectorWithModules(GuiceInjectors.makeStartupInjector(), ImmutableList.of(
        new Module() {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null, null, new ServerConfig())
            );
            binder.bind(Cache.class).toInstance(MapCache.create(0));
          }
        }
    ));

    CacheMonitor monitor = injector.getInstance(CacheMonitor.class);
    Assert.assertNotNull(monitor.cache);
  }
}
