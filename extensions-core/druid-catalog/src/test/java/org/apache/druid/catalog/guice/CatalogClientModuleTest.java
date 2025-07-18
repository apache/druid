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

package org.apache.druid.catalog.guice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.catalog.sync.CatalogClientConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.initialization.ServerInjectorBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CatalogClientModuleTest
{
  @Test
  public void testClientConfigDefaults()
  {
    Injector injector = makeInjector(new Properties());
    CatalogClientConfig clientConfig = injector.getInstance(CatalogClientConfig.class);
    Assertions.assertEquals(TimeUnit.MINUTES.toMillis(1), clientConfig.getPollingPeriod());
    Assertions.assertEquals(TimeUnit.SECONDS.toMillis(10), clientConfig.getMaxRandomDelay());
    Assertions.assertEquals(5, clientConfig.getMaxSyncRetries());
  }

  @Test
  public void testClientConfigOverrides()
  {
    Properties props = new Properties();
    props.setProperty("druid.catalog.client.pollingPeriod", "300000");
    props.setProperty("druid.catalog.client.maxRandomDelay", "60000");
    props.setProperty("druid.catalog.client.maxSyncRetries", "1");
    Injector injector = makeInjector(props);
    CatalogClientConfig clientConfig = injector.getInstance(CatalogClientConfig.class);
    Assertions.assertEquals(TimeUnit.MINUTES.toMillis(5), clientConfig.getPollingPeriod());
    Assertions.assertEquals(TimeUnit.MINUTES.toMillis(1), clientConfig.getMaxRandomDelay());
    Assertions.assertEquals(1, clientConfig.getMaxSyncRetries());
  }

  private Injector makeInjector(Properties props)
  {
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        GuiceInjectors.makeStartupInjector(),
        ImmutableSet.of(NodeRole.BROKER),
        ImmutableList.of(
            (Module) binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              binder.bind(Properties.class).toInstance(props);
            },
            new CatalogClientModule()
        )
    );
    return injector;
  }
}
