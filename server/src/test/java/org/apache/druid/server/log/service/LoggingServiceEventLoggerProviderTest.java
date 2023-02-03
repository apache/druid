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

package org.apache.druid.server.log.service;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ServiceLogEventModule;
import org.apache.druid.initialization.Initialization;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;

public class LoggingServiceEventLoggerProviderTest
{
  private final String propertyPrefix = UUID.randomUUID().toString().replace('-', '_');
  private final JsonConfigProvider<ServiceEventLoggerProvider> provider = JsonConfigProvider.of(
      propertyPrefix,
      ServiceEventLoggerProvider.class
  );
  private final Injector injector = makeInjector();

  @Test
  public void testNoopConfigParsing()
  {
    final Properties properties = new Properties();
    properties.put(propertyPrefix + ".type", "noop");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    Assert.assertThat(provider.get().get().get(), Matchers.instanceOf(NoopServiceEventLogger.class));
  }

  @Test
  public void testEmitterConfigParsing()
  {
    final Properties properties = new Properties();
    properties.put(propertyPrefix + ".type", "emitter");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    Assert.assertThat(provider.get().get().get(), Matchers.instanceOf(EmittingServiceEventLogger.class));
  }

  private Injector makeInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new ServiceLogEventModule()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(ServiceEventLogger.class).toProvider(ServiceEventLoggerProvider.class).in(ManageLifecycle.class);
                binder.bind(Key.get(String.class, Names.named("serviceName"))).toInstance("some service");
                binder.bind(Key.get(Integer.class, Names.named("servicePort"))).toInstance(0);
                binder.bind(Key.get(Integer.class, Names.named("tlsServicePort"))).toInstance(-1);
                JsonConfigProvider.bind(binder, propertyPrefix, ServiceEventLoggerProvider.class);
              }
            }
        )
    );
  }
}
