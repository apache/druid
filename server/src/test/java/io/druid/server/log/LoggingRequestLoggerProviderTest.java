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

package io.druid.server.log;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.QueryableModule;
import io.druid.initialization.Initialization;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;

public class LoggingRequestLoggerProviderTest
{
  private final String propertyPrefix = UUID.randomUUID().toString().replace('-', '_');
  private final JsonConfigProvider<RequestLoggerProvider> provider = JsonConfigProvider.of(
      propertyPrefix,
      RequestLoggerProvider.class
  );
  private final Injector injector = makeInjector();

  @Test
  public void testDefaultConfigParsing()
  {
    final Properties properties = new Properties();
    properties.put(propertyPrefix + ".type", "slf4j");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final LoggingRequestLogger requestLogger = (LoggingRequestLogger) provider.get().get().get();
    Assert.assertFalse(requestLogger.isSetContextMDC());
    Assert.assertFalse(requestLogger.isSetMDC());
  }

  @Test
  public void testConfigParsingFull()
  {
    final Properties properties = new Properties();
    properties.put(propertyPrefix + ".type", "slf4j");
    properties.put(propertyPrefix + ".setMDC", "true");
    properties.put(propertyPrefix + ".setContextMDC", "true");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final LoggingRequestLogger requestLogger = (LoggingRequestLogger) provider.get().get().get();
    Assert.assertTrue(requestLogger.isSetContextMDC());
    Assert.assertTrue(requestLogger.isSetMDC());
  }

  private Injector makeInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new QueryableModule()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(RequestLogger.class).toProvider(RequestLoggerProvider.class).in(ManageLifecycle.class);
                binder.bind(Key.get(String.class, Names.named("serviceName"))).toInstance("some service");
                binder.bind(Key.get(Integer.class, Names.named("servicePort"))).toInstance(0);
                binder.bind(Key.get(Integer.class, Names.named("tlsServicePort"))).toInstance(-1);
                JsonConfigProvider.bind(binder, propertyPrefix, RequestLoggerProvider.class);
              }
            }
        )
    );
  }
}
