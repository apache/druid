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


package org.apache.druid.guice;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.log.EmittingRequestLogger;
import org.apache.druid.server.log.EmittingRequestLoggerProvider;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.log.NoopRequestLoggerProvider;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.RequestLoggerProvider;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class QueryableModuleTest
{

  @Test
  public void testGetEmitterRequestLoggerProvider()
  {
    Properties properties = new Properties();
    properties.setProperty("druid.request.logging.type", "emitter");
    properties.setProperty("druid.request.logging.feed", "requestlog");
    final Injector injector = makeInjector(properties);
    RequestLoggerProvider emittingRequestLoggerProvider = injector.getInstance(RequestLoggerProvider.class);
    Assert.assertTrue(emittingRequestLoggerProvider instanceof EmittingRequestLoggerProvider);
    RequestLogger requestLogger = emittingRequestLoggerProvider.get();
    Assert.assertTrue(requestLogger instanceof EmittingRequestLogger);
  }

  @Test
  public void testGetDefaultRequestLoggerProvider()
  {
    Properties properties = new Properties();
    final Injector injector = makeInjector(properties);
    RequestLoggerProvider emittingRequestLoggerProvider = injector.getInstance(RequestLoggerProvider.class);
    Assert.assertTrue(emittingRequestLoggerProvider instanceof NoopRequestLoggerProvider);
    RequestLogger requestLogger = emittingRequestLoggerProvider.get();
    Assert.assertTrue(requestLogger instanceof NoopRequestLogger);
  }


  private Injector makeInjector(Properties properties)
  {
    Injector injector = Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new JacksonModule(),
            new ConfigModule(),
            new QueryRunnerFactoryModule(),
            new DruidProcessingConfigModule(),
            new BrokerProcessingModule(),
            new LifecycleModule(),
            binder -> binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class),
            binder -> binder.bind(Properties.class).toInstance(properties),
            new QueryableModule()
        )
    );

    ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    mapper.registerModules(new QueryableModule().getJacksonModules());
    mapper.setInjectableValues(new InjectableValues.Std()
                                   .addValue(ServiceEmitter.class, new NoopServiceEmitter())
    );

    return injector;
  }
}
