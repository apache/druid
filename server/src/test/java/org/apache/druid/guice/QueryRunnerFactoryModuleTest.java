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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

public class QueryRunnerFactoryModuleTest
{
  @Test
  public void testRegistersSelectedQueryTypesOnly()
  {
    final Injector injector = Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new ConfigModule(),
            new QueryableModule(),
            new QueryRunnerFactoryModule(ImmutableSet.of(ScanQuery.class)),
            new LifecycleModule(),
            binder -> binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class),
            binder -> binder.bind(Properties.class).toInstance(new Properties())
        )
    );

    final Map<Class<? extends Query>, QueryRunnerFactory> queryRunnerFactories = injector.getInstance(
        Key.get(new TypeLiteral<>() {})
    );
    final Map<Class<? extends Query>, QueryToolChest> queryToolChests = injector.getInstance(
        Key.get(new TypeLiteral<>() {})
    );

    Assertions.assertEquals(ImmutableSet.of(ScanQuery.class), queryRunnerFactories.keySet());
    Assertions.assertEquals(ImmutableSet.of(ScanQuery.class), queryToolChests.keySet());
    Assertions.assertNotNull(injector.getExistingBinding(Key.get(ScanQueryConfig.class)));
    Assertions.assertNull(injector.getExistingBinding(Key.get(GroupByQueryConfig.class)));
  }

  @Test
  public void testRejectsUnsupportedQueryType()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new QueryRunnerFactoryModule(ImmutableSet.of(Query.class))
    );
  }
}
