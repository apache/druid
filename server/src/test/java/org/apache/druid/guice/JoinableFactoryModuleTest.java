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
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.join.NoopDataSource;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class JoinableFactoryModuleTest
{
  private Injector injector;

  @Before
  public void setUp()
  {
    injector = makeInjectorWithProperties();
  }

  @Test
  public void testInjectJoinableFactoryIsSingleton()
  {
    JoinableFactory factory = injector.getInstance(JoinableFactory.class);
    Assert.assertEquals(MapJoinableFactory.class, factory.getClass());
    JoinableFactory otherFactory = injector.getInstance(JoinableFactory.class);
    Assert.assertSame(factory, otherFactory);
  }

  @Test
  public void testInjectDefaultBindingsShouldBeInjected()
  {
    Map<Class<? extends DataSource>, JoinableFactory> joinableFactories =
        injector.getInstance(Key.get(new TypeLiteral<Map<Class<? extends DataSource>, JoinableFactory>>() {}));
    Assert.assertEquals(JoinableFactoryModule.FACTORY_MAPPINGS.size(), joinableFactories.size());

    final Set<Map.Entry<Class<? extends DataSource>, Class<? extends JoinableFactory>>> expectedEntries =
        JoinableFactoryModule.FACTORY_MAPPINGS.entrySet();

    for (Map.Entry<Class<? extends DataSource>, Class<? extends JoinableFactory>> entry : expectedEntries) {
      Assert.assertThat(joinableFactories.get(entry.getKey()), CoreMatchers.instanceOf(entry.getValue()));
    }
  }

  @Test
  public void testJoinableFactoryCanBind()
  {
    injector = makeInjectorWithProperties(
        binder -> DruidBinders
            .joinableFactoryBinder(binder).addBinding(NoopDataSource.class).toInstance(NoopJoinableFactory.INSTANCE));
    Map<Class<? extends DataSource>, JoinableFactory> joinableFactories =
        injector.getInstance(Key.get(new TypeLiteral<Map<Class<? extends DataSource>, JoinableFactory>>() {}));
    Assert.assertEquals(JoinableFactoryModule.FACTORY_MAPPINGS.size() + 1, joinableFactories.size());
    Assert.assertEquals(NoopJoinableFactory.INSTANCE, joinableFactories.get(NoopDataSource.class));
  }

  private Injector makeInjectorWithProperties(Module... otherModules)
  {
    final LookupExtractorFactoryContainerProvider lookupProvider =
        LookupEnabledTestExprMacroTable.createTestLookupProvider(Collections.emptyMap());

    final ImmutableList.Builder<Module> modulesBuilder =
        ImmutableList.<Module>builder()
            .add(new JoinableFactoryModule())
            .add(binder -> binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupProvider))
            .add(binder -> binder.bindScope(LazySingleton.class, Scopes.SINGLETON));

    for (final Module otherModule : otherModules) {
      modulesBuilder.add(otherModule);
    }

    return Guice.createInjector(modulesBuilder.build());
  }
}
