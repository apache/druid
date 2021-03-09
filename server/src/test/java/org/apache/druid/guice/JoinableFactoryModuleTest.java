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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.join.BroadcastTableJoinableFactory;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.LookupJoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.join.NoopDataSource;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.server.SegmentManager;
import org.easymock.EasyMock;
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
    final Set<JoinableFactory> factories =
        injector.getInstance(Key.get(new TypeLiteral<Set<JoinableFactory>>() {}));
    Assert.assertEquals(JoinableFactoryModule.FACTORY_MAPPINGS.size(), factories.size());
    Map<Class<? extends JoinableFactory>, Class<? extends DataSource>> joinableFactoriesMappings = injector.getInstance(
        Key.get(new TypeLiteral<Map<Class<? extends JoinableFactory>, Class<? extends DataSource>>>() {})
    );
    assertDefaultFactories(joinableFactoriesMappings);
  }

  @Test
  public void testJoinableFactoryCanBind()
  {
    injector = makeInjectorWithProperties(
        binder -> {
          DruidBinders.joinableFactoryMultiBinder(binder).addBinding().toInstance(NoopJoinableFactory.INSTANCE);
          DruidBinders.joinableMappingBinder(binder).addBinding(NoopJoinableFactory.class).toInstance(NoopDataSource.class);
        }
    );
    Map<Class<? extends JoinableFactory>, Class<? extends DataSource>> joinableFactoriesMappings = injector.getInstance(
        Key.get(new TypeLiteral<Map<Class<? extends JoinableFactory>, Class<? extends DataSource>>>() {})
    );
    Set<JoinableFactory> factories = injector.getInstance(Key.get(new TypeLiteral<Set<JoinableFactory>>() {}));

    Assert.assertEquals(JoinableFactoryModule.FACTORY_MAPPINGS.size() + 1, factories.size());
    Assert.assertEquals(NoopDataSource.class, joinableFactoriesMappings.get(NoopJoinableFactory.class));
    assertDefaultFactories(joinableFactoriesMappings);
  }

  private void assertDefaultFactories(
      Map<Class<? extends JoinableFactory>, Class<? extends DataSource>> joinableFactoriesMappings
  )
  {
    Assert.assertEquals(LookupDataSource.class, joinableFactoriesMappings.get(LookupJoinableFactory.class));
    Assert.assertEquals(InlineDataSource.class, joinableFactoriesMappings.get(InlineJoinableFactory.class));
    Assert.assertEquals(
        GlobalTableDataSource.class,
        joinableFactoriesMappings.get(BroadcastTableJoinableFactory.class)
    );
  }

  private Injector makeInjectorWithProperties(Module... otherModules)
  {
    final LookupExtractorFactoryContainerProvider lookupProvider =
        LookupEnabledTestExprMacroTable.createTestLookupProvider(Collections.emptyMap());

    final ImmutableList.Builder<Module> modulesBuilder =
        ImmutableList.<Module>builder()
            .add(new JoinableFactoryModule())
            .add(binder -> binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupProvider))
            .add(binder -> binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class)))
            .add(binder -> binder.bindScope(LazySingleton.class, Scopes.SINGLETON));

    for (final Module otherModule : otherModules) {
      modulesBuilder.add(otherModule);
    }

    return Guice.createInjector(modulesBuilder.build());
  }
}
