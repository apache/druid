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

package org.apache.druid.query.lookup;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.metrics.TaskHolder;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.metrics.DefaultLoadSpecHolder;
import org.apache.druid.server.metrics.LoadSpecHolder;
import org.apache.druid.server.metrics.TestLoadSpecHolder;
import org.apache.druid.server.metrics.TestTaskHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.Set;

public class LookupListeningAnnouncerConfigTest
{
  private static final String PROPERTY_BASE = "some.property";
  private final Injector injector = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(),
      ImmutableList.of(
          new Module()
          {
            @Override
            public void configure(Binder binder)
            {
              JsonConfigProvider.bindInstance(
                  binder,
                  Key.get(DruidNode.class, Self.class),
                  new DruidNode("test-inject", null, false, null, null, true, false)
              );
              binder.bind(TaskHolder.class).toInstance(new TestTaskHolder("some_datasource", "some_taskid", "test_tasktype", "test_groupid"));
              binder.bind(LoadSpecHolder.class).toInstance(
                  new TestLoadSpecHolder(LookupLoadingSpec.loadOnly(Set.of("lookupName1", "lookupName2")), BroadcastDatasourceLoadingSpec.ALL)
              );
            }
          },
          new LookupModule()
      )
  );

  private final Properties properties = injector.getInstance(Properties.class);

  @Before
  public void setUp()
  {
    properties.clear();
  }

  @Test
  public void testDefaultInjection()
  {
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        PROPERTY_BASE,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get();
    Assert.assertEquals(LookupListeningAnnouncerConfig.DEFAULT_TIER, config.getLookupTier());
  }

  @Test
  public void testSimpleInjection()
  {
    final String lookupTier = "some_tier";
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(PROPERTY_BASE + ".lookupTier", lookupTier);
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        PROPERTY_BASE,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get();
    Assert.assertEquals(lookupTier, config.getLookupTier());
  }

  @Test(expected = NullPointerException.class)
  public void testFailsOnEmptyTier()
  {
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(PROPERTY_BASE + ".lookupTier", "");
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        PROPERTY_BASE,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get();
    config.getLookupTier();
  }

  @Test
  public void testDatasourceInjection()
  {
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(PROPERTY_BASE + ".lookupTierIsDatasource", "true");
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        PROPERTY_BASE,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get();
    Assert.assertEquals("some_datasource", config.getLookupTier());
  }

  @Test
  public void testLookupsToLoadInjection()
  {
    final LoadSpecHolder taskHolder = new DefaultLoadSpecHolder();
    injector.injectMembers(taskHolder);
    Assert.assertEquals(LookupLoadingSpec.Mode.ALL, taskHolder.getLookupLoadingSpec().getMode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsInjection()
  {
    final String lookupTier = "some_tier";
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(PROPERTY_BASE + ".lookupTier", lookupTier);
    properties.put(PROPERTY_BASE + ".lookupTierIsDatasource", "true");
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        PROPERTY_BASE,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get();
    Assert.assertEquals(lookupTier, config.getLookupTier());
  }

  @Test
  public void testLookupTierDefaultsForNonPeonServers()
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            (Module) binder -> JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                new DruidNode("test-inject", null, false, null, null, true, false)
            ),
            new LookupModule()
        ));
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(PROPERTY_BASE + ".lookupTierIsDatasource", "true");
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        PROPERTY_BASE,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get();
    Assert.assertEquals("__default", config.getLookupTier());
    Assert.assertEquals(LookupLoadingSpec.ALL, config.getLookupLoadingSpec());
  }
}
