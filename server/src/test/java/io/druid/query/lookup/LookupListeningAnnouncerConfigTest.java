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

package io.druid.query.lookup;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.metrics.DataSourceTaskIdHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class LookupListeningAnnouncerConfigTest
{
  private static final String propertyBase = "some.property";
  private final Injector injector = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(),
      ImmutableList.of(
          new Module()
          {
            @Override
            public void configure(Binder binder)
            {
              JsonConfigProvider.bindInstance(
                  binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null)
              );
              binder.bind(Key.get(
                  String.class,
                  Names.named(DataSourceTaskIdHolder.DATA_SOURCE_BINDING)
              )).toInstance("some_datasource");
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
        propertyBase,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get().get();
    Assert.assertEquals(LookupListeningAnnouncerConfig.DEFAULT_TIER, config.getLookupTier());
  }

  @Test
  public void testSimpleInjection()
  {
    final String lookupTier = "some_tier";
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(propertyBase + ".lookupTier", lookupTier);
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        propertyBase,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get().get();
    Assert.assertEquals(lookupTier, config.getLookupTier());
  }

  @Test(expected = NullPointerException.class)
  public void testFailsOnEmptyTier()
  {
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(propertyBase + ".lookupTier", "");
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        propertyBase,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get().get();
    config.getLookupTier();
  }

  @Test
  public void testDatasourceInjection()
  {
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(propertyBase + ".lookupTierIsDatasource", "true");
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        propertyBase,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get().get();
    Assert.assertEquals("some_datasource", config.getLookupTier());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailsInjection()
  {
    final String lookupTier = "some_tier";
    final JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    properties.put(propertyBase + ".lookupTier", lookupTier);
    properties.put(propertyBase + ".lookupTierIsDatasource", "true");
    final JsonConfigProvider<LookupListeningAnnouncerConfig> configProvider = JsonConfigProvider.of(
        propertyBase,
        LookupListeningAnnouncerConfig.class
    );
    configProvider.inject(properties, configurator);
    final LookupListeningAnnouncerConfig config = configProvider.get().get();
    Assert.assertEquals(lookupTier, config.getLookupTier());
  }
}
