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

package org.apache.druid.segment.metadata;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Pathetic little unit test just to keep Jacoco happy.
 */
public class CoordinatorSegmentMetadataCacheConfigTest
{
  private static final String CONFIG_BASE = "druid.coordinator.segmentMetadataCache";

  @Test
  public void testDefaultConfig()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<SegmentMetadataCacheConfig> provider = JsonConfigProvider.of(
        CONFIG_BASE,
        SegmentMetadataCacheConfig.class
    );

    final Properties properties = new Properties();
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final SegmentMetadataCacheConfig config = provider.get();
    Assert.assertFalse(config.isAwaitInitializationOnStart());
    Assert.assertEquals(Period.minutes(1), config.getMetadataRefreshPeriod());
    Assert.assertEquals(new AbstractSegmentMetadataCache.LeastRestrictiveTypeMergePolicy(), config.getMetadataColumnTypeMergePolicy());
  }

  @Test
  public void testCustomizedConfig()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<SegmentMetadataCacheConfig> provider = JsonConfigProvider.of(
        CONFIG_BASE,
        SegmentMetadataCacheConfig.class
    );
    final Properties properties = new Properties();
    properties.setProperty(
        CONFIG_BASE + ".metadataColumnTypeMergePolicy",
        "latestInterval"
    );
    properties.setProperty(CONFIG_BASE + ".metadataRefreshPeriod", "PT2M");
    properties.setProperty(CONFIG_BASE + ".awaitInitializationOnStart", "false");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final SegmentMetadataCacheConfig config = provider.get();
    Assert.assertFalse(config.isAwaitInitializationOnStart());
    Assert.assertEquals(Period.minutes(2), config.getMetadataRefreshPeriod());
    Assert.assertEquals(
        new AbstractSegmentMetadataCache.FirstTypeMergePolicy(),
        config.getMetadataColumnTypeMergePolicy()
    );
  }

  private Injector createInjector()
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> {
              JsonConfigProvider.bind(binder, CONFIG_BASE, SegmentMetadataCacheConfig.class);
            }
        )
    );
  }
}
