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

package org.apache.druid.query;

import com.google.inject.Injector;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.annotations.Global;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class DefaultQueryConfigTest
{
  @Test
  public void testSerdeContextMap()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.default";
    final JsonConfigProvider<DefaultQueryConfig> provider = JsonConfigProvider.of(
        propertyPrefix,
        DefaultQueryConfig.class
    );
    final Properties properties = new Properties();
    properties.put(propertyPrefix + ".context.joinFilterRewriteMaxSize", "10");
    properties.put(propertyPrefix + ".context.vectorize", "true");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final DefaultQueryConfig defaultQueryConfig = provider.get().get();
    Assert.assertNotNull(defaultQueryConfig.getContext());
    Assert.assertEquals(2, defaultQueryConfig.getContext().size());
    Assert.assertEquals("10", defaultQueryConfig.getContext().get("joinFilterRewriteMaxSize"));
    Assert.assertEquals("true", defaultQueryConfig.getContext().get("vectorize"));
  }

  @Test
  public void testSerdeEmptyContextMap()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.query.default";
    final JsonConfigProvider<DefaultQueryConfig> provider = JsonConfigProvider.of(
        propertyPrefix,
        DefaultQueryConfig.class
    );
    final Properties properties = new Properties();
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final DefaultQueryConfig defaultQueryConfig = provider.get().get();
    Assert.assertNotNull(defaultQueryConfig.getContext());
    Assert.assertEquals(0, defaultQueryConfig.getContext().size());
  }

  private Injector createInjector()
  {
    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> {
              JsonConfigProvider.bind(binder, "druid.query.default", DefaultQueryConfig.class, Global.class);
            }
        )
    );
    return injector;
  }
}
