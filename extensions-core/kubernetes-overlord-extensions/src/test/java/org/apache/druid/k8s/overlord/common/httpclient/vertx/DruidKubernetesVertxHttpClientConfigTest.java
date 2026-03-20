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

package org.apache.druid.k8s.overlord.common.httpclient.vertx;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class DruidKubernetesVertxHttpClientConfigTest
{
  private static final String PROPERTY_PREFIX = "druid.indexer.runner.k8sAndWorker.http.vertx";

  @Test
  void testSerdeWebClientOptionsMap()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<DruidKubernetesVertxHttpClientConfig> provider = JsonConfigProvider.of(
        PROPERTY_PREFIX,
        DruidKubernetesVertxHttpClientConfig.class
    );
    final Properties properties = new Properties();
    properties.put(PROPERTY_PREFIX + ".webClientOptions.maxPoolSize", "10");
    properties.put(PROPERTY_PREFIX + ".webClientOptions.connectTimeout", "5000");
    properties.put(PROPERTY_PREFIX + ".webClientOptions.idleTimeout", "120");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final DruidKubernetesVertxHttpClientConfig config = provider.get();
    Assertions.assertNotNull(config.getWebClientOptions());
    Assertions.assertEquals(3, config.getWebClientOptions().size());
    Assertions.assertEquals("10", config.getWebClientOptions().get("maxPoolSize"));
    Assertions.assertEquals("5000", config.getWebClientOptions().get("connectTimeout"));
    Assertions.assertEquals("120", config.getWebClientOptions().get("idleTimeout"));
  }

  @Test
  void testSerdeEmptyWebClientOptionsMap()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<DruidKubernetesVertxHttpClientConfig> provider = JsonConfigProvider.of(
        PROPERTY_PREFIX,
        DruidKubernetesVertxHttpClientConfig.class
    );
    final Properties properties = new Properties();
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final DruidKubernetesVertxHttpClientConfig config = provider.get();
    Assertions.assertNotNull(config.getWebClientOptions());
    Assertions.assertEquals(0, config.getWebClientOptions().size());
  }

  private Injector createInjector()
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> JsonConfigProvider.bind(binder, PROPERTY_PREFIX, DruidKubernetesVertxHttpClientConfig.class)
        )
    );
  }
}
