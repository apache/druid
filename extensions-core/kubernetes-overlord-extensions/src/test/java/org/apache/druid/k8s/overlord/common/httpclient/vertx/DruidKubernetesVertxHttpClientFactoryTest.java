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
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class DruidKubernetesVertxHttpClientFactoryTest
{
  private static final String PROPERTY_PREFIX = "druid.indexer.runner.k8sAndWorker.http.vertx";

  private Vertx vertx;

  @Before
  public void setUp()
  {
    vertx = DruidKubernetesVertxHttpClientFactory.createVertxInstance(new DruidKubernetesVertxHttpClientConfig());
  }

  @After
  public void tearDown()
  {
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test
  public void testAdditionalConfigAppliesWebClientOptions()
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

    DruidKubernetesVertxHttpClientFactory factory = new DruidKubernetesVertxHttpClientFactory(vertx, config);

    WebClientOptions options = new WebClientOptions();
    factory.additionalConfig(options);

    Assert.assertEquals(10, options.getMaxPoolSize());
    Assert.assertEquals(5000, options.getConnectTimeout());
    Assert.assertEquals(120, options.getIdleTimeout());
  }

  @Test
  public void testAdditionalConfigWithEmptyMapDoesNotModifyOptions()
  {
    DruidKubernetesVertxHttpClientConfig config = new DruidKubernetesVertxHttpClientConfig();
    DruidKubernetesVertxHttpClientFactory factory = new DruidKubernetesVertxHttpClientFactory(vertx, config);

    WebClientOptions options = new WebClientOptions();
    int defaultMaxPoolSize = options.getMaxPoolSize();
    int defaultConnectTimeout = options.getConnectTimeout();

    factory.additionalConfig(options);

    Assert.assertEquals(defaultMaxPoolSize, options.getMaxPoolSize());
    Assert.assertEquals(defaultConnectTimeout, options.getConnectTimeout());
  }

  @Test
  public void testAdditionalConfigWithInvalidPropertyThrowsException()
  {
    final Injector injector = createInjector();
    final JsonConfigProvider<DruidKubernetesVertxHttpClientConfig> provider = JsonConfigProvider.of(
        PROPERTY_PREFIX,
        DruidKubernetesVertxHttpClientConfig.class
    );
    final Properties properties = new Properties();
    properties.put(PROPERTY_PREFIX + ".webClientOptions.nonExistentProperty", "value");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final DruidKubernetesVertxHttpClientConfig config = provider.get();

    DruidKubernetesVertxHttpClientFactory factory = new DruidKubernetesVertxHttpClientFactory(vertx, config);

    WebClientOptions options = new WebClientOptions();
    RuntimeException exception = Assert.assertThrows(RuntimeException.class, () -> factory.additionalConfig(options));
    Assert.assertTrue(
        exception.getMessage(),
        exception.getMessage().contains("Failed to apply webClientOptions")
    );
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
