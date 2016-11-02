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

package io.druid.testing.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.core.LoggingEmitterConfig;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.http.client.CredentialedHttpClient;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.auth.BasicCredentials;
import io.druid.curator.CuratorConfig;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.IntegrationTestingConfigProvider;
import io.druid.testing.IntegrationTestingCuratorConfig;

import javax.net.ssl.SSLContext;

/**
 */
public class DruidTestModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(IntegrationTestingConfig.class)
          .toProvider(IntegrationTestingConfigProvider.class)
          .in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.test.config", IntegrationTestingConfigProvider.class);

    binder.bind(CuratorConfig.class).to(IntegrationTestingCuratorConfig.class);
  }

  @Provides
  @TestClient
  public HttpClient getHttpClient(
    IntegrationTestingConfig config,
    DruidHttpClientConfig httpClientConfig,
    Lifecycle lifecycle
  )
    throws Exception
  {

    final HttpClientConfig.Builder builder = HttpClientConfig
      .builder()
      .withNumConnections(httpClientConfig.getNumConnections())
      .withReadTimeout(httpClientConfig.getReadTimeout())
      .withWorkerCount(httpClientConfig.getNumMaxThreads());

    builder.withSslContext(SSLContext.getDefault());
    HttpClient delegate = HttpClientInit.createClient(builder.build(), lifecycle);
    if (config.getUsername() != null) {
      return new CredentialedHttpClient(new BasicCredentials(config.getUsername(), config.getPassword()), delegate);
    }
    return delegate;
  }

  @Provides
  @LazySingleton
  public ServiceEmitter getServiceEmitter(Supplier<LoggingEmitterConfig> config, ObjectMapper jsonMapper)
  {
    return new ServiceEmitter("", "", new LoggingEmitter(config.get(), jsonMapper));
  }
}
