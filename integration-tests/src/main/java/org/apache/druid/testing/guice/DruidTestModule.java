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

package org.apache.druid.testing.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.core.LoggingEmitterConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.IntegrationTestingConfigProvider;
import org.apache.druid.testing.IntegrationTestingCuratorConfig;

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

    // Bind DruidNode instance to make Guice happy. This instance is currently unused.
    binder.bind(DruidNode.class).annotatedWith(Self.class).toInstance(
        new DruidNode("integration-tests", "localhost", 9191, null, null, true, false)
    );
  }

  @Provides
  @TestClient
  public HttpClient getHttpClient(
      IntegrationTestingConfig config,
      Lifecycle lifecycle,
      @EscalatedClient HttpClient delegate
  )
  {
    if (config.getUsername() != null) {
      return new CredentialedHttpClient(new BasicCredentials(config.getUsername(), config.getPassword()), delegate);
    } else {
      return new CredentialedHttpClient(new BasicCredentials("admin", "priest"), delegate);
    }
  }

  @Provides
  @ManageLifecycle
  public ServiceEmitter getServiceEmitter(Supplier<LoggingEmitterConfig> config, ObjectMapper jsonMapper)
  {
    return new ServiceEmitter("", "", new LoggingEmitter(config.get(), jsonMapper));
  }
}
