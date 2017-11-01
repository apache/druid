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

package io.druid.server.emitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.ParametrizedUriEmitter;
import com.metamx.emitter.core.ParametrizedUriEmitterConfig;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

public class ParametrizedUriEmitterModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.parametrized", ParametrizedUriEmitterConfig.class);
    HttpEmitterModule.configureSsl(binder);
  }

  @Provides
  @ManageLifecycle
  @Named("parametrized")
  public Emitter getEmitter(
      Supplier<ParametrizedUriEmitterConfig> config,
      @Nullable SSLContext sslContext,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper
  )
  {
    final DefaultAsyncHttpClientConfig.Builder builder = new DefaultAsyncHttpClientConfig.Builder();
    if (sslContext != null) {
      builder.setSslContext(new JdkSslContext(sslContext, true, ClientAuth.NONE));
    }
    final AsyncHttpClient client = new DefaultAsyncHttpClient(builder.build());
    lifecycle.addCloseableInstance(client);

    return new ParametrizedUriEmitter(config.get(), client, jsonMapper);
  }
}
