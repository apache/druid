/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.util.Providers;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.HttpPostEmitter;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;

/**
 */
public class HttpEmitterModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.http", HttpEmitterConfig.class);

    final SSLContext context;
    try {
      context = SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }

    binder.bind(SSLContext.class).toProvider(Providers.of(context)).in(LazySingleton.class);
  }

  @Provides
  @ManageLifecycle
  @Named("http")
  public Emitter getEmitter(
      Supplier<HttpEmitterConfig> config,
      @Nullable SSLContext sslContext,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper
  )
  {
    final HttpClientConfig.Builder builder = HttpClientConfig
        .builder()
        .withNumConnections(1)
        .withReadTimeout(config.get().getReadTimeout().toStandardDuration());

    if (sslContext != null) {
      builder.withSslContext(sslContext);
    }

    return new HttpPostEmitter(config.get(), HttpClientInit.createClient(builder.build(), lifecycle), jsonMapper);
  }
}
