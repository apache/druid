/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.initialization;

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.guice.JsonConfigProvider;
import com.metamx.druid.guice.LazySingleton;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.HttpPostEmitter;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

/**
 */
public class HttpEmitterModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.http", HttpEmitterConfig.class);
  }

  @Provides @LazySingleton @Named("http")
  public Emitter getEmitter(Supplier<HttpEmitterConfig> config, @Nullable SSLContext sslContext, Lifecycle lifecycle)
  {
    final HttpClientConfig.Builder builder = HttpClientConfig
        .builder()
        .withNumConnections(1)
        .withReadTimeout(config.get().getReadTimeout());

    if (sslContext != null) {
      builder.withSslContext(sslContext);
    }

    return new HttpPostEmitter(config.get(), HttpClientInit.createClient(builder.build(), lifecycle));
  }
}
