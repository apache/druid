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

package io.druid.guice.http;

import com.google.common.base.Supplier;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.metamx.common.lifecycle.Lifecycle;

import javax.net.ssl.SSLContext;
import java.lang.annotation.Annotation;

/**
 */
public abstract class AbstractHttpClientProvider<HttpClientType> implements Provider<HttpClientType>
{
  private final Key<Supplier<DruidHttpClientConfig>> configKey;
  private final Key<SSLContext> sslContextKey;

  private Provider<Supplier<DruidHttpClientConfig>> configProvider;
  private Provider<Lifecycle> lifecycleProvider;
  private Binding<SSLContext> sslContextBinding;

  public AbstractHttpClientProvider()
  {
    configKey = Key.get(
        new TypeLiteral<Supplier<DruidHttpClientConfig>>()
        {
        }
    );
    sslContextKey = Key.get(SSLContext.class);
  }

  public AbstractHttpClientProvider(Annotation annotation)
  {
    configKey = Key.get(
        new TypeLiteral<Supplier<DruidHttpClientConfig>>()
        {
        }, annotation
    );
    sslContextKey = Key.get(SSLContext.class, annotation);
  }

  public AbstractHttpClientProvider(Class<? extends Annotation> annotation)
  {
    configKey = Key.get(
        new TypeLiteral<Supplier<DruidHttpClientConfig>>()
        {
        }, annotation
    );
    sslContextKey = Key.get(SSLContext.class, annotation);
  }

  @Inject
  public void configure(Injector injector)
  {
    configProvider = injector.getProvider(configKey);
    sslContextBinding = injector.getExistingBinding(sslContextKey);
    lifecycleProvider = injector.getProvider(Lifecycle.class);
  }

  public Key<Supplier<DruidHttpClientConfig>> getConfigKey()
  {
    return configKey;
  }

  public Key<SSLContext> getSslContextKey()
  {
    return sslContextKey;
  }

  public Provider<Supplier<DruidHttpClientConfig>> getConfigProvider()
  {
    return configProvider;
  }

  public Provider<Lifecycle> getLifecycleProvider()
  {
    return lifecycleProvider;
  }

  public Binding<SSLContext> getSslContextBinding()
  {
    return sslContextBinding;
  }
}
