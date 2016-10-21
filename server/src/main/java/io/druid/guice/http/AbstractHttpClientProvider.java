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

package io.druid.guice.http;

import com.google.common.base.Supplier;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;

import io.druid.java.util.common.lifecycle.Lifecycle;

import javax.net.ssl.SSLContext;
import java.lang.annotation.Annotation;

/**
 */
public abstract class AbstractHttpClientProvider<HttpClientType> implements Provider<HttpClientType>
{
  private final Key<Supplier<DruidHttpClientConfig>> configKey;
  private final Key<SSLContext> sslContextKey;

  private Injector injector;

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
    this.injector = injector;
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
    return injector.getProvider(configKey);
  }

  public Provider<Lifecycle> getLifecycleProvider()
  {
    return injector.getProvider(Lifecycle.class);
  }

  public Binding<SSLContext> getSslContextBinding()
  {
    return injector.getExistingBinding(sslContextKey);
  }
}
