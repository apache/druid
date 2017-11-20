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

import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.EscalatedClient;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.StringUtils;
import io.druid.server.security.Escalator;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 */
public class HttpClientModule implements Module
{
  public static HttpClientModule global()
  {
    return new HttpClientModule("druid.global.http", Global.class);
  }

  public static HttpClientModule escalatedGlobal()
  {
    return new HttpClientModule("druid.global.http", EscalatedGlobal.class);
  }

  private static Set<Class<? extends Annotation>> ESCALATING_ANNOTATIONS = Sets.newHashSet(
      EscalatedGlobal.class, EscalatedClient.class
  );

  private final String propertyPrefix;
  private Annotation annotation = null;
  private Class<? extends Annotation> annotationClazz = null;
  private boolean isEscalated = false;

  public HttpClientModule(String propertyPrefix)
  {
    this.propertyPrefix = propertyPrefix;
  }

  public HttpClientModule(String propertyPrefix, Class<? extends Annotation> annotation)
  {
    this.propertyPrefix = propertyPrefix;
    this.annotationClazz = annotation;

    isEscalated = ESCALATING_ANNOTATIONS.contains(annotationClazz);
  }

  public HttpClientModule(String propertyPrefix, Annotation annotation)
  {
    this.propertyPrefix = propertyPrefix;
    this.annotation = annotation;
  }

  @Override
  public void configure(Binder binder)
  {
    if (annotation != null) {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotation);
      binder.bind(HttpClient.class)
            .annotatedWith(annotation)
            .toProvider(new HttpClientProvider(annotation, isEscalated))
            .in(LazySingleton.class);
    } else if (annotationClazz != null) {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotationClazz);
      binder.bind(HttpClient.class)
            .annotatedWith(annotationClazz)
            .toProvider(new HttpClientProvider(annotationClazz, isEscalated))
            .in(LazySingleton.class);
    } else {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class);
      binder.bind(HttpClient.class)
            .toProvider(new HttpClientProvider(isEscalated))
            .in(LazySingleton.class);
    }
  }

  public static class HttpClientProvider extends AbstractHttpClientProvider<HttpClient>
  {
    private boolean isEscalated;
    private Escalator escalator;

    public HttpClientProvider(boolean isEscalated)
    {
      this.isEscalated = isEscalated;
    }

    public HttpClientProvider(Annotation annotation, boolean isEscalated)
    {
      super(annotation);
      this.isEscalated = isEscalated;
    }

    public HttpClientProvider(Class<? extends Annotation> annotationClazz, boolean isEscalated)
    {
      super(annotationClazz);
      this.isEscalated = isEscalated;
    }

    @Inject
    public void inject(Escalator escalator)
    {
      this.escalator = escalator;
    }

    @Override
    public HttpClient get()
    {
      final DruidHttpClientConfig config = getConfigProvider().get().get();

      final HttpClientConfig.Builder builder = HttpClientConfig
          .builder()
          .withNumConnections(config.getNumConnections())
          .withReadTimeout(config.getReadTimeout())
          .withWorkerCount(config.getNumMaxThreads())
          .withCompressionCodec(
              HttpClientConfig.CompressionCodec.valueOf(StringUtils.toUpperCase(config.getCompressionCodec()))
          );

      if (getSslContextBinding() != null) {
        builder.withSslContext(getSslContextBinding().getProvider().get());
      }

      HttpClient client = HttpClientInit.createClient(
          builder.build(),
          LifecycleUtils.asMmxLifecycle(getLifecycleProvider().get())
      );

      if (isEscalated) {
        return escalator.createEscalatedClient(client);
      } else {
        return client;
      }
    }
  }
}
