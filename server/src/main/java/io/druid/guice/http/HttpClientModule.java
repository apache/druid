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

import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.lifecycle.Lifecycle;

import java.lang.annotation.Annotation;

/**
 */
public class HttpClientModule implements Module
{
  public static HttpClientModule global()
  {
    return new HttpClientModule("druid.global.http", Global.class);
  }

  private final String propertyPrefix;
  private Annotation annotation = null;
  private Class<? extends Annotation> annotationClazz = null;

  public HttpClientModule(String propertyPrefix)
  {
    this.propertyPrefix = propertyPrefix;
  }

  public HttpClientModule(String propertyPrefix, Class<? extends Annotation> annotation)
  {
    this.propertyPrefix = propertyPrefix;
    this.annotationClazz = annotation;
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
            .toProvider(new HttpClientProvider(annotation))
            .in(LazySingleton.class);
    } else if (annotationClazz != null) {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotationClazz);
      binder.bind(HttpClient.class)
            .annotatedWith(annotationClazz)
            .toProvider(new HttpClientProvider(annotationClazz))
            .in(LazySingleton.class);
    } else {
      JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class);
      binder.bind(HttpClient.class)
            .toProvider(new HttpClientProvider())
            .in(LazySingleton.class);
    }
  }

  public static class HttpClientProvider extends AbstractHttpClientProvider<HttpClient>
  {
    public HttpClientProvider()
    {
    }

    public HttpClientProvider(Annotation annotation)
    {
      super(annotation);
    }

    public HttpClientProvider(Class<? extends Annotation> annotationClazz)
    {
      super(annotationClazz);
    }

    @Override
    public HttpClient get()
    {
      final DruidHttpClientConfig config = getConfigProvider().get().get();

      final HttpClientConfig.Builder builder = HttpClientConfig
          .builder()
          .withNumConnections(config.getNumConnections())
          .withReadTimeout(config.getReadTimeout())
          .withWorkerCount(config.getNumMaxThreads());

      if (getSslContextBinding() != null) {
        builder.withSslContext(getSslContextBinding().getProvider().get());
      }
      return HttpClientInit.createClient(builder.build(), LifecycleUtils.asMmxLifecycle(getLifecycleProvider().get()));
    }
  }
}
