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

import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Global;
import io.druid.java.util.common.lifecycle.Lifecycle;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import java.lang.annotation.Annotation;

/**
 */
public class JettyHttpClientModule implements Module
{
  public static JettyHttpClientModule global()
  {
    return new JettyHttpClientModule("druid.global.http", Global.class);
  }

  private final String propertyPrefix;
  private Annotation annotation = null;
  private Class<? extends Annotation> annotationClazz = null;

  public JettyHttpClientModule(String propertyPrefix)
  {
    this.propertyPrefix = propertyPrefix;
  }

  public JettyHttpClientModule(String propertyPrefix, Class<? extends Annotation> annotation)
  {
    this.propertyPrefix = propertyPrefix;
    this.annotationClazz = annotation;
  }

  public JettyHttpClientModule(String propertyPrefix, Annotation annotation)
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

    public HttpClientProvider(Class<? extends Annotation> annotation)
    {
      super(annotation);
    }

    @Override
    public HttpClient get()
    {
      final DruidHttpClientConfig config = getConfigProvider().get().get();

      final HttpClient httpClient;
      if (getSslContextBinding() != null) {
        final SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setSslContext(getSslContextBinding().getProvider().get());
        httpClient = new HttpClient(sslContextFactory);
      } else {
        httpClient = new HttpClient();
      }

      httpClient.setIdleTimeout(config.getReadTimeout().getMillis());
      httpClient.setMaxConnectionsPerDestination(config.getNumConnections());
      final QueuedThreadPool pool = new QueuedThreadPool(config.getNumMaxThreads());
      pool.setName(JettyHttpClientModule.class.getSimpleName() + "-threadPool-" + pool.hashCode());
      httpClient.setExecutor(pool);

      final Lifecycle lifecycle = getLifecycleProvider().get();

      try {
        lifecycle.addMaybeStartHandler(
            new Lifecycle.Handler()
            {
              @Override
              public void start() throws Exception
              {
              }

              @Override
              public void stop()
              {
                try {
                  httpClient.stop();
                }
                catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      return httpClient;
    }
  }
}
