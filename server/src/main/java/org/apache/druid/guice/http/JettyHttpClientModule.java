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

package org.apache.druid.guice.http;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Module;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.net.ssl.SSLContext;
import java.lang.annotation.Annotation;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class JettyHttpClientModule implements Module
{
  private static final long CLIENT_CONNECT_TIMEOUT_MILLIS = TimeUnit.MILLISECONDS.toMillis(500);

  public static JettyHttpClientModule global()
  {
    return new JettyHttpClientModule("druid.global.http", Global.class);
  }

  private final String propertyPrefix;
  private final Class<? extends Annotation> annotationClazz;

  public JettyHttpClientModule(String propertyPrefix, Class<? extends Annotation> annotationClazz)
  {
    this.propertyPrefix = Preconditions.checkNotNull(propertyPrefix, "propertyPrefix");
    this.annotationClazz = Preconditions.checkNotNull(annotationClazz, "annotationClazz");
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotationClazz);
    binder.bind(HttpClient.class)
          .annotatedWith(annotationClazz)
          .toProvider(new HttpClientProvider(annotationClazz))
          .in(LazySingleton.class);
  }

  public static class HttpClientProvider extends AbstractHttpClientProvider<HttpClient>
  {
    public HttpClientProvider(Class<? extends Annotation> annotation)
    {
      super(annotation);
    }

    @Override
    public HttpClient get()
    {
      final DruidHttpClientConfig config = getConfigProvider().get().get();

      final HttpClient httpClient;
      final Binding<SSLContext> sslContextBinding = getSslContextBinding();
      if (sslContextBinding != null) {
        final SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
        sslContextFactory.setSslContext(sslContextBinding.getProvider().get());
        httpClient = new HttpClient(sslContextFactory);
      } else {
        httpClient = new HttpClient();
      }

      httpClient.setIdleTimeout(config.getReadTimeout().getMillis());
      httpClient.setMaxConnectionsPerDestination(config.getNumConnections());
      httpClient.setMaxRequestsQueuedPerDestination(config.getNumRequestsQueued());
      httpClient.setConnectTimeout(CLIENT_CONNECT_TIMEOUT_MILLIS);
      httpClient.setRequestBufferSize(config.getRequestBuffersize());
      final QueuedThreadPool pool = new QueuedThreadPool(config.getNumMaxThreads());
      pool.setName(JettyHttpClientModule.class.getSimpleName() + "-threadPool-" + pool.hashCode());
      httpClient.setExecutor(pool);

      final Lifecycle lifecycle = getLifecycleProvider().get();

      try {
        lifecycle.addMaybeStartHandler(
            new Lifecycle.Handler()
            {
              @Override
              public void start()
              {
              }

              @Override
              public void stop()
              {
                try {
                  httpClient.stop();
                }
                catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return httpClient;
    }
  }
}
