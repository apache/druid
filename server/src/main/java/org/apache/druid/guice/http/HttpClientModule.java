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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Module;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.AbstractHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.NettyHttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.HttpClientPoolRegistry;
import org.apache.druid.server.security.Escalator;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.joda.time.Duration;

import javax.net.ssl.SSLContext;
import java.lang.annotation.Annotation;
import java.util.Set;

/**
 *
 */
public class HttpClientModule implements Module
{
  public static HttpClientModule global()
  {
    return new HttpClientModule("druid.global.http", Global.class, false);
  }

  public static HttpClientModule escalatedGlobal()
  {
    return new HttpClientModule("druid.global.http", EscalatedGlobal.class, false);
  }

  private static final Set<Class<? extends Annotation>> ESCALATING_ANNOTATIONS =
      ImmutableSet.of(EscalatedGlobal.class, EscalatedClient.class);

  private final String propertyPrefix;
  private final Class<? extends Annotation> annotationClazz;
  private final boolean isEscalated;
  private final boolean eagerByDefault;

  public HttpClientModule(String propertyPrefix, Class<? extends Annotation> annotationClazz, boolean eagerByDefault)
  {
    this.propertyPrefix = Preconditions.checkNotNull(propertyPrefix, "propertyPrefix");
    this.annotationClazz = Preconditions.checkNotNull(annotationClazz, "annotationClazz");
    this.eagerByDefault = eagerByDefault;

    isEscalated = ESCALATING_ANNOTATIONS.contains(this.annotationClazz);
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotationClazz);
    binder.bind(HttpClient.class)
          .annotatedWith(annotationClazz)
          .toProvider(new HttpClientProvider(annotationClazz, isEscalated, eagerByDefault))
          .in(LazySingleton.class);
  }

  public static class HttpClientProvider extends AbstractHttpClientProvider<HttpClient>
  {
    private final Class<? extends Annotation> annotationClazz;
    private final boolean isEscalated;
    private final boolean eagerByDefault;
    private Escalator escalator;
    private DruidNode node;
    private HttpClientPoolRegistry poolRegistry;

    public HttpClientProvider(Class<? extends Annotation> annotationClazz, boolean isEscalated, boolean eagerByDefault)
    {
      super(annotationClazz);
      this.annotationClazz = annotationClazz;
      this.isEscalated = isEscalated;
      this.eagerByDefault = eagerByDefault;
    }

    @Inject
    public void inject(Escalator escalator, @Self DruidNode node, HttpClientPoolRegistry poolRegistry)
    {
      this.escalator = escalator;
      this.node = node;
      this.poolRegistry = poolRegistry;
    }

    @Override
    public HttpClient get()
    {
      final DruidHttpClientConfig config = getConfigProvider().get().get();

      final HttpClientConfig.Builder builder = HttpClientConfig
          .builder()
          .withNumConnections(config.getNumConnections())
          .withEagerInitialization(config.isEagerInitialization(eagerByDefault))
          .withPoolImplementation(config.getPoolImplementation())
          .withStrictConnectionValidation(config.isStrictConnectionValidation())
          .withReadTimeout(config.getReadTimeout())
          .withWorkerCount(config.getNumMaxThreads())
          .withCompressionCodec(
              HttpClientConfig.CompressionCodec.valueOf(StringUtils.toUpperCase(config.getCompressionCodec()))
          )
          .withUnusedConnectionTimeoutDuration(config.getUnusedConnectionTimeout());

      final Binding<SSLContext> sslContextBinding = getSslContextBinding();

      if (sslContextBinding != null) {
        builder.withSslContext(sslContextBinding.getProvider().get());
      }

      NettyHttpClient client = HttpClientInit.createNettyClient(
          builder.build(),
          getLifecycleProvider().get()
      );
      poolRegistry.register(clientName(), client.getPool());
      HttpClient clientWithUserAgent = new AbstractHttpClient()
      {
        @Override
        public <Intermediate, Final> ListenableFuture<Final> go(
            Request request,
            HttpResponseHandler<Intermediate, Final> handler,
            Duration readTimeout
        )
        {
          request.setHeader(HttpHeaders.Names.USER_AGENT, StringUtils.format("%s/%s", node.getServiceName(), node.getVersion()));
          return client.go(request, handler, readTimeout);
        }
      };

      if (isEscalated) {
        return escalator.createEscalatedClient(clientWithUserAgent);
      } else {
        return clientWithUserAgent;
      }
    }

    /**
     * The binding annotation of this client, which is what tells its pool apart from the pools of the other clients
     * of the same process - {@code druid.global.http} backs two of them.
     */
    private String clientName()
    {
      final String simpleName = annotationClazz.getSimpleName();
      return StringUtils.toLowerCase(simpleName.substring(0, 1)) + simpleName.substring(1);
    }
  }
}
