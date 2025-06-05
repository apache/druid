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
import com.google.inject.*;

import javax.inject.Named;

import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.server.metrics.MetricsModule;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.net.ssl.SSLContext;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 */
public class JettyHttpClientModule implements Module {
    public static JettyHttpClientModule global() {
        return new JettyHttpClientModule("druid.global.http", Global.class);
    }

    public final String propertyPrefix;
    public final Class<? extends Annotation> annotationClazz;

    public JettyHttpClientModule(String propertyPrefix, Class<? extends Annotation> annotationClazz) {
        this.propertyPrefix = Preconditions.checkNotNull(propertyPrefix, "propertyPrefix");
        this.annotationClazz = Preconditions.checkNotNull(annotationClazz, "annotationClazz");
    }

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, propertyPrefix, DruidHttpClientConfig.class, annotationClazz);
        HttpClientProvider httpClientProvider = new HttpClientProvider(annotationClazz);


        binder.bind(HttpClient.class)
                .annotatedWith(annotationClazz)
                .toProvider(httpClientProvider)
                .in(LazySingleton.class);

        binder.bind(HttpClientProvider.class)
                .annotatedWith(annotationClazz)
                .toInstance(httpClientProvider);
    }


    public static class HttpClientProvider extends AbstractHttpClientProvider<HttpClient> {

        private QueuedThreadPool threadPool;
        private final String annotationName;

        @Inject
        public HttpClientProvider(Class<? extends Annotation> annotation) {
            super(annotation);
            this.annotationName = annotation.getSimpleName();
        }

        public QueuedThreadPool getThreadPool() {
            return threadPool;
        }

        @Override
        public HttpClient get() {
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
            httpClient.setConnectTimeout(config.getClientConnectTimeout());
            httpClient.setRequestBufferSize(config.getRequestBuffersize());
            final QueuedThreadPool pool = new QueuedThreadPool(config.getNumMaxThreads());
//            final QueuedThreadMetricEmittor pool = new QueuedThreadMetricEmittor(config.getNumMaxThreads(), annotationName);
            this.threadPool = pool;
            pool.setName(JettyHttpClientModule.class.getSimpleName() + "-threadPool-" + pool.hashCode());
            httpClient.setExecutor(pool);

            final Lifecycle lifecycle = getLifecycleProvider().get();

            try {
                lifecycle.addMaybeStartHandler(
                        new Lifecycle.Handler() {
                            @Override
                            public void start() {
                            }

                            @Override
                            public void stop() {
                                try {
                                    httpClient.stop();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return httpClient;
        }
    }
}
