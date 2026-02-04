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

package org.apache.druid.k8s.overlord.common.httpclient.vertx;

import io.fabric8.kubernetes.client.vertx.VertxHttpClientBuilder;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;
import io.vertx.core.spi.resolver.ResolverProvider;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.overlord.common.DruidKubernetesHttpClientFactory;

/**
 * Creates a Vertx-backed HTTP client for the Kubernetes client.
 *
 * Vertx uses non-blocking I/O with event loops, which is more efficient than
 * thread-per-request models (OkHttp) when handling many concurrent K8s API calls.
 *
 * Backported from Druid 35 PR #18540 with enhanced logging.
 */
public class DruidKubernetesVertxHttpClientFactory implements DruidKubernetesHttpClientFactory
{
  private static final Logger log = new Logger(DruidKubernetesVertxHttpClientFactory.class);

  private final Vertx vertx;
  private final DruidKubernetesVertxHttpClientConfig config;

  public DruidKubernetesVertxHttpClientFactory(final DruidKubernetesVertxHttpClientConfig httpClientConfig)
  {
    this.config = httpClientConfig;
    log.info("Initializing Vertx HTTP client factory with config: %s", httpClientConfig);
    this.vertx = createVertxInstance(httpClientConfig);

    // Log actual pool sizes after initialization
    log.info(
        "Vertx HTTP client initialized - workerPoolSize=[%d], eventLoopPoolSize=[%d], internalBlockingPoolSize=[%d]",
        httpClientConfig.getWorkerPoolSize(),
        httpClientConfig.getEventLoopPoolSize() > 0
            ? httpClientConfig.getEventLoopPoolSize()
            : Runtime.getRuntime().availableProcessors() * 2,
        httpClientConfig.getInternalBlockingPoolSize()
    );
  }

  @Override
  public VertxHttpClientBuilder<DruidKubernetesVertxHttpClientFactory> newBuilder()
  {
    log.debug("Creating new Vertx HTTP client builder");
    return new VertxHttpClientBuilder<>(this, vertx);
  }

  /**
   * Creates a Vertx instance with custom thread pool configuration.
   * Adapted from fabric8 kubernetes-client.
   */
  private static Vertx createVertxInstance(final DruidKubernetesVertxHttpClientConfig httpClientConfig)
  {
    // Fabric8 disables the async DNS resolver while creating Vertx.
    // Keeping this behavior to align with upstream.
    final String originalDnsResolverProperty = System.getProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME);

    Vertx vertx;
    try {
      System.setProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME, "true");

      VertxOptions options = new VertxOptions()
          .setFileSystemOptions(
              new FileSystemOptions()
                  .setFileCachingEnabled(false)
                  .setClassPathResolvingEnabled(false)
          )
          .setWorkerPoolSize(httpClientConfig.getWorkerPoolSize())
          .setInternalBlockingPoolSize(httpClientConfig.getInternalBlockingPoolSize())
          .setUseDaemonThread(true);  // Don't prevent JVM shutdown

      // Only set event loop pool size if explicitly configured (0 = use Vertx default)
      if (httpClientConfig.getEventLoopPoolSize() > 0) {
        options.setEventLoopPoolSize(httpClientConfig.getEventLoopPoolSize());
      }

      vertx = Vertx.vertx(options);

      log.info("Vertx instance created successfully with daemon threads");
    }
    finally {
      if (originalDnsResolverProperty == null) {
        System.clearProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME);
      } else {
        System.setProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME, originalDnsResolverProperty);
      }
    }

    return vertx;
  }

  public void close()
  {
    log.info("Closing Vertx HTTP client factory");
    if (vertx != null) {
      vertx.close();
      log.info("Vertx instance closed");
    }
  }
}
