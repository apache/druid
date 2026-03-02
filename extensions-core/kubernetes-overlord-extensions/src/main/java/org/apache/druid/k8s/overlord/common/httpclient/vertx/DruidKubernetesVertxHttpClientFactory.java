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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.vertx.VertxHttpClientFactory;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;
import io.vertx.core.spi.resolver.ResolverProvider;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.k8s.overlord.common.httpclient.DruidKubernetesHttpClientFactory;

/**
 * Similar to {@link VertxHttpClientFactory} but allows us to override thread pool configurations.
 */
public class DruidKubernetesVertxHttpClientFactory extends VertxHttpClientFactory implements DruidKubernetesHttpClientFactory
{
  public static final String TYPE_NAME = "vertx";

  private static final Logger LOG = new Logger(DruidKubernetesVertxHttpClientFactory.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final DruidKubernetesVertxHttpClientConfig httpClientConfig;

  public DruidKubernetesVertxHttpClientFactory(Vertx vertx, DruidKubernetesVertxHttpClientConfig httpClientConfig)
  {
    super(vertx);
    this.httpClientConfig = httpClientConfig;
  }

  @Override
  protected void additionalConfig(WebClientOptions options)
  {
    if (!httpClientConfig.getWebClientOptions().isEmpty()) {
      try {
        LOG.info("Applying additional WebClientOptions from configuration: %s", httpClientConfig.getWebClientOptions());
        OBJECT_MAPPER.updateValue(options, httpClientConfig.getWebClientOptions());
      }
      catch (Exception e) {
        throw new RuntimeException(
            "Failed to apply webClientOptions to WebClientOptions. "
            + "Check that all property names and values are valid. "
            + "Properties provided: " + httpClientConfig.getWebClientOptions(),
            e
        );
      }
    }
  }

  /**
   * Adapted from fabric8 kubernetes-client 7.1.0. We bring this here so we can customize thread pool sizes
   * and force usage of daemon threads.
   */
  public static Vertx createVertxInstance(final DruidKubernetesVertxHttpClientConfig httpClientConfig)
  {
    // fabric8 disables the async DNS resolver while creating Vertx.
    // I'm not sure if we really need to do this, but I'm keeping it to align behavior with upstream.
    final String originalDnsResolverProperty = System.getProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME);
    Vertx vertx;
    try {
      System.setProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME, "true");
      vertx = Vertx.vertx(
          new VertxOptions()
              .setFileSystemOptions(
                  new FileSystemOptions().setFileCachingEnabled(false)
                                         .setClassPathResolvingEnabled(false)
              )
              .setWorkerPoolSize(httpClientConfig.getWorkerPoolSize())
              .setEventLoopPoolSize(httpClientConfig.getEventLoopPoolSize())
              .setInternalBlockingPoolSize(httpClientConfig.getInternalBlockingPoolSize())
              .setUseDaemonThread(true)
      );
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
}
