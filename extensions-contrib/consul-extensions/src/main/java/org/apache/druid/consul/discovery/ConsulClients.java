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

package org.apache.druid.consul.discovery;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.security.TLSUtils;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * Helper for constructing a ConsulClient with TLS support.
 * <p>
 * This class properly configures HTTPS transport for the Ecwid Consul client
 * using Druid's standard TLS infrastructure.
 */
final class ConsulClients
{
  private static final Logger LOGGER = new Logger(ConsulClients.class);

  private ConsulClients()
  {
  }

  static ConsulClient create(ConsulDiscoveryConfig config)
  {
    ConsulDiscoveryConfig.ConnectionConfig connection = config.getConnection();
    ConsulDiscoveryConfig.AuthConfig auth = config.getAuth();

    ConsulSSLConfig sslConfig = connection.getSslClientConfig();
    String basicUser = auth.getBasicAuthUser();
    String basicPass = auth.getBasicAuthPassword();
    boolean tlsConfigured = sslConfig != null && sslConfig.getTrustStorePath() != null;

    // Validate basic auth over HTTP security requirements
    if (basicUser != null && basicPass != null && !tlsConfigured) {
      if (!auth.getAllowBasicAuthOverHttp()) {
        throw new IllegalStateException(
            "Basic authentication credentials are configured but TLS is not enabled. " +
            "This would transmit credentials in cleartext over the network. " +
            "Either configure TLS (connection.sslClientConfig.trustStorePath) or explicitly allow " +
            "insecure transmission by setting auth.allowBasicAuthOverHttp=true " +
            "(only use this for sidecar TLS termination scenarios)."
        );
      }
      LOGGER.warn(
          "Using Basic Auth to Consul over plain HTTP (host: %s, port: %d) with allowBasicAuthOverHttp=true. " +
          "Credentials will be transmitted in cleartext. " +
          "Only use this configuration with sidecar TLS termination or in secure network environments.",
          connection.getHost(),
          connection.getPort()
      );
    }

    if (tlsConfigured) {
      try {
        SSLContext sslContext = buildSslContext(sslConfig);
        HttpClient httpClient = createHttpClientWithOptionalBasicAuth(sslContext, basicUser, basicPass, connection, sslConfig);

        String httpsHost = "https://" + connection.getHost();

        ConsulRawClient rawClient = new ConsulRawClient(httpsHost, connection.getPort(), httpClient);
        LOGGER.info("Created Consul client with HTTPS to %s:%d", connection.getHost(), connection.getPort());
        return new ConsulClient(rawClient);
      }
      catch (Exception e) {
        // TLS was explicitly configured; fail fast rather than silently downgrade to HTTP
        LOGGER.error(e, "Failed to configure TLS for Consul client (host: %s, port: %d)", connection.getHost(), connection.getPort());
        throw new IllegalStateException("Consul TLS configuration failed; refusing to fall back to HTTP", e);
      }
    }

    // No TLS configured - use plain HTTP
    HttpClient httpClient = createHttpClientWithOptionalBasicAuth(null, basicUser, basicPass, connection, null);
    String httpHost = "http://" + connection.getHost();
    ConsulRawClient rawClient = new ConsulRawClient(httpHost, connection.getPort(), httpClient);
    LOGGER.info("Created Consul client with HTTP to %s:%d", connection.getHost(), connection.getPort());
    return new ConsulClient(rawClient);
  }

  /**
   * Build SSLContext from ConsulSSLConfig using Druid's standard TLS infrastructure.
   */
  private static SSLContext buildSslContext(ConsulSSLConfig config)
  {
    try {
      return new TLSUtils.ClientSSLContextBuilder()
          .setProtocol(config.getProtocol())
          .setTrustStoreType(config.getTrustStoreType())
          .setTrustStorePath(config.getTrustStorePath())
          .setTrustStoreAlgorithm(config.getTrustStoreAlgorithm())
          .setTrustStorePasswordProvider(config.getTrustStorePasswordProvider())
          .setKeyStoreType(config.getKeyStoreType())
          .setKeyStorePath(config.getKeyStorePath())
          .setKeyStoreAlgorithm(config.getKeyManagerFactoryAlgorithm())
          .setCertAlias(config.getCertAlias())
          .setKeyStorePasswordProvider(config.getKeyStorePasswordProvider())
          .setKeyManagerFactoryPasswordProvider(config.getKeyManagerPasswordProvider())
          .setValidateHostnames(config.getValidateHostnames())
          .build();
    }
    catch (Exception e) {
      LOGGER.error(e, "Failed to build SSLContext from ConsulSSLConfig");
      throw new IllegalStateException("Failed to build SSLContext", e);
    }
  }

  /**
   * Create an HttpClient with the given SSLContext.
   */
  private static HttpClient createHttpClientWithOptionalBasicAuth(
      SSLContext sslContext,
      String basicUser,
      String basicPass,
      ConsulDiscoveryConfig.ConnectionConfig connection,
      ConsulSSLConfig sslConfig
  )
  {
    HttpClientBuilder httpBuilder = HttpClients.custom();

    // Always use a PoolingHttpClientConnectionManager with proper pool sizing
    // This prevents ConnectionPoolTimeoutException when multiple threads use the client concurrently
    PoolingHttpClientConnectionManager connectionManager;

    if (sslContext != null) {
      // Configure hostname verification based on sslConfig.validateHostnames
      HostnameVerifier hostnameVerifier = (sslConfig != null && Boolean.FALSE.equals(sslConfig.getValidateHostnames()))
          ? NoopHostnameVerifier.INSTANCE
          : SSLConnectionSocketFactory.getDefaultHostnameVerifier();

      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
      Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
          .register("https", sslSocketFactory)
          .build();
      connectionManager = new PoolingHttpClientConnectionManager(registry);
      httpBuilder.setSSLContext(sslContext);
    } else {
      connectionManager = new PoolingHttpClientConnectionManager();
    }

    // Configure connection pool sizes (configurable for large clusters)
    connectionManager.setMaxTotal(connection.getMaxTotalConnections());
    connectionManager.setDefaultMaxPerRoute(connection.getMaxConnectionsPerRoute());

    httpBuilder.setConnectionManager(connectionManager);

    if (basicUser != null && basicPass != null) {
      final String token = Base64.getEncoder().encodeToString((basicUser + ":" + basicPass).getBytes(StandardCharsets.UTF_8));
      HttpRequestInterceptor authInjector = (request, context) -> {
        if (!request.containsHeader("Authorization")) {
          request.addHeader("Authorization", "Basic " + token);
        }
      };
      httpBuilder.addInterceptorFirst(authInjector);
    }

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout((int) connection.getConnectTimeout().getMillis())
        .setSocketTimeout((int) connection.getSocketTimeout().getMillis())
        .setConnectionRequestTimeout((int) TimeUnit.SECONDS.toMillis(10))
        .build();
    httpBuilder.setDefaultRequestConfig(requestConfig);

    LOGGER.info("ConsulClient configured with connectTimeout=%s, socketTimeout=%s",
                connection.getConnectTimeout(), connection.getSocketTimeout());

    return httpBuilder.build();
  }
}
