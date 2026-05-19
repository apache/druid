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

package org.apache.druid.extensions.openlineage;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.RequestLoggerProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Set;

/**
 * Configure via {@code druid.request.logging.type=openlineage} in {@code runtime.properties}.
 */
@JsonTypeName("openlineage")
public class OpenLineageRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(OpenLineageRequestLoggerProvider.class);

  public enum TransportType
  {
    CONSOLE,
    HTTP
  }

  @JacksonInject
  @Json
  @NotNull
  private ObjectMapper jsonMapper;

  @JsonProperty
  @NotNull
  private String namespace = "druid://" + DruidNode.getDefaultHost();

  @JsonProperty
  @NotNull
  private TransportType transportType = TransportType.CONSOLE;

  @Nullable
  @JsonProperty
  private String transportUrl;

  @JsonProperty
  @NotNull
  private Set<String> excludedNativeQueryTypes = Set.of(
      "segmentMetadata",
      "dataSourceMetadata",
      "timeBoundary"
  );

  @JsonProperty
  private int emitQueueCapacity = OpenLineageRequestLogger.DEFAULT_EMIT_QUEUE_CAPACITY;

  @JsonProperty
  private int emitThreadCount = OpenLineageRequestLogger.DEFAULT_EMIT_THREAD_COUNT;

  @Nullable
  @JsonProperty
  private String trustStorePath;

  @Nullable
  @JsonProperty
  private String trustStorePassword;

  @Nullable
  @JsonProperty
  private String keyStorePath;

  @Nullable
  @JsonProperty
  private String keyStorePassword;

  @Override
  public RequestLogger get()
  {
    log.debug("Creating OpenLineageRequestLogger [namespace=%s, transport=%s]", namespace, transportType);
    HttpClient httpClient = transportType == TransportType.HTTP ? buildHttpClient() : null;
    return new OpenLineageRequestLogger(
        jsonMapper,
        namespace,
        transportType,
        transportUrl,
        excludedNativeQueryTypes,
        emitQueueCapacity,
        emitThreadCount,
        httpClient
    );
  }

  private HttpClient buildHttpClient()
  {
    RequestConfig requestConfig = RequestConfig.custom()
                                               .setConnectTimeout(5000)
                                               .setSocketTimeout(10000)
                                               .setConnectionRequestTimeout(5000)
                                               .build();
    if (trustStorePath == null && keyStorePath == null) {
      return HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
    }
    try {
      HttpClientBuilder builder = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig);
      TrustManagerFactory tmf = null;
      if (trustStorePath != null) {
        try (FileInputStream in = new FileInputStream(new File(trustStorePath))) {
          KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
          trustStore.load(in, trustStorePassword != null ? trustStorePassword.toCharArray() : null);
          tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          tmf.init(trustStore);
        }
      }
      KeyManagerFactory kmf = null;
      if (keyStorePath != null) {
        try (FileInputStream in = new FileInputStream(new File(keyStorePath))) {
          KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
          keyStore.load(in, keyStorePassword != null ? keyStorePassword.toCharArray() : null);
          kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          kmf.init(keyStore, keyStorePassword != null ? keyStorePassword.toCharArray() : null);
        }
      }
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(
          kmf != null ? kmf.getKeyManagers() : null,
          tmf != null ? tmf.getTrustManagers() : null,
          null
      );
      return builder.setSSLContext(sslContext).build();
    }
    catch (Exception e) {
      throw new IllegalStateException("Failed to configure TLS for OpenLineage HTTP transport", e);
    }
  }
}
