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

package org.apache.druid.tests.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.guice.http.LifecycleUtils;
import org.apache.druid.https.SSLClientConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.security.TLSUtils;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITTLSTest
{
  private static final Logger LOG = new Logger(ITTLSTest.class);

  private static final Duration SSL_HANDSHAKE_TIMEOUT = new Duration(30 * 1000);

  private static final int MAX_CONNECTION_EXCEPTION_RETRIES = 30;

  @Inject
  IntegrationTestingConfig config;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  SSLClientConfig sslClientConfig;

  @Inject
  @Client
  HttpClient httpClient;

  @Inject
  @Client
  DruidHttpClientConfig httpClientConfig;

  StatusResponseHandler responseHandler = new StatusResponseHandler(StandardCharsets.UTF_8);

  @Test
  public void testPlaintextAccess()
  {
    LOG.info("---------Testing resource access without TLS---------");
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );
    makeRequest(adminClient, HttpMethod.GET, config.getCoordinatorUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getIndexerUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getHistoricalUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getRouterUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getPermissiveRouterUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getNoClientAuthRouterUrl() + "/status", null);
  }

  @Test
  public void testTLSNodeAccess()
  {
    LOG.info("---------Testing resource access with TLS enabled---------");
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );
    makeRequest(adminClient, HttpMethod.GET, config.getCoordinatorTLSUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getIndexerTLSUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getBrokerTLSUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getHistoricalTLSUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getRouterTLSUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl() + "/status", null);
    makeRequest(adminClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  @Test
  public void testTLSNodeAccessWithIntermediate()
  {
    LOG.info("---------Testing TLS resource access with 3-part cert chain---------");
    HttpClient intermediateCertClient = makeCustomHttpClient(
        "client_tls/intermediate_ca_client.jks",
        "intermediate_ca_client"
    );
    makeRequest(intermediateCertClient, HttpMethod.GET, config.getCoordinatorTLSUrl() + "/status", null);
    makeRequest(intermediateCertClient, HttpMethod.GET, config.getIndexerTLSUrl() + "/status", null);
    makeRequest(intermediateCertClient, HttpMethod.GET, config.getBrokerTLSUrl() + "/status", null);
    makeRequest(intermediateCertClient, HttpMethod.GET, config.getHistoricalTLSUrl() + "/status", null);
    makeRequest(intermediateCertClient, HttpMethod.GET, config.getRouterTLSUrl() + "/status", null);
    makeRequest(intermediateCertClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl() + "/status", null);
    makeRequest(intermediateCertClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  @Test
  public void checkAccessWithNoCert()
  {
    LOG.info("---------Testing TLS resource access without a certificate---------");
    HttpClient certlessClient = makeCertlessClient();
    checkFailedAccessNoCert(certlessClient, HttpMethod.GET, config.getCoordinatorTLSUrl());
    checkFailedAccessNoCert(certlessClient, HttpMethod.GET, config.getIndexerTLSUrl());
    checkFailedAccessNoCert(certlessClient, HttpMethod.GET, config.getBrokerTLSUrl());
    checkFailedAccessNoCert(certlessClient, HttpMethod.GET, config.getHistoricalTLSUrl());
    checkFailedAccessNoCert(certlessClient, HttpMethod.GET, config.getRouterTLSUrl());
    checkFailedAccessNoCert(certlessClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl());
    makeRequest(certlessClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  @Test
  public void checkAccessWithWrongHostname()
  {
    LOG.info("---------Testing TLS resource access when client certificate has non-matching hostnames---------");
    HttpClient wrongHostnameClient = makeCustomHttpClient(
        "client_tls/invalid_hostname_client.jks",
        "invalid_hostname_client"
    );
    checkFailedAccessWrongHostname(wrongHostnameClient, HttpMethod.GET, config.getCoordinatorTLSUrl());
    checkFailedAccessWrongHostname(wrongHostnameClient, HttpMethod.GET, config.getIndexerTLSUrl());
    checkFailedAccessWrongHostname(wrongHostnameClient, HttpMethod.GET, config.getBrokerTLSUrl());
    checkFailedAccessWrongHostname(wrongHostnameClient, HttpMethod.GET, config.getHistoricalTLSUrl());
    checkFailedAccessWrongHostname(wrongHostnameClient, HttpMethod.GET, config.getRouterTLSUrl());
    makeRequest(wrongHostnameClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl() + "/status", null);
    makeRequest(wrongHostnameClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  @Test
  public void checkAccessWithWrongRoot()
  {
    LOG.info("---------Testing TLS resource access when client certificate is signed by a non-trusted root CA---------");
    HttpClient wrongRootClient = makeCustomHttpClient(
        "client_tls/client_another_root.jks",
        "druid_another_root"
    );
    checkFailedAccessWrongRoot(wrongRootClient, HttpMethod.GET, config.getCoordinatorTLSUrl());
    checkFailedAccessWrongRoot(wrongRootClient, HttpMethod.GET, config.getIndexerTLSUrl());
    checkFailedAccessWrongRoot(wrongRootClient, HttpMethod.GET, config.getBrokerTLSUrl());
    checkFailedAccessWrongRoot(wrongRootClient, HttpMethod.GET, config.getHistoricalTLSUrl());
    checkFailedAccessWrongRoot(wrongRootClient, HttpMethod.GET, config.getRouterTLSUrl());
    checkFailedAccessWrongRoot(wrongRootClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl());
    makeRequest(wrongRootClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  @Test
  public void checkAccessWithRevokedCert()
  {
    LOG.info("---------Testing TLS resource access when client certificate has been revoked---------");
    HttpClient revokedClient = makeCustomHttpClient(
        "client_tls/revoked_client.jks",
        "revoked_druid"
    );
    checkFailedAccessRevoked(revokedClient, HttpMethod.GET, config.getCoordinatorTLSUrl());
    checkFailedAccessRevoked(revokedClient, HttpMethod.GET, config.getIndexerTLSUrl());
    checkFailedAccessRevoked(revokedClient, HttpMethod.GET, config.getBrokerTLSUrl());
    checkFailedAccessRevoked(revokedClient, HttpMethod.GET, config.getHistoricalTLSUrl());
    checkFailedAccessRevoked(revokedClient, HttpMethod.GET, config.getRouterTLSUrl());
    makeRequest(revokedClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl() + "/status", null);
    makeRequest(revokedClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  @Test
  public void checkAccessWithExpiredCert()
  {
    LOG.info("---------Testing TLS resource access when client certificate has expired---------");
    HttpClient expiredClient = makeCustomHttpClient(
        "client_tls/expired_client.jks",
        "expired_client"
    );
    checkFailedAccessExpired(expiredClient, HttpMethod.GET, config.getCoordinatorTLSUrl());
    checkFailedAccessExpired(expiredClient, HttpMethod.GET, config.getIndexerTLSUrl());
    checkFailedAccessExpired(expiredClient, HttpMethod.GET, config.getBrokerTLSUrl());
    checkFailedAccessExpired(expiredClient, HttpMethod.GET, config.getHistoricalTLSUrl());
    checkFailedAccessExpired(expiredClient, HttpMethod.GET, config.getRouterTLSUrl());
    checkFailedAccessExpired(expiredClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl());
    makeRequest(expiredClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  @Test
  public void checkAccessWithNotCASignedCert()
  {
    LOG.info(
        "---------Testing TLS resource access when client certificate is signed by a non-CA intermediate cert---------");
    HttpClient notCAClient = makeCustomHttpClient(
        "client_tls/invalid_ca_client.jks",
        "invalid_ca_client"
    );
    checkFailedAccessNotCA(notCAClient, HttpMethod.GET, config.getCoordinatorTLSUrl());
    checkFailedAccessNotCA(notCAClient, HttpMethod.GET, config.getIndexerTLSUrl());
    checkFailedAccessNotCA(notCAClient, HttpMethod.GET, config.getBrokerTLSUrl());
    checkFailedAccessNotCA(notCAClient, HttpMethod.GET, config.getHistoricalTLSUrl());
    checkFailedAccessNotCA(notCAClient, HttpMethod.GET, config.getRouterTLSUrl());
    checkFailedAccessNotCA(notCAClient, HttpMethod.GET, config.getPermissiveRouterTLSUrl());
    makeRequest(notCAClient, HttpMethod.GET, config.getNoClientAuthRouterTLSUrl() + "/status", null);
  }

  private void checkFailedAccessNoCert(HttpClient httpClient, HttpMethod method, String url)
  {
    checkFailedAccess(
        httpClient,
        method,
        url + "/status",
        "Certless",
        SSLException.class,
        "Received fatal alert: bad_certificate"
    );
  }

  private void checkFailedAccessWrongHostname(HttpClient httpClient, HttpMethod method, String url)
  {
    checkFailedAccess(
        httpClient,
        method,
        url + "/status",
        "Wrong hostname",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessWrongRoot(HttpClient httpClient, HttpMethod method, String url)
  {
    checkFailedAccess(
        httpClient,
        method,
        url + "/status",
        "Wrong root cert",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessRevoked(HttpClient httpClient, HttpMethod method, String url)
  {
    checkFailedAccess(
        httpClient,
        method,
        url + "/status",
        "Revoked cert",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessExpired(HttpClient httpClient, HttpMethod method, String url)
  {
    checkFailedAccess(
        httpClient,
        method,
        url + "/status",
        "Expired cert",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessNotCA(HttpClient httpClient, HttpMethod method, String url)
  {
    checkFailedAccess(
        httpClient,
        method,
        url + "/status",
        "Cert signed by non-CA",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private HttpClientConfig.Builder getHttpClientConfigBuilder(SSLContext sslContext)
  {
    return HttpClientConfig
        .builder()
        .withNumConnections(httpClientConfig.getNumConnections())
        .withReadTimeout(httpClientConfig.getReadTimeout())
        .withWorkerCount(httpClientConfig.getNumMaxThreads())
        .withCompressionCodec(
            HttpClientConfig.CompressionCodec.valueOf(StringUtils.toUpperCase(httpClientConfig.getCompressionCodec()))
        )
        .withUnusedConnectionTimeoutDuration(httpClientConfig.getUnusedConnectionTimeout())
        .withSslHandshakeTimeout(SSL_HANDSHAKE_TIMEOUT)
        .withSslContext(sslContext);
  }

  private HttpClient makeCustomHttpClient(String keystorePath, String certAlias)
  {
    SSLContext intermediateClientSSLContext = new TLSUtils.ClientSSLContextBuilder()
        .setProtocol(sslClientConfig.getProtocol())
        .setTrustStoreType(sslClientConfig.getTrustStoreType())
        .setTrustStorePath(sslClientConfig.getTrustStorePath())
        .setTrustStoreAlgorithm(sslClientConfig.getTrustStoreAlgorithm())
        .setTrustStorePasswordProvider(sslClientConfig.getTrustStorePasswordProvider())
        .setKeyStoreType(sslClientConfig.getKeyStoreType())
        .setKeyStorePath(keystorePath)
        .setKeyStoreAlgorithm(sslClientConfig.getKeyManagerFactoryAlgorithm())
        .setCertAlias(certAlias)
        .setKeyStorePasswordProvider(sslClientConfig.getKeyStorePasswordProvider())
        .setKeyManagerFactoryPasswordProvider(sslClientConfig.getKeyManagerPasswordProvider())
        .build();

    final HttpClientConfig.Builder builder = getHttpClientConfigBuilder(intermediateClientSSLContext);

    final Lifecycle lifecycle = new Lifecycle();

    HttpClient client = HttpClientInit.createClient(
        builder.build(),
        LifecycleUtils.asMmxLifecycle(lifecycle)
    );

    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        client
    );
    return adminClient;
  }

  private HttpClient makeCertlessClient()
  {
    SSLContext certlessClientSSLContext = new TLSUtils.ClientSSLContextBuilder()
        .setProtocol(sslClientConfig.getProtocol())
        .setTrustStoreType(sslClientConfig.getTrustStoreType())
        .setTrustStorePath(sslClientConfig.getTrustStorePath())
        .setTrustStoreAlgorithm(sslClientConfig.getTrustStoreAlgorithm())
        .setTrustStorePasswordProvider(sslClientConfig.getTrustStorePasswordProvider())
        .build();

    final HttpClientConfig.Builder builder = getHttpClientConfigBuilder(certlessClientSSLContext);

    final Lifecycle lifecycle = new Lifecycle();

    HttpClient client = HttpClientInit.createClient(
        builder.build(),
        LifecycleUtils.asMmxLifecycle(lifecycle)
    );

    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        client
    );
    return adminClient;
  }

  private void checkFailedAccess(
      HttpClient httpClient,
      HttpMethod method,
      String url,
      String clientDesc,
      Class expectedException,
      String expectedExceptionMsg
  )
  {
    int retries = 0;
    while (true) {
      try {
        makeRequest(httpClient, method, url, null, -1);
      }
      catch (RuntimeException re) {
        Throwable rootCause = Throwables.getRootCause(re);

        if (rootCause instanceof IOException && ("Broken pipe".equals(rootCause.getMessage())
                                                 || "Connection reset by peer".contains(rootCause.getMessage()))) {
          if (retries > MAX_CONNECTION_EXCEPTION_RETRIES) {
            Assert.fail(StringUtils.format(
                "Broken pipe / connection reset retries exhausted, test failed, did not get %s.",
                expectedException
            ));
          } else {
            retries += 1;
            continue;
          }
        }

        Assert.assertTrue(
            expectedException.isInstance(rootCause),
            StringUtils.format("Expected %s but found %s instead.", expectedException, rootCause)
        );

        Assert.assertEquals(
            rootCause.getMessage(),
            expectedExceptionMsg
        );

        LOG.info("%s client [%s] request failed as expected when accessing [%s]", clientDesc, method, url);
        return;
      }
      Assert.fail(StringUtils.format("Test failed, did not get %s.", expectedException));
    }
  }

  private StatusResponseHolder makeRequest(HttpClient httpClient, HttpMethod method, String url, byte[] content)
  {
    return makeRequest(httpClient, method, url, content, 4);
  }

  private StatusResponseHolder makeRequest(
      HttpClient httpClient,
      HttpMethod method,
      String url,
      byte[] content,
      int maxRetries
  )
  {
    try {
      Request request = new Request(method, new URL(url));
      if (content != null) {
        request.setContent(MediaType.APPLICATION_JSON, content);
      }
      int retryCount = 0;

      StatusResponseHolder response;

      while (true) {
        response = httpClient.go(
            request,
            responseHandler
        ).get();

        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
          String errMsg = StringUtils.format(
              "Error while making request to url[%s] status[%s] content[%s]",
              url,
              response.getStatus(),
              response.getContent()
          );
          if (retryCount > maxRetries) {
            throw new ISE(errMsg);
          } else {
            LOG.error(errMsg);
            LOG.error("retrying in 3000ms, retryCount: " + retryCount);
            retryCount++;
            Thread.sleep(3000);
          }
        } else {
          LOG.info("[%s] request to [%s] succeeded.", method, url);
          break;
        }
      }
      return response;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
