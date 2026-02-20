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

package org.apache.druid.testing.embedded.auth;

import com.google.common.base.Throwables;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.apache.druid.java.util.http.client.HttpClientInit;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.apache.druid.server.security.TLSUtils;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.net.URL;

public class ITTLSTest extends EmbeddedClusterTestBase
{
  private static final Duration SSL_HANDSHAKE_TIMEOUT = new Duration(30 * 1000);

  private DruidHttpClientConfig httpClientConfig;
  private TLSCertificateChecker certificateChecker;

  private final EmbeddedSSLAuthResource sslAuthResource = new EmbeddedSSLAuthResource();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedRouter router = new EmbeddedRouter();

  // Router which accepts any certificate
  private final EmbeddedRouter permissiveAnyCertRouter = new EmbeddedRouter()
      .addProperty("druid.plaintextPort", "8889")
      .addProperty("druid.tlsPort", "9089")
      .addProperty("druid.server.https.validateHostnames", "false");

  // Router which does not require a client certificate
  private final EmbeddedRouter noClientAuthRouter = new EmbeddedRouter()
      .addProperty("druid.plaintextPort", "8890")
      .addProperty("druid.tlsPort", "9090")
      .addProperty("druid.server.https.requireClientCertificate", "false")
      .addProperty("druid.server.https.validateHostnames", "false");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addResource(new EmbeddedBasicAuthResource())
        .addResource(sslAuthResource)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker)
        .addServer(historical)
        .addServer(router)
        .addServer(permissiveAnyCertRouter)
        .addServer(noClientAuthRouter);
  }

  @BeforeAll
  public void initHttpClient()
  {
    this.httpClientConfig = overlord.bindings().getInstance(DruidHttpClientConfig.class);
    this.certificateChecker = overlord.bindings().getInstance(TLSCertificateChecker.class);
  }

  @Test
  public void testPlaintextAccess()
  {
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        overlord.bindings().globalHttpClient()
    );
    makeRequest(adminClient, getServerUrl(coordinator) + "/status");
    makeRequest(adminClient, getServerUrl(overlord) + "/status");
    makeRequest(adminClient, getServerUrl(broker) + "/status");
    makeRequest(adminClient, getServerUrl(historical) + "/status");
    makeRequest(adminClient, getServerUrl(router) + "/status");
    makeRequest(adminClient, getServerUrl(permissiveAnyCertRouter) + "/status");
    makeRequest(adminClient, getServerUrl(noClientAuthRouter) + "/status");
  }

  @Test
  public void testTLSNodeAccess()
  {
    HttpClient adminClient = makeCustomHttpClient(
        "client_tls/client.jks",
        "druid"
    );
    makeRequest(adminClient, getServerTlsUrl(coordinator) + "/status");
    makeRequest(adminClient, getServerTlsUrl(overlord) + "/status");
    makeRequest(adminClient, getServerTlsUrl(broker) + "/status");
    makeRequest(adminClient, getServerTlsUrl(historical) + "/status");
    makeRequest(adminClient, getServerTlsUrl(router) + "/status");
    makeRequest(adminClient, getServerTlsUrl(permissiveAnyCertRouter) + "/status");
    makeRequest(adminClient, getServerTlsUrl(noClientAuthRouter) + "/status");
  }

  @Test
  public void testTLSNodeAccessWithIntermediate()
  {
    HttpClient intermediateCertClient = makeCustomHttpClient(
        "client_tls/intermediate_ca_client.jks",
        "intermediate_ca_client"
    );
    makeRequest(intermediateCertClient, getServerTlsUrl(coordinator) + "/status");
    makeRequest(intermediateCertClient, getServerTlsUrl(overlord) + "/status");
    makeRequest(intermediateCertClient, getServerTlsUrl(broker) + "/status");
    makeRequest(intermediateCertClient, getServerTlsUrl(historical) + "/status");
    makeRequest(intermediateCertClient, getServerTlsUrl(router) + "/status");
    makeRequest(intermediateCertClient, getServerTlsUrl(permissiveAnyCertRouter) + "/status");
    makeRequest(intermediateCertClient, getServerTlsUrl(noClientAuthRouter) + "/status");
  }

  @Test
  public void checkAccessWithNoCert()
  {
    HttpClient certlessClient = makeCertlessClient();
    makeRequest(certlessClient, getServerTlsUrl(noClientAuthRouter) + "/status");

    checkFailedAccessNoCert(certlessClient, getServerTlsUrl(coordinator));
    checkFailedAccessNoCert(certlessClient, getServerTlsUrl(overlord));
    checkFailedAccessNoCert(certlessClient, getServerTlsUrl(broker));
    checkFailedAccessNoCert(certlessClient, getServerTlsUrl(historical));
    checkFailedAccessNoCert(certlessClient, getServerTlsUrl(router));
    checkFailedAccessNoCert(certlessClient, getServerTlsUrl(permissiveAnyCertRouter));
  }

  @Test
  public void checkAccessWithWrongHostname()
  {
    HttpClient wrongHostnameClient = makeCustomHttpClient(
        "client_tls/invalid_hostname_client.jks",
        "invalid_hostname_client"
    );
    checkFailedAccessWrongHostname(wrongHostnameClient, getServerTlsUrl(coordinator));
    checkFailedAccessWrongHostname(wrongHostnameClient, getServerTlsUrl(overlord));
    checkFailedAccessWrongHostname(wrongHostnameClient, getServerTlsUrl(broker));
    checkFailedAccessWrongHostname(wrongHostnameClient, getServerTlsUrl(historical));
    checkFailedAccessWrongHostname(wrongHostnameClient, getServerTlsUrl(router));
    makeRequest(wrongHostnameClient, getServerTlsUrl(permissiveAnyCertRouter) + "/status");
    makeRequest(wrongHostnameClient, getServerTlsUrl(noClientAuthRouter) + "/status");
  }

  @Test
  public void checkAccessWithWrongRoot()
  {
    HttpClient wrongRootClient = makeCustomHttpClient(
        "client_tls/client_another_root.jks",
        "druid_another_root"
    );
    checkFailedAccessWrongRoot(wrongRootClient, getServerTlsUrl(coordinator));
    checkFailedAccessWrongRoot(wrongRootClient, getServerTlsUrl(overlord));
    checkFailedAccessWrongRoot(wrongRootClient, getServerTlsUrl(broker));
    checkFailedAccessWrongRoot(wrongRootClient, getServerTlsUrl(historical));
    checkFailedAccessWrongRoot(wrongRootClient, getServerTlsUrl(router));
    checkFailedAccessWrongRoot(wrongRootClient, getServerTlsUrl(permissiveAnyCertRouter));
    makeRequest(wrongRootClient, getServerTlsUrl(noClientAuthRouter) + "/status");
  }

  @Test
  public void checkAccessWithRevokedCert()
  {
    HttpClient revokedClient = makeCustomHttpClient(
        "client_tls/revoked_client.jks",
        "revoked_druid"
    );
    checkFailedAccessRevoked(revokedClient, getServerTlsUrl(coordinator));
    checkFailedAccessRevoked(revokedClient, getServerTlsUrl(overlord));
    checkFailedAccessRevoked(revokedClient, getServerTlsUrl(broker));
    checkFailedAccessRevoked(revokedClient, getServerTlsUrl(historical));
    checkFailedAccessRevoked(revokedClient, getServerTlsUrl(router));
    makeRequest(revokedClient, getServerTlsUrl(permissiveAnyCertRouter) + "/status");
    makeRequest(revokedClient, getServerTlsUrl(noClientAuthRouter) + "/status");
  }

  @Test
  public void checkAccessWithExpiredCert()
  {
    HttpClient expiredClient = makeCustomHttpClient(
        "client_tls/expired_client.jks",
        "expired_client"
    );
    checkFailedAccessExpired(expiredClient, getServerTlsUrl(coordinator));
    checkFailedAccessExpired(expiredClient, getServerTlsUrl(overlord));
    checkFailedAccessExpired(expiredClient, getServerTlsUrl(broker));
    checkFailedAccessExpired(expiredClient, getServerTlsUrl(historical));
    checkFailedAccessExpired(expiredClient, getServerTlsUrl(router));
    checkFailedAccessExpired(expiredClient, getServerTlsUrl(permissiveAnyCertRouter));
    makeRequest(expiredClient, getServerTlsUrl(noClientAuthRouter) + "/status");
  }

  @Test
  public void checkAccessWithNotCASignedCert()
  {
    HttpClient notCAClient = makeCustomHttpClient(
        "client_tls/invalid_ca_client.jks",
        "invalid_ca_client"
    );
    checkFailedAccessNotCA(notCAClient, getServerTlsUrl(coordinator));
    checkFailedAccessNotCA(notCAClient, getServerTlsUrl(overlord));
    checkFailedAccessNotCA(notCAClient, getServerTlsUrl(broker));
    checkFailedAccessNotCA(notCAClient, getServerTlsUrl(historical));
    checkFailedAccessNotCA(notCAClient, getServerTlsUrl(router));
    checkFailedAccessNotCA(notCAClient, getServerTlsUrl(permissiveAnyCertRouter));
    makeRequest(notCAClient, getServerTlsUrl(noClientAuthRouter) + "/status");
  }

  @Test
  @Disabled("Add custom ITTLSCertificateChecker to enable this test")
  public void checkAccessWithCustomCertificateChecks()
  {
    HttpClient wrongHostnameClient = makeCustomHttpClient(
        "client_tls/invalid_hostname_client.jks",
        "invalid_hostname_client",
        null // new ITTLSCertificateChecker()
    );

    // checkFailedAccessWrongHostname(httpClient, HttpMethod.GET, config.getCustomCertCheckRouterTLSUrl());

    // makeRequest(wrongHostnameClient, HttpMethod.GET, config.getCustomCertCheckRouterTLSUrl() + "/status", null);

    checkFailedAccess(
        wrongHostnameClient,
        HttpMethod.POST,
        getServerTlsUrl(router) + "/druid/v2",
        ISE.class,
        "Error while making request to url[https://127.0.0.1:9091/druid/v2] status[400 Bad Request] content[{\"error\":\"Unknown exception\",\"errorMessage\":\"No content to map due to end-of-input"
    );

    makeRequest(wrongHostnameClient, getServerTlsUrl(router) + "/druid/coordinator/v1/leader");
  }

  private void checkFailedAccessNoCert(HttpClient httpClient, String url)
  {
    checkFailedAccess(
        httpClient,
        HttpMethod.GET,
        url + "/status",
        SSLException.class,
        "Received fatal alert: bad_certificate"
    );
  }

  private void checkFailedAccessWrongHostname(HttpClient httpClient, String url)
  {
    checkFailedAccess(
        httpClient,
        HttpMethod.GET,
        url + "/status",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessWrongRoot(HttpClient httpClient, String url)
  {
    checkFailedAccess(
        httpClient,
        HttpMethod.GET,
        url + "/status",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessRevoked(HttpClient httpClient, String url)
  {
    checkFailedAccess(
        httpClient,
        HttpMethod.GET,
        url + "/status",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessExpired(HttpClient httpClient, String url)
  {
    checkFailedAccess(
        httpClient,
        HttpMethod.GET,
        url + "/status",
        SSLException.class,
        "Received fatal alert: certificate_unknown"
    );
  }

  private void checkFailedAccessNotCA(HttpClient httpClient, String url)
  {
    checkFailedAccess(
        httpClient,
        HttpMethod.GET,
        url + "/status",
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
    return makeCustomHttpClient(keystorePath, certAlias, certificateChecker);
  }

  private HttpClient makeCustomHttpClient(
      String keystorePath,
      String certAlias,
      TLSCertificateChecker certificateChecker
  )
  {
    final DefaultPasswordProvider passwordProvider = new DefaultPasswordProvider("druid123");
    SSLContext intermediateClientSSLContext = new TLSUtils.ClientSSLContextBuilder()
        .setProtocol("TLSv1.2")
        .setTrustStorePath(sslAuthResource.getTlsFilePath("client_tls/truststore.jks"))
        .setTrustStoreAlgorithm("PKIX")
        .setTrustStorePasswordProvider(passwordProvider)
        .setKeyStoreType("PKCS12")
        .setKeyStorePath(sslAuthResource.getTlsFilePath(keystorePath))
        .setCertAlias(certAlias)
        .setKeyStorePasswordProvider(passwordProvider)
        .setKeyManagerFactoryPasswordProvider(passwordProvider)
        .setCertificateChecker(certificateChecker)
        .build();

    final HttpClientConfig.Builder builder = getHttpClientConfigBuilder(intermediateClientSSLContext);

    HttpClient client = HttpClientInit.createClient(
        builder.build(),
        new Lifecycle()
    );

    return new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        client
    );
  }

  private HttpClient makeCertlessClient()
  {
    SSLContext certlessClientSSLContext = new TLSUtils.ClientSSLContextBuilder()
        .setProtocol("TLSv1.2")
        .setTrustStoreType("JKS")
        .setTrustStorePath(sslAuthResource.getTlsFilePath("client_tls/truststore.jks"))
        .setTrustStoreAlgorithm("PKIX")
        .setTrustStorePasswordProvider(new DefaultPasswordProvider("druid123"))
        .setCertificateChecker(certificateChecker)
        .build();

    final HttpClientConfig.Builder builder = getHttpClientConfigBuilder(certlessClientSSLContext);

    HttpClient client = HttpClientInit.createClient(
        builder.build(),
        new Lifecycle()
    );

    return new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        client
    );
  }

  private void checkFailedAccess(
      HttpClient httpClient,
      HttpMethod method,
      String url,
      Class<?> expectedException,
      String expectedExceptionMsg
  )
  {
    try {
      makeRequest(httpClient, method, url);
      Assertions.fail(StringUtils.format("Test failed, did not get %s.", expectedException));
    }
    catch (RuntimeException re) {
      Throwable rootCause = Throwables.getRootCause(re);

      Assertions.assertTrue(
          expectedException.isInstance(rootCause),
          StringUtils.format(
              "Expected %s but found %s instead.",
              expectedException,
              Throwables.getStackTraceAsString(rootCause)
          )
      );

      Assertions.assertTrue(rootCause.getMessage().contains(expectedExceptionMsg));
    }
  }

  private void makeRequest(HttpClient httpClient, String url)
  {
    makeRequest(httpClient, HttpMethod.GET, url);
  }

  private void makeRequest(
      HttpClient httpClient,
      HttpMethod method,
      String url
  )
  {
    try {
      Request request = new Request(method, new URL(url));
      StatusResponseHolder response = httpClient.go(
          request,
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        String errMsg = StringUtils.format(
            "Error while making request to url[%s] status[%s] content[%s]",
            url,
            response.getStatus(),
            response.getContent()
        );
        throw new ISE(errMsg);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
