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
import org.apache.druid.java.util.common.RetryUtils;
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
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.ClosedChannelException;

public class ITTLSTest extends EmbeddedClusterTestBase
{
  private static final int MAX_BROKEN_PIPE_RETRIES = 10;
  private static final Duration SSL_HANDSHAKE_TIMEOUT = new Duration(30 * 1000);

  private static final String ERROR_CERTIFICATE_UNKNOWN = "Received fatal alert: certificate_unknown";

  private DruidHttpClientConfig httpClientConfig;
  private TLSCertificateChecker certificateChecker;

  private final EmbeddedSSLAuthResource sslAuthResource = new EmbeddedSSLAuthResource();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedRouter router = new EmbeddedRouter();

  private final EmbeddedRouter routerAcceptsAnyCert = new EmbeddedRouter()
      .addProperty("druid.plaintextPort", "8889")
      .addProperty("druid.tlsPort", "9089")
      .addProperty("druid.server.https.validateHostnames", "false");

  private final EmbeddedRouter routerNeedsNoClientCert = new EmbeddedRouter()
      .addProperty("druid.plaintextPort", "8890")
      .addProperty("druid.tlsPort", "9090")
      .addProperty("druid.server.https.requireClientCertificate", "false")
      .addProperty("druid.server.https.validateHostnames", "false");

  private final EmbeddedRouter routerWithCustomCertChecker = new EmbeddedRouter()
      .addProperty("druid.plaintextPort", "8891")
      .addProperty("druid.tlsPort", "9091")
      .addProperty("druid.tls.certificateChecker", ITTLSCertificateCheckerModule.IT_CHECKER_TYPE);

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addResource(new EmbeddedBasicAuthResource())
        .addExtension(ITTLSCertificateCheckerModule.class)
        .addResource(sslAuthResource)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker)
        .addServer(historical)
        .addServer(router)
        .addServer(routerAcceptsAnyCert)
        .addServer(routerNeedsNoClientCert)
        .addServer(routerWithCustomCertChecker);
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
    verifyGetStatusIsOk(adminClient, coordinator);
    verifyGetStatusIsOk(adminClient, overlord);
    verifyGetStatusIsOk(adminClient, broker);
    verifyGetStatusIsOk(adminClient, historical);
    verifyGetStatusIsOk(adminClient, router);
    verifyGetStatusIsOk(adminClient, routerAcceptsAnyCert);
    verifyGetStatusIsOk(adminClient, routerNeedsNoClientCert);
  }

  @Test
  public void testTLSNodeAccess()
  {
    HttpClient adminClient = makeCustomHttpClient(
        "client_tls/client.jks",
        "druid"
    );
    verifyGetStatusHttpsIsOk(adminClient, coordinator);
    verifyGetStatusHttpsIsOk(adminClient, overlord);
    verifyGetStatusHttpsIsOk(adminClient, broker);
    verifyGetStatusHttpsIsOk(adminClient, historical);
    verifyGetStatusHttpsIsOk(adminClient, router);
    verifyGetStatusHttpsIsOk(adminClient, routerAcceptsAnyCert);
    verifyGetStatusHttpsIsOk(adminClient, routerNeedsNoClientCert);
  }

  @Test
  public void testTLSNodeAccessWithIntermediate()
  {
    HttpClient intermediateCertClient = makeCustomHttpClient(
        "client_tls/intermediate_ca_client.jks",
        "intermediate_ca_client"
    );
    verifyGetStatusHttpsIsOk(intermediateCertClient, coordinator);
    verifyGetStatusHttpsIsOk(intermediateCertClient, overlord);
    verifyGetStatusHttpsIsOk(intermediateCertClient, broker);
    verifyGetStatusHttpsIsOk(intermediateCertClient, historical);
    verifyGetStatusHttpsIsOk(intermediateCertClient, router);
    verifyGetStatusHttpsIsOk(intermediateCertClient, routerAcceptsAnyCert);
    verifyGetStatusHttpsIsOk(intermediateCertClient, routerNeedsNoClientCert);
  }

  @Test
  public void checkAccessWithNoCert()
  {
    HttpClient certlessClient = makeCertlessClient();
    verifyGetStatusHttpsIsOk(certlessClient, routerNeedsNoClientCert);

    final String sslErrorMessage = "Received fatal alert: bad_certificate";
    verifyGetHttpsFailsWith(sslErrorMessage, coordinator, certlessClient);
    verifyGetHttpsFailsWith(sslErrorMessage, overlord, certlessClient);
    verifyGetHttpsFailsWith(sslErrorMessage, broker, certlessClient);
    verifyGetHttpsFailsWith(sslErrorMessage, historical, certlessClient);
    verifyGetHttpsFailsWith(sslErrorMessage, router, certlessClient);
    verifyGetHttpsFailsWith(sslErrorMessage, routerAcceptsAnyCert, certlessClient);
  }

  @Test
  public void checkAccessWithWrongHostname()
  {
    HttpClient wrongHostnameClient = makeCustomHttpClient(
        "client_tls/invalid_hostname_client.jks",
        "invalid_hostname_client"
    );
    verifyGetStatusHttpsIsOk(wrongHostnameClient, routerAcceptsAnyCert);
    verifyGetStatusHttpsIsOk(wrongHostnameClient, routerNeedsNoClientCert);

    final String sslErrorMessage = ERROR_CERTIFICATE_UNKNOWN;
    verifyGetHttpsFailsWith(sslErrorMessage, coordinator, wrongHostnameClient);
    verifyGetHttpsFailsWith(sslErrorMessage, overlord, wrongHostnameClient);
    verifyGetHttpsFailsWith(sslErrorMessage, broker, wrongHostnameClient);
    verifyGetHttpsFailsWith(sslErrorMessage, historical, wrongHostnameClient);
    verifyGetHttpsFailsWith(sslErrorMessage, router, wrongHostnameClient);
  }

  @Test
  public void checkAccessWithWrongRoot()
  {
    HttpClient wrongRootClient = makeCustomHttpClient(
        "client_tls/client_another_root.jks",
        "druid_another_root"
    );
    verifyGetStatusHttpsIsOk(wrongRootClient, routerNeedsNoClientCert);

    final String sslErrorMessage = ERROR_CERTIFICATE_UNKNOWN;
    verifyGetHttpsFailsWith(sslErrorMessage, coordinator, wrongRootClient);
    verifyGetHttpsFailsWith(sslErrorMessage, overlord, wrongRootClient);
    verifyGetHttpsFailsWith(sslErrorMessage, broker, wrongRootClient);
    verifyGetHttpsFailsWith(sslErrorMessage, historical, wrongRootClient);
    verifyGetHttpsFailsWith(sslErrorMessage, router, wrongRootClient);
    verifyGetHttpsFailsWith(sslErrorMessage, routerAcceptsAnyCert, wrongRootClient);
  }

  @Test
  public void checkAccessWithRevokedCert()
  {
    HttpClient revokedClient = makeCustomHttpClient(
        "client_tls/revoked_client.jks",
        "revoked_druid"
    );
    verifyGetStatusHttpsIsOk(revokedClient, routerAcceptsAnyCert);
    verifyGetStatusHttpsIsOk(revokedClient, routerNeedsNoClientCert);

    final String sslErrorMessage = ERROR_CERTIFICATE_UNKNOWN;
    verifyGetHttpsFailsWith(sslErrorMessage, coordinator, revokedClient);
    verifyGetHttpsFailsWith(sslErrorMessage, overlord, revokedClient);
    verifyGetHttpsFailsWith(sslErrorMessage, broker, revokedClient);
    verifyGetHttpsFailsWith(sslErrorMessage, historical, revokedClient);
    verifyGetHttpsFailsWith(sslErrorMessage, router, revokedClient);
  }

  @Test
  public void checkAccessWithExpiredCert()
  {
    HttpClient expiredClient = makeCustomHttpClient(
        "client_tls/expired_client.jks",
        "expired_client"
    );
    verifyGetStatusHttpsIsOk(expiredClient, routerNeedsNoClientCert);

    final String sslErrorMessage = ERROR_CERTIFICATE_UNKNOWN;
    verifyGetHttpsFailsWith(sslErrorMessage, coordinator, expiredClient);
    verifyGetHttpsFailsWith(sslErrorMessage, overlord, expiredClient);
    verifyGetHttpsFailsWith(sslErrorMessage, broker, expiredClient);
    verifyGetHttpsFailsWith(sslErrorMessage, historical, expiredClient);
    verifyGetHttpsFailsWith(sslErrorMessage, router, expiredClient);
    verifyGetHttpsFailsWith(sslErrorMessage, routerAcceptsAnyCert, expiredClient);
  }

  @Test
  public void checkAccessWithNotCASignedCert()
  {
    HttpClient notCAClient = makeCustomHttpClient(
        "client_tls/invalid_ca_client.jks",
        "invalid_ca_client"
    );
    final String sslErrorMessage = ERROR_CERTIFICATE_UNKNOWN;
    verifyGetHttpsFailsWith(sslErrorMessage, coordinator, notCAClient);
    verifyGetHttpsFailsWith(sslErrorMessage, overlord, notCAClient);
    verifyGetHttpsFailsWith(sslErrorMessage, broker, notCAClient);
    verifyGetHttpsFailsWith(sslErrorMessage, historical, notCAClient);
    verifyGetHttpsFailsWith(sslErrorMessage, router, notCAClient);
    verifyGetHttpsFailsWith(sslErrorMessage, routerAcceptsAnyCert, notCAClient);

    verifyGetStatusHttpsIsOk(notCAClient, routerNeedsNoClientCert);
  }

  @Test
  public void checkAccessWithCustomCertificateChecks()
  {
    // Verify GET over HTTPS fails with default HttpClient
    verifyGetHttpsFailsWith(
        ERROR_CERTIFICATE_UNKNOWN,
        routerWithCustomCertChecker,
        overlord.bindings().escalatedHttpClient()
    );

    final HttpClient wrongHostnameClient = makeCustomHttpClient(
        "client_tls/invalid_hostname_client.jks",
        "invalid_hostname_client",
        new ITTLSCertificateChecker()
    );
    verifyGetStatusHttpsIsOk(wrongHostnameClient, routerWithCustomCertChecker);

    // Verify that access to Broker fails
    checkFailedAccess(
        wrongHostnameClient,
        HttpMethod.POST,
        getServerHttpsUrl(routerWithCustomCertChecker) + "/druid/v2",
        ISE.class,
        "Error while making request to url[https://127.0.0.1:9091/druid/v2] status[400 Bad Request]"
    );

    // Verify that access to Coordinator works
    makeRequest(
        wrongHostnameClient,
        HttpMethod.GET,
        getServerHttpsUrl(routerWithCustomCertChecker) + "/druid/coordinator/v1/leader"
    );
  }

  private void verifyGetHttpsFailsWith(String expectedMessage, EmbeddedDruidServer<?> server, HttpClient httpClient)
  {
    checkFailedAccess(
        httpClient,
        HttpMethod.GET,
        getServerHttpsUrl(server),
        SSLException.class,
        expectedMessage
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
      RetryUtils.retry(
          () -> {
            makeRequest(httpClient, method, url);
            return 0;
          },
          e -> isRetriable(Throwables.getRootCause(e)),
          MAX_BROKEN_PIPE_RETRIES
      );

      Assertions.fail("Did not get expected exception: " + expectedException);
    }
    catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (expectedException.isInstance(rootCause)) {
        Assertions.assertTrue(rootCause.getMessage().contains(expectedExceptionMsg));
      } else {
        Assertions.fail("Got a different exception instead of expected: " + expectedException, rootCause);
      }
    }
  }

  private void verifyGetStatusIsOk(HttpClient httpClient, EmbeddedDruidServer<?> server)
  {
    makeRequest(httpClient, HttpMethod.GET, getServerUrl(server) + "/status");
  }
  
  private void verifyGetStatusHttpsIsOk(HttpClient httpClient, EmbeddedDruidServer<?> server)
  {
    makeRequest(httpClient, HttpMethod.GET, getServerHttpsUrl(server) + "/status");
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

  private boolean isRetriable(Throwable ex)
  {
    if (!(ex instanceof IOException)) {
      return false;
    }

    if (ex instanceof ClosedChannelException) {
      return true;
    }

    return null != ex.getMessage()
           && ("Broken pipe".equals(ex.getMessage())
               || "Connection reset by peer".contains(ex.getMessage()));
  }
}
