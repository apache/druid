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

package org.apache.druid.jdbc.embedded;

import org.apache.druid.https.SSLContextModule;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.security.basic.BasicSecurityDruidModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;


public class EmbeddedSecureJdbcDriverTest extends EmbeddedJdbcDriverTest
{
  private static final String ADMIN_USERNAME = "admin";
  private static final String ADMIN_PASSWORD = "admin123";
  private static final String INTERNAL_USERNAME = "druid_system";
  private static final String INTERNAL_PASSWORD = "internal123";
  private static final String KEYSTORE_PASSWORD = "druid123";

  private Path tlsTempDir;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    final String keyStorePath;
    final String trustStorePath;
    try {
      tlsTempDir = FileUtils.createTempDir("druid-tls-test").toPath();
      keyStorePath = copyClasspathResource("server.jks").toString();
      trustStorePath = copyClasspathResource("truststore.jks").toString();
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to set up TLS test keystores", e);
    }

    return super
        .createCluster()
        // Basic authentication
        .addExtension(BasicSecurityDruidModule.class)
        .addCommonProperty("druid.auth.authenticatorChain", "[\"basic\"]")
        .addCommonProperty("druid.auth.authenticator.basic.type", "basic")
        .addCommonProperty("druid.auth.authenticator.basic.initialAdminPassword", ADMIN_PASSWORD)
        .addCommonProperty("druid.auth.authenticator.basic.initialInternalClientPassword", INTERNAL_PASSWORD)
        .addCommonProperty("druid.auth.authenticator.basic.authorizerName", "basic")
        .addCommonProperty("druid.auth.authorizers", "[\"basic\"]")
        .addCommonProperty("druid.auth.authorizer.basic.type", "basic")
        .addCommonProperty("druid.escalator.type", "basic")
        .addCommonProperty("druid.escalator.internalClientUsername", INTERNAL_USERNAME)
        .addCommonProperty("druid.escalator.internalClientPassword", INTERNAL_PASSWORD)
        .addCommonProperty("druid.escalator.authorizerName", "basic")
        // TLS - server configuration
        .addExtension(SSLContextModule.class)
        .addCommonProperty("druid.enableTlsPort", "true")
        .addCommonProperty("druid.server.https.keyStorePath", keyStorePath)
        .addCommonProperty("druid.server.https.keyStoreType", "jks")
        .addCommonProperty("druid.server.https.keyStorePassword", KEYSTORE_PASSWORD)
        .addCommonProperty("druid.server.https.keyManagerPassword", KEYSTORE_PASSWORD)
        .addCommonProperty("druid.server.https.certAlias", "druid")
        .addCommonProperty("druid.server.https.requireClientCertificate", "false")
        .addCommonProperty("druid.server.https.validateHostnames", "false")
        .addCommonProperty("druid.server.https.trustStorePath", trustStorePath)
        .addCommonProperty("druid.server.https.trustStoreAlgorithm", "PKIX")
        .addCommonProperty("druid.server.https.trustStorePassword", KEYSTORE_PASSWORD)
        // TLS - client configuration for inter-service communication
        .addCommonProperty("druid.client.https.protocol", "TLSv1.2")
        .addCommonProperty("druid.client.https.trustStorePath", trustStorePath)
        .addCommonProperty("druid.client.https.trustStoreAlgorithm", "PKIX")
        .addCommonProperty("druid.client.https.trustStorePassword", KEYSTORE_PASSWORD);
  }

  @AfterAll
  void cleanupTls() throws IOException
  {
    if (tlsTempDir != null) {
      Files.walk(tlsTempDir)
           .sorted(Comparator.reverseOrder())
           .map(Path::toFile)
           .forEach(File::delete);
    }
  }

  @Test
  @Timeout(120)
  public void test_plainUrl_shouldFail()
  {
    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> {
          try (Connection ignored = DriverManager.getConnection(getJdbcUrlPlain())) {
            // The connection must fail to open.
          }
        }
    );

    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("HTTP 401 error"));
  }

  @Test
  @Timeout(120)
  public void test_jdbcConnection_basicProperties() throws Exception
  {
    final Properties props = new Properties();
    props.setProperty("authentication", "basic");
    props.setProperty("user", ADMIN_USERNAME);
    props.setProperty("password", ADMIN_PASSWORD);

    try (Connection connection = DriverManager.getConnection(getJdbcUrlPlain(), props)) {
      try (Statement statement = connection.createStatement()) {
        final String query = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
        try (ResultSet resultSet = statement.executeQuery(query)) {
          assertResultSet(Collections.singletonList(new Object[]{10L}), resultSet);
        }
      }
    }
  }

  @Test
  @Timeout(120)
  public void test_jdbcConnection_basicRawProperties() throws Exception
  {
    final Properties props = new Properties();
    props.setProperty("authentication", "basicRaw");
    props.setProperty(
        "password",
        Base64.getEncoder()
              .encodeToString((ADMIN_USERNAME + ':' + ADMIN_PASSWORD).getBytes(StandardCharsets.UTF_8))
    );

    try (Connection connection = DriverManager.getConnection(getJdbcUrlPlain(), props)) {
      try (Statement statement = connection.createStatement()) {
        final String query = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
        try (ResultSet resultSet = statement.executeQuery(query)) {
          assertResultSet(Collections.singletonList(new Object[]{10L}), resultSet);
        }
      }
    }
  }

  /**
   * The server's certificate is self-signed, so the default trust store rejects it.
   */
  @Test
  @Timeout(120)
  public void test_untrustedCert_shouldFail()
  {
    final String urlWithVerification = StringUtils.format(
        "jdbc:druid:https://localhost:9088/druid/v2/sql/?authentication=basic&user=%s&password=%s",
        StringUtils.urlEncode(ADMIN_USERNAME),
        StringUtils.urlEncode(ADMIN_PASSWORD)
    );

    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> {
          try (Connection ignored = DriverManager.getConnection(urlWithVerification)) {
            // The connection must fail to open.
          }
        }
    );

    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("PKIX path building failed"));
  }

  @Override
  protected String getJdbcUrl()
  {
    return StringUtils.format(
        "%s&authentication=basic&user=%s&password=%s",
        getJdbcUrlPlain(),
        StringUtils.urlEncode(ADMIN_USERNAME),
        StringUtils.urlEncode(ADMIN_PASSWORD)
    );
  }

  /**
   * HTTPS with TLS verification off, and no authentication parameters.
   */
  private String getJdbcUrlPlain()
  {
    return "jdbc:druid:https://localhost:9088/druid/v2/sql/?verifyTls=false";
  }

  private Path copyClasspathResource(final String resourceName) throws IOException
  {
    final Path target = tlsTempDir.resolve(resourceName);
    try (InputStream in = getClass().getClassLoader().getResourceAsStream(resourceName)) {
      if (in == null) {
        throw new IOException("Classpath resource not found: " + resourceName);
      }
      Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
    }
    return target;
  }
}
