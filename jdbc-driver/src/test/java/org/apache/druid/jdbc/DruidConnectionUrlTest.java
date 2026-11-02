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

package org.apache.druid.jdbc;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;


public class DruidConnectionUrlTest
{
  @ParameterizedTest(name = "{0}")
  @CsvSource({
      "jdbc:druid:http://localhost:8888/druid/v2/sql/,       http,  localhost, 8888, /druid/v2/sql/,       http://localhost:8888/druid/v2/sql/",
      "jdbc:druid:http://localhost:8888/druid/v2/sql/extra,  http,  localhost, 8888, /druid/v2/sql/extra,  http://localhost:8888/druid/v2/sql/extra",
      "jdbc:druid:http://localhost:8888/druid/v2/sql/extra/, http,  localhost, 8888, /druid/v2/sql/extra/, http://localhost:8888/druid/v2/sql/extra/",
      "jdbc:druid:http://localhost/druid/v2/sql/,            http,  localhost, 80,   /druid/v2/sql/,       http://localhost/druid/v2/sql/",
      // The parser does not require a path; an absent path is accepted and yields an empty path.
      "jdbc:druid:http://localhost:8888,                     http,  localhost, 8888, '',                   http://localhost:8888",
      "jdbc:druid:https://localhost/druid/v2/sql/,           https, localhost, 443,  /druid/v2/sql/,       https://localhost/druid/v2/sql/",
      "jdbc:druid:https://localhost:8443/druid/v2/sql/,      https, localhost, 8443, /druid/v2/sql/,       https://localhost:8443/druid/v2/sql/"
  })
  public void testParse(
      final String jdbcUrl,
      final String scheme,
      final String host,
      final int port,
      final String path,
      final String expectedHttpUrl
  ) throws SQLException
  {
    final DruidConnectionUrl url = DruidConnectionUrl.parse(jdbcUrl, null);

    Assertions.assertEquals(scheme, url.getScheme());
    Assertions.assertEquals(host, url.getHost());
    Assertions.assertEquals(port, url.getPort());
    Assertions.assertEquals(path, url.getPath());
    Assertions.assertEquals(expectedHttpUrl, url.buildHttpUrl());
  }

  @Test
  public void testClientPropertiesAndQueryContextSeparation() throws SQLException
  {
    final DruidConnectionUrl url = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:8888/druid/v2/sql/druid"
        + "?authentication=basic&user=admin&password=secret"
        + "&timeout=5000&useApproximateCountDistinct=false&maxRows=1000",
        null
    );

    Assertions.assertEquals("/druid/v2/sql/druid", url.getPath());

    final ClientProperties clientProps = url.getClientProperties();
    Assertions.assertEquals("basic", clientProps.getAuthentication());
    Assertions.assertEquals("admin", clientProps.getUser());
    Assertions.assertEquals("secret", clientProps.getPassword());

    final Map<String, String> queryContext = url.getQueryContext();
    Assertions.assertEquals(3, queryContext.size());
    Assertions.assertEquals("5000", queryContext.get("timeout"));
    Assertions.assertEquals("false", queryContext.get("useApproximateCountDistinct"));
    Assertions.assertEquals("1000", queryContext.get("maxRows"));
  }

  @Test
  public void testOnlyOneKindOfParameter() throws SQLException
  {
    final DruidConnectionUrl clientOnly = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:8888/druid/v2/sql/?authentication=basic&user=admin&password=secret",
        null
    );
    Assertions.assertEquals("admin", clientOnly.getClientProperties().getUser());
    Assertions.assertTrue(clientOnly.getQueryContext().isEmpty());

    final DruidConnectionUrl contextOnly = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:8888/druid/v2/sql/?timeout=5000&maxRows=1000",
        null
    );
    Assertions.assertNull(contextOnly.getClientProperties().getAuthentication());
    Assertions.assertNull(contextOnly.getClientProperties().getUser());
    Assertions.assertNull(contextOnly.getClientProperties().getPassword());
    Assertions.assertEquals(2, contextOnly.getQueryContext().size());
  }

  @Test
  public void testUrlParametersOverrideConnectionProperties() throws SQLException
  {
    final Properties connectionProps = new Properties();
    connectionProps.setProperty("timeout", "1000");
    connectionProps.setProperty("user", "defaultuser");
    connectionProps.setProperty("password", "testpass");
    connectionProps.setProperty("authentication", "token");
    connectionProps.setProperty("queryTimeout", "30000");

    final DruidConnectionUrl url = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:8888/druid/v2/sql/?timeout=5000&user=urluser",
        connectionProps
    );

    Assertions.assertEquals("5000", url.getQueryContext().get("timeout"));
    Assertions.assertEquals("urluser", url.getClientProperties().getUser());

    // Connection properties with no URL counterpart are preserved.
    Assertions.assertEquals("token", url.getClientProperties().getAuthentication());
    Assertions.assertEquals("testpass", url.getClientProperties().getPassword());
    Assertions.assertEquals("30000", url.getQueryContext().get("queryTimeout"));
  }

  @Test
  public void testParseFailures()
  {
    assertParseFails(null, "Must start with[jdbc:druid:]");
    assertParseFails("jdbc:mysql://localhost:3306/test", "Must start with[jdbc:druid:]");
    assertParseFails("jdbc:druid:http://:8888/", "Host is required");
    assertParseFails("jdbc:druid:http://localhost:invalid_port/", "Invalid JDBC URL");
    assertParseFails("jdbc:druid://localhost:8888/", "Scheme must be 'http' or 'https'");
    assertParseFails("jdbc:druid:ftp://localhost:8888/", "Scheme must be 'http' or 'https'");
  }

  private static SQLException assertParseFails(final String jdbcUrl, final String expectedMessage)
  {
    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> DruidConnectionUrl.parse(jdbcUrl, null)
    );
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString(expectedMessage));
    return exception;
  }

  @Test
  public void testVerifyTlsParameter() throws SQLException
  {
    final String jdbcUrl = "jdbc:druid:https://localhost:8443/druid/v2/sql/";
    Assertions.assertTrue(DruidConnectionUrl.parse(jdbcUrl, null).getClientProperties().isVerifyTls());

    final Properties props = new Properties();
    props.setProperty("verifyTls", "false");
    Assertions.assertFalse(DruidConnectionUrl.parse(jdbcUrl, props).getClientProperties().isVerifyTls());
  }

  @Test
  public void testUrlEncodedParameters() throws SQLException
  {
    final DruidConnectionUrl url = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:8888/druid/v2/sql/"
        + "?user=test%20user&password=p%40ss%2Fword&query%2Bname=value%20with%20spaces"
        + "&jsonConfig=%7B%22key%22%3A%22value%22%7D",
        null
    );

    Assertions.assertEquals("test user", url.getClientProperties().getUser());
    Assertions.assertEquals("p@ss/word", url.getClientProperties().getPassword());

    Assertions.assertEquals(2, url.getQueryContext().size());
    Assertions.assertEquals("value with spaces", url.getQueryContext().get("query+name"));
    Assertions.assertEquals("{\"key\":\"value\"}", url.getQueryContext().get("jsonConfig"));
  }

  @Test
  public void testUrlEncodedParametersDecodedOnlyOnce() throws SQLException
  {
    final DruidConnectionUrl url = DruidConnectionUrl.parse(
        "jdbc:druid:http://localhost:8888/druid/v2/sql/?password=p%26w%3Dd%2Bx%25y&user=bob",
        null
    );

    Assertions.assertEquals("bob", url.getClientProperties().getUser());
    Assertions.assertEquals("p&w=d+x%y", url.getClientProperties().getPassword());
    Assertions.assertEquals(Map.of(), url.getQueryContext());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(DruidConnectionUrl.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testParseFailureDoesNotLeakUrl()
  {
    // Each of these fails at a different point in parse().
    final String[] urls = {
        // Illegal character in the query string.
        "jdbc:druid:http://localhost:8888/druid/v2/sql/?user=admin&password=hunter2|x",
        // Illegal character in the authority.
        "jdbc:druid:http://local host:8888/druid/v2/sql/?user=admin&password=hunter2",
        // Parses as a URI, but has no host.
        "jdbc:druid:http://localhost:not_a_port/druid/v2/sql/?user=admin&password=hunter2",
        // Parses as a URI, but the scheme is rejected by the driver.
        "jdbc:druid:ftp://localhost:8888/druid/v2/sql/?user=admin&password=hunter2"
    };

    for (final String jdbcUrl : urls) {
      final SQLException exception = Assertions.assertThrows(
          SQLException.class,
          () -> DruidConnectionUrl.parse(jdbcUrl, null)
      );

      // Neither the message nor anything reachable from it, such as a cause's message in a logged stack trace,
      // may contain the URL.
      for (Throwable t = exception; t != null; t = t.getCause()) {
        final String message = t.getMessage();
        if (message == null) {
          continue;
        }
        MatcherAssert.assertThat(message, Matchers.not(Matchers.containsString("localhost")));
        MatcherAssert.assertThat(message, Matchers.not(Matchers.containsString("hunter2")));
      }
    }
  }

  @Test
  public void testParseFailureReportsReasonWithoutUrl()
  {
    // A URISyntaxException still yields an actionable message: the reason and offset, but not the input.
    final SQLException exception = Assertions.assertThrows(
        SQLException.class,
        () -> DruidConnectionUrl.parse(
            "jdbc:druid:http://localhost:8888/druid/v2/sql/?user=admin&password=hunter2|x",
            null
        )
    );

    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("Illegal character in query"));
    MatcherAssert.assertThat(exception.getMessage(), Matchers.containsString("at index["));
  }

  @Test
  public void testParseRejectsUserinfoAndDoesNotLeakIt()
  {
    final String[] urls = {
        "jdbc:druid:http://alice:s3cret@localhost:8888/druid/v2/sql/",
        "jdbc:druid:http://alice@localhost:8888/druid/v2/sql/"
    };

    for (final String jdbcUrl : urls) {
      final SQLException exception = assertParseFails(jdbcUrl, "userinfo in URL is not supported");
      MatcherAssert.assertThat(exception.getMessage(), Matchers.not(Matchers.containsString("alice")));
      MatcherAssert.assertThat(exception.getMessage(), Matchers.not(Matchers.containsString("s3cret")));
    }
  }
}
