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

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Parses and validates Druid JDBC URLs.
 */
public class DruidConnectionUrl
{
  private final String scheme;
  private final String host;
  private final int port;
  private final String path;
  private final ClientProperties clientProperties;
  private final Map<String, String> queryContext;

  private DruidConnectionUrl(
      final String scheme,
      final String host,
      final int port,
      final String path,
      final ClientProperties clientProperties,
      final Map<String, String> queryContext
  )
  {
    this.scheme = scheme;
    this.host = host;
    this.port = port;
    this.path = path;
    this.clientProperties = clientProperties;
    this.queryContext = queryContext;
  }

  /**
   * Creates a DruidConnectionUrl object from JDBC URL and properties.
   *
   * <p>The URL format is: {@code jdbc:druid:http://host:port/path?param1=value1&param2=value2}
   *
   * @param url                  the JDBC URL string to parse, must start with "jdbc:druid:"
   * @param connectionProperties additional connection properties that will be merged with URL parameters
   *
   * @return a DruidConnectionUrl object containing the parsed components
   *
   * @throws SQLException if the URL is invalid or cannot be parsed
   */
  public static DruidConnectionUrl parse(final String url, final Properties connectionProperties) throws SQLException
  {
    if (url == null || !url.startsWith(DruidJdbcDriver.CONNECT_STRING_PREFIX)) {
      throw invalidUrl(StringUtils.format("Must start with[%s]", DruidJdbcDriver.CONNECT_STRING_PREFIX));
    }

    try {
      // Remove the jdbc:druid: prefix and parse as URI
      final String uriString = url.substring(DruidJdbcDriver.CONNECT_STRING_PREFIX.length());

      final URI uri = new URI(uriString);

      final String scheme = uri.getScheme();
      if (!"http".equalsIgnoreCase(scheme) && !isHttpsScheme(scheme)) {
        throw invalidUrl("Scheme must be 'http' or 'https'");
      }

      final String host = uri.getHost();
      if (host == null || host.isEmpty()) {
        throw invalidUrl("Host is required");
      }

      // Reject userinfo in the URL: credentials must come from connection properties or query parameters,
      // not as user:password@host. We deliberately do not echo the userinfo back in the message.
      if (uri.getUserInfo() != null) {
        throw invalidUrl("userinfo in URL is not supported; use user/password connection properties instead");
      }

      int port = uri.getPort();
      if (port == -1) {
        port = isHttpsScheme(scheme) ? 443 : 80;
      }

      final Map<String, String> allProperties = new HashMap<>();

      // Add JDBC connection properties.
      if (connectionProperties != null) {
        for (final String propertyName : connectionProperties.stringPropertyNames()) {
          allProperties.put(propertyName, connectionProperties.getProperty(propertyName));
        }
      }

      // Properties from URL have precedence over JDBC connection properties. Use getRawQuery(), not getQuery():
      // the latter is already decoded, so it would decode twice and split on encoded separators.
      allProperties.putAll(parseQueryParameters(uri.getRawQuery()));

      // Separate client-side properties from query context parameters.
      final ClientProperties.SplitProperties splitProperties = ClientProperties.splitProperties(allProperties);

      return new DruidConnectionUrl(
          scheme,
          host,
          port,
          uri.getPath(),
          splitProperties.clientProperties(),
          splitProperties.queryContext()
      );
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Exception e) {
      throw invalidUrl(e);
    }
  }

  public String getScheme()
  {
    return scheme;
  }

  public boolean isHttps()
  {
    return isHttpsScheme(scheme);
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  public String getPath()
  {
    return path;
  }

  public ClientProperties getClientProperties()
  {
    return clientProperties;
  }

  public Map<String, String> getQueryContext()
  {
    return queryContext;
  }

  public String buildHttpUrl()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append(scheme).append("://").append(host);

    // Only include port if it's not the default port for the scheme.
    final boolean isDefaultPort = (isHttps() && port == 443) || (!isHttps() && port == 80);
    if (!isDefaultPort) {
      sb.append(":").append(port);
    }

    sb.append(path);
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DruidConnectionUrl that = (DruidConnectionUrl) o;
    return port == that.port
           && Objects.equals(scheme, that.scheme)
           && Objects.equals(host, that.host)
           && Objects.equals(path, that.path)
           && Objects.equals(clientProperties, that.clientProperties)
           && Objects.equals(queryContext, that.queryContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(scheme, host, port, path, clientProperties, queryContext);
  }

  @Override
  public String toString()
  {
    return "DruidConnectionUrl{" +
           "scheme='" + scheme + '\'' +
           ", host='" + host + '\'' +
           ", port=" + port +
           ", path='" + path + '\'' +
           ", clientProperties=" + clientProperties +
           ", queryContext=" + queryContext +
           '}';
  }

  private static boolean isHttpsScheme(final String scheme)
  {
    return "https".equalsIgnoreCase(scheme);
  }

  private static SQLException invalidUrl(final String reason)
  {
    return new DruidJdbcException("Invalid JDBC URL. %s", reason);
  }

  /**
   * Builds the error for a URL that could not be parsed. Deliberately does not include the actual URL or the
   * exception cause, because the URL may contain credentials.
   */
  private static SQLException invalidUrl(final Throwable cause)
  {
    if (cause instanceof URISyntaxException e) {
      return new DruidJdbcException("Invalid JDBC URL: %s at index[%s]", e.getReason(), e.getIndex());
    } else {
      return new DruidJdbcException("Invalid JDBC URL");
    }
  }

  private static Map<String, String> parseQueryParameters(final String query)
  {
    if (query == null) {
      return Collections.emptyMap();
    }
    final Map<String, String> map = new HashMap<>();
    final String[] pairs = query.split("&");
    for (final String pair : pairs) {
      final int idx = pair.indexOf('=');
      if (idx > 0) {
        final String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
        final String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
        map.put(key, value);
      } else if (!pair.isEmpty()) {
        final String key = URLDecoder.decode(pair, StandardCharsets.UTF_8);
        map.put(key, "");
      }
    }
    return map;
  }
}
