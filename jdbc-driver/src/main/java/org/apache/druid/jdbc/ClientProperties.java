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

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Handles the client-side JDBC connection properties that are interpreted by the driver,
 * rather than being sent to the Druid server as query context parameters.
 */
public class ClientProperties
{
  public static final String AUTHENTICATION = "authentication";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String VERIFY_TLS = "verifyTls";
  public static final boolean DEFAULT_VERIFY_TLS = true;

  /**
   * Standard HTTP basic authentication: {@link #USER} and {@link #PASSWORD} joined by a colon, then base64-encoded.
   */
  public static final String AUTHENTICATION_BASIC = "basic";

  /**
   * Sends {@link #PASSWORD} verbatim as the credential, with no encoding.
   */
  public static final String AUTHENTICATION_BASIC_RAW = "basicRaw";

  /**
   * Set of all recognized client-side property names. All others are sent to the Druid server.
   */
  private static final Set<String> CLIENT_SIDE_PROPERTY_NAMES = Set.of(
      AUTHENTICATION,
      USER,
      PASSWORD,
      VERIFY_TLS
  );

  @Nullable
  private final String authentication;
  @Nullable
  private final String user;
  @Nullable
  private final String password;
  private final boolean verifyTls;

  private ClientProperties(
      @Nullable final String authentication,
      @Nullable final String user,
      @Nullable final String password,
      final boolean verifyTls
  )
  {
    this.authentication = authentication;
    this.user = user;
    this.password = password;
    this.verifyTls = verifyTls;
  }

  /**
   * Separates client-side properties from query context parameters.
   */
  public static SplitProperties splitProperties(final Map<String, String> allProperties) throws SQLException
  {
    final Map<String, String> clientProps = new HashMap<>();
    final Map<String, String> queryContext = new HashMap<>();

    for (final Map.Entry<String, String> entry : allProperties.entrySet()) {
      final String key = entry.getKey();
      final String value = entry.getValue();
      if (CLIENT_SIDE_PROPERTY_NAMES.contains(key)) {
        clientProps.put(key, value);
      } else {
        queryContext.put(key, value);
      }
    }

    final ClientProperties clientProperties = new ClientProperties(
        getAuthentication(clientProps),
        clientProps.get(USER),
        clientProps.get(PASSWORD),
        getVerifyTls(clientProps)
    );

    return new SplitProperties(clientProperties, Collections.unmodifiableMap(queryContext));
  }

  @Nullable
  public String getAuthentication()
  {
    return authentication;
  }

  @Nullable
  public String getUser()
  {
    return user;
  }

  @Nullable
  public String getPassword()
  {
    return password;
  }

  public boolean isVerifyTls()
  {
    return verifyTls;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ClientProperties that = (ClientProperties) o;
    return verifyTls == that.verifyTls
           && Objects.equals(authentication, that.authentication)
           && Objects.equals(user, that.user)
           && Objects.equals(password, that.password);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(authentication, user, password, verifyTls);
  }

  @Override
  public String toString()
  {
    return "ClientProperties{" +
           "authentication='" + authentication + '\'' +
           ", user='" + user + '\'' +
           ", password='[REDACTED]'" +
           ", verifyTls=" + verifyTls +
           '}';
  }

  /**
   * Returns the value of {@link #AUTHENTICATION} to use. If not provided by the caller, defaults to
   * {@link #AUTHENTICATION_BASIC} if {@link #USER} or {@link #PASSWORD} is set, or {@code null} otherwise.
   */
  @Nullable
  private static String getAuthentication(final Map<String, String> clientProps)
  {
    final String value = clientProps.get(AUTHENTICATION);

    if (value != null) {
      return value;
    } else if (clientProps.get(USER) != null || clientProps.get(PASSWORD) != null) {
      return AUTHENTICATION_BASIC;
    } else {
      return null;
    }
  }

  /**
   * Returns the value of {@link #VERIFY_TLS}, or {@link #DEFAULT_VERIFY_TLS} if unset.
   *
   * @throws SQLException if the value is not a valid boolean string
   */
  private static boolean getVerifyTls(final Map<String, String> clientProps) throws SQLException
  {
    final String value = clientProps.get(VERIFY_TLS);
    if (value == null) {
      return DEFAULT_VERIFY_TLS;
    }

    if (BooleanUtils.isBooleanTrue(value)) {
      return true;
    } else if (BooleanUtils.isBooleanFalse(value)) {
      return false;
    } else {
      throw new DruidJdbcException(
          "Invalid value[%s] for property[%s], must be 'true' or 'false'.", value, VERIFY_TLS
      );
    }
  }

  /**
   * Result of separating client-side properties from query context parameters.
   */
  public record SplitProperties(ClientProperties clientProperties, Map<String, String> queryContext)
  {
  }
}
