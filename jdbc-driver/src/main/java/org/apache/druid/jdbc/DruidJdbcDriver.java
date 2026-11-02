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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jdbc.http.DruidHttpClient;
import org.apache.druid.jdbc.http.DruidHttpClientImpl;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class DruidJdbcDriver implements Driver
{
  public static final String CONNECT_STRING_PREFIX = "jdbc:druid:";

  /**
   * Fallback version string used when the implementation version cannot be read from the JAR manifest.
   * This is an arbitrary recent-ish Druid version and carries no special meaning.
   */
  public static final String FALLBACK_VERSION = "36.0.0";

  private static final String VERSION;
  private static final int MAJOR_VERSION;
  private static final int MINOR_VERSION;

  static {
    final String implVersion = DruidJdbcDriver.class.getPackage().getImplementationVersion();
    VERSION = implVersion != null ? implVersion : FALLBACK_VERSION;
    MAJOR_VERSION = parseMajorVersion(VERSION);
    MINOR_VERSION = parseMinorVersion(VERSION);

    try {
      DriverManager.registerDriver(new DruidJdbcDriver());
    }
    catch (SQLException e) {
      throw new RuntimeException("Failed to register Druid JDBC driver", e);
    }
  }

  public static String getVersion()
  {
    return VERSION;
  }

  public static int getStaticMajorVersion()
  {
    return MAJOR_VERSION;
  }

  public static int getStaticMinorVersion()
  {
    return MINOR_VERSION;
  }

  @Override
  @Nullable
  public Connection connect(final String url, final Properties info) throws SQLException
  {
    if (!acceptsURL(url)) {
      return null;
    }

    final DruidConnectionUrl connectionUrl = DruidConnectionUrl.parse(url, info);
    final ObjectMapper jsonMapper = new ObjectMapper();
    final DruidHttpClient httpClient = new DruidHttpClientImpl(connectionUrl, jsonMapper);
    final DruidConnection connection = new DruidConnection(connectionUrl, httpClient, jsonMapper);
    return validateOrClose(connection);
  }

  @Override
  public boolean acceptsURL(final String url)
  {
    return url != null && url.startsWith(CONNECT_STRING_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info)
  {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion()
  {
    return MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion()
  {
    return MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant()
  {
    return false;
  }

  @Override
  public Logger getParentLogger()
  {
    return Logger.getLogger(getClass().getPackageName());
  }

  /**
   * Parses the major version (first dot-separated segment) from a version string.
   * For example, "38.0.0-SNAPSHOT" returns 38. Returns 0 if the version string
   * cannot be parsed.
   */
  static int parseMajorVersion(final String version)
  {
    final String segment = firstSegment(version);
    try {
      return Integer.parseInt(segment);
    }
    catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Parses the minor version (second dot-separated segment) from a version string.
   * For example, "38.0.0-SNAPSHOT" returns 0. Returns 0 if the version string has
   * no second segment or cannot be parsed.
   */
  static int parseMinorVersion(final String version)
  {
    final int firstDot = version.indexOf('.');
    if (firstDot < 0 || firstDot == version.length() - 1) {
      return 0;
    }

    final String rest = version.substring(firstDot + 1);
    final String segment = firstSegment(rest);
    try {
      return Integer.parseInt(segment);
    }
    catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Runs the connection validation query and returns the connection if it succeeds. Otherwise, closes
   * the connection.
   */
  static Connection validateOrClose(final DruidConnection connection) throws SQLException
  {
    try {
      connection.runValidationQuery(0);
      return connection;
    }
    catch (Throwable e) {
      try {
        connection.close();
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  /**
   * Returns the portion of a version string up to the first '.' or '-', whichever comes first.
   */
  private static String firstSegment(final String s)
  {
    int end = s.length();
    final int dot = s.indexOf('.');
    if (dot >= 0) {
      end = dot;
    }
    final int dash = s.indexOf('-');
    if (dash >= 0 && dash < end) {
      end = dash;
    }
    return s.substring(0, end);
  }
}
