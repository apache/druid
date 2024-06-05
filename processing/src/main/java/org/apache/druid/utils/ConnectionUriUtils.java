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

package org.apache.druid.utils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.IAE;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public final class ConnectionUriUtils
{
  private static final String MARIADB_EXTRAS = "nonMappedOptions";
  // Note: MySQL JDBC connector 8 supports 7 other protocols than just `jdbc:mysql:`
  // (https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html).
  // We should consider either expanding recognized mysql protocols or restricting allowed protocols to
  // just a basic one.
  public static final String MYSQL_PREFIX = "jdbc:mysql:";
  public static final String POSTGRES_PREFIX = "jdbc:postgresql:";
  public static final String MARIADB_PREFIX = "jdbc:mariadb:";

  public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
  public static final String MYSQL_CONNECTION_URL = "com.mysql.cj.conf.ConnectionUrl";
  public static final String MYSQL_HOST_INFO = "com.mysql.cj.conf.HostInfo";

  /**
   * This method checks {@param actualProperties} against {@param allowedProperties} if they are not system properties.
   * A property is regarded as a system property if its name starts with a prefix in {@param systemPropertyPrefixes}.
   * See org.apache.druid.server.initialization.JDBCAccessSecurityConfig for more details.
   * <p>
   * If a non-system property that is not allowed is found, this method throws an {@link IllegalArgumentException}.
   */
  public static void throwIfPropertiesAreNotAllowed(
      Set<String> actualProperties,
      Set<String> systemPropertyPrefixes,
      Set<String> allowedProperties
  )
  {
    for (String property : actualProperties) {
      if (systemPropertyPrefixes.stream().noneMatch(property::startsWith)) {
        Preconditions.checkArgument(
            allowedProperties.contains(property),
            "The property [%s] is not in the allowed list %s",
            property,
            allowedProperties
        );
      }
    }
  }

  /**
   * This method tries to determine the correct type of database for a given JDBC connection string URI, then load the
   * driver using reflection to parse the uri parameters, returning the set of keys which can be used for JDBC
   * parameter whitelist validation.
   * <p>
   * uris starting with {@link #MYSQL_PREFIX} will first try to use the MySQL Connector/J driver (5.x), then fallback
   * to MariaDB Connector/J (version 2.x) which also accepts jdbc:mysql uris. This method does not attempt to use
   * MariaDB Connector/J 3.x alpha driver (at the time of these javadocs, it only handles the jdbc:mariadb prefix)
   * <p>
   * uris starting with {@link #POSTGRES_PREFIX} will use the postgresql driver to parse the uri
   * <p>
   * uris starting with {@link #MARIADB_PREFIX} will first try to use MariaDB Connector/J driver (2.x) then fallback to
   * MariaDB Connector/J 3.x driver.
   * <p>
   * If the uri does not match any of these schemes, this method will return an empty set if unknown uris are allowed,
   * or throw an exception if not.
   */
  public static Set<String> tryParseJdbcUriParameters(String connectionUri, boolean allowUnknown)
  {
    if (connectionUri.startsWith(MYSQL_PREFIX)) {
      try {
        return tryParseMySqlConnectionUri(connectionUri);
      }
      catch (ClassNotFoundException notFoundMysql) {
        try {
          return tryParseMariaDb2xConnectionUri(connectionUri);
        }
        catch (ClassNotFoundException notFoundMaria2x) {
          throw new RuntimeException(
              "Failed to find MySQL driver class. Please check the MySQL connector version 8.2.0 is in the classpath",
              notFoundMysql
          );
        }
        catch (IllegalArgumentException iaeMaria2x) {
          throw iaeMaria2x;
        }
        catch (Throwable otherMaria2x) {
          throw new RuntimeException(otherMaria2x);
        }
      }
      catch (IllegalArgumentException iaeMySql) {
        throw iaeMySql;
      }
      catch (Throwable otherMysql) {
        throw new RuntimeException(otherMysql);
      }
    } else if (connectionUri.startsWith(MARIADB_PREFIX)) {
      try {
        return tryParseMariaDb2xConnectionUri(connectionUri);
      }
      catch (ClassNotFoundException notFoundMaria2x) {
        try {
          return tryParseMariaDb3xConnectionUri(connectionUri);
        }
        catch (ClassNotFoundException notFoundMaria3x) {
          throw new RuntimeException(
              "Failed to find MariaDB driver class. Please check the MariaDB connector version 2.7.3 is in the classpath",
              notFoundMaria2x
          );
        }
        catch (IllegalArgumentException iaeMaria3x) {
          throw iaeMaria3x;
        }
        catch (Throwable otherMaria3x) {
          throw new RuntimeException(otherMaria3x);
        }
      }
      catch (IllegalArgumentException iaeMaria2x) {
        throw iaeMaria2x;
      }
      catch (Throwable otherMaria2x) {
        throw new RuntimeException(otherMaria2x);
      }
    } else if (connectionUri.startsWith(POSTGRES_PREFIX)) {
      try {
        return tryParsePostgresConnectionUri(connectionUri);
      }
      catch (IllegalArgumentException iaePostgres) {
        throw iaePostgres;
      }
      catch (Throwable otherPostgres) {
        // no special handling for class not found because postgres driver is in distribution and should be available.
        throw new RuntimeException(otherPostgres);
      }
    } else {
      if (!allowUnknown) {
        throw new IAE("Unknown JDBC connection scheme: %s", connectionUri.split(":")[1]);
      }
      return Collections.emptySet();
    }
  }

  public static Set<String> tryParsePostgresConnectionUri(String connectionUri)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    Class<?> driverClass = Class.forName(POSTGRES_DRIVER);
    Method parseUrl = driverClass.getMethod("parseURL", String.class, Properties.class);
    Properties properties = (Properties) parseUrl.invoke(null, connectionUri, null);
    if (properties == null) {
      throw new IAE("Invalid URL format for PostgreSQL: [%s]", connectionUri);
    }
    Set<String> keys = Sets.newHashSetWithExpectedSize(properties.size());
    properties.forEach((k, v) -> keys.add((String) k));
    return keys;
  }

  public static Set<String> tryParseMySqlConnectionUri(String connectionUri)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
             InvocationTargetException
  {
    Class<?> connectionUrlClass = Class.forName(MYSQL_CONNECTION_URL);
    Method isConnectionStringSupported = connectionUrlClass.getMethod("acceptsUrl", String.class);
    if (!(boolean) isConnectionStringSupported.invoke(connectionUrlClass, connectionUri)) {
      throw new IAE("Invalid URL format for MySQL: [%s]", connectionUri);
    }
    Method getConnectionUrlInstanceMethod = connectionUrlClass.getMethod("getConnectionUrlInstance", String.class, Properties.class);
    Object conUrl = getConnectionUrlInstanceMethod.invoke(connectionUrlClass, connectionUri, null);
    Method getHostsList = connectionUrlClass.getMethod("getHostsList");
    return getKeysFromOptions(getPropertiesFromHosts((List<?>) getHostsList.invoke(conUrl)));
  }

  private static Properties getPropertiesFromHosts(List<?> hostsList)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException
  {
    Properties properties = new Properties();
    Class<?> hostInfoClass = Class.forName(MYSQL_HOST_INFO);
    for (Object host : hostsList) {
      Method getHostMethod = hostInfoClass.getMethod("getHost");
      String hostName = (String) getHostMethod.invoke(host);
      if (hostName != null) {
        properties.setProperty("HOST", hostName);
      }

      Method getPortMethod = hostInfoClass.getMethod("getPort");
      Integer port = (Integer) getPortMethod.invoke(host);
      if (port != null) {
        properties.setProperty("PORT", String.valueOf(port));
      }

      Method getUserMethod = hostInfoClass.getMethod("getUser");
      String user = (String) getUserMethod.invoke(host);
      if (user != null) {
        properties.setProperty("user", user);
      }

      Method getPasswordMethod = hostInfoClass.getMethod("getPassword");
      String password = (String) getPasswordMethod.invoke(host);
      if (password != null) {
        properties.setProperty("password", password);
      }

      Method getHostPropertiesMethod = hostInfoClass.getMethod("getHostProperties");
      Map<String, String> hostProperties =
          (Map<String, String>) getHostPropertiesMethod.invoke(host);
      if (hostProperties != null) {
        for (Map.Entry<String, String> entry : hostProperties.entrySet()) {
          if (entry.getKey() != null && entry.getValue() != null) {
            properties.setProperty(entry.getKey(), entry.getValue());
          }
        }
      }
    }
    return properties;
  }

  private static Set<String> getKeysFromOptions(Properties properties)
  {
    Set<String> keys = Sets.newHashSetWithExpectedSize(properties.size());
    properties.forEach((k, v) -> keys.add((String) k));
    return keys;
  }

  public static Set<String> tryParseMariaDb2xConnectionUri(String connectionUri)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
             NoSuchFieldException, InstantiationException
  {
    // these are a bit more complicated
    Class<?> urlParserClass = Class.forName("org.mariadb.jdbc.UrlParser");
    Class<?> optionsClass = Class.forName("org.mariadb.jdbc.util.Options");
    Method parseUrl = urlParserClass.getMethod("parse", String.class);
    Method getOptions = urlParserClass.getMethod("getOptions");

    Object urlParser = parseUrl.invoke(null, connectionUri);

    if (urlParser == null) {
      throw new IAE("Invalid URL format for MariaDB: [%s]", connectionUri);
    }

    Object options = getOptions.invoke(urlParser);
    Field nonMappedOptionsField = optionsClass.getField(MARIADB_EXTRAS);
    Properties properties = (Properties) nonMappedOptionsField.get(options);

    Field[] fields = optionsClass.getDeclaredFields();
    Set<String> keys = Sets.newHashSetWithExpectedSize(properties.size() + fields.length);
    properties.forEach((k, v) -> keys.add((String) k));

    Object defaultOptions = optionsClass.getConstructor().newInstance();
    for (Field field : fields) {
      if (field.getName().equals(MARIADB_EXTRAS)) {
        continue;
      }
      try {
        if (!Objects.equal(field.get(options), field.get(defaultOptions))) {
          keys.add(field.getName());
        }
      }
      catch (IllegalAccessException ignored) {
        // ignore stuff we aren't allowed to read
      }
    }

    return keys;
  }

  public static Set<String> tryParseMariaDb3xConnectionUri(String connectionUri)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
             InstantiationException
  {
    Class<?> configurationClass = Class.forName("org.mariadb.jdbc.Configuration");
    Class<?> configurationBuilderClass = Class.forName("org.mariadb.jdbc.Configuration$Builder");
    Method parseUrl = configurationClass.getMethod("parse", String.class);
    Method buildMethod = configurationBuilderClass.getMethod("build");
    Object configuration = parseUrl.invoke(null, connectionUri);

    if (configuration == null) {
      throw new IAE("Invalid URL format for MariaDB: [%s]", connectionUri);
    }

    Method nonMappedOptionsGetter = configurationClass.getMethod(MARIADB_EXTRAS);
    Properties properties = (Properties) nonMappedOptionsGetter.invoke(configuration);

    Field[] fields = configurationClass.getDeclaredFields();
    Set<String> keys = Sets.newHashSetWithExpectedSize(properties.size() + fields.length);
    properties.forEach((k, v) -> keys.add((String) k));

    Object defaultConfiguration = buildMethod.invoke(configurationBuilderClass.getConstructor().newInstance());
    for (Field field : fields) {
      if (field.getName().equals(MARIADB_EXTRAS)) {
        continue;
      }
      try {
        final Method fieldGetter = configurationClass.getMethod(field.getName());
        if (!Objects.equal(fieldGetter.invoke(configuration), fieldGetter.invoke(defaultConfiguration))) {
          keys.add(field.getName());
        }
      }
      catch (IllegalAccessException | NoSuchMethodException ignored) {
        // ignore stuff we aren't allowed to read
      }
    }

    return keys;
  }

  private ConnectionUriUtils()
  {
  }
}
