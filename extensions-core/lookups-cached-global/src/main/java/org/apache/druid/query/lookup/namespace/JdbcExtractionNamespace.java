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

package org.apache.druid.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.mysql.jdbc.NonRegisteringDriver;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.utils.ConnectionUriUtils;
import org.apache.druid.utils.Throwables;
import org.joda.time.Period;
import org.postgresql.Driver;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 *
 */
@JsonTypeName("jdbc")
public class JdbcExtractionNamespace implements ExtractionNamespace
{
  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final String keyColumn;
  @JsonProperty
  private final String valueColumn;
  @JsonProperty
  private final String tsColumn;
  @JsonProperty
  private final String filter;
  @JsonProperty
  private final Period pollPeriod;

  @JsonCreator
  public JdbcExtractionNamespace(
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @NotNull @JsonProperty(value = "table", required = true) final String table,
      @NotNull @JsonProperty(value = "keyColumn", required = true) final String keyColumn,
      @NotNull @JsonProperty(value = "valueColumn", required = true) final String valueColumn,
      @JsonProperty(value = "tsColumn", required = false) @Nullable final String tsColumn,
      @JsonProperty(value = "filter", required = false) @Nullable final String filter,
      @Min(0) @JsonProperty(value = "pollPeriod", required = false) @Nullable final Period pollPeriod,
      @JacksonInject JdbcAccessSecurityConfig securityConfig
  )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    // Check the properties in the connection URL. Note that JdbcExtractionNamespace doesn't use
    // MetadataStorageConnectorConfig.getDbcpProperties(). If we want to use them,
    // those DBCP properties should be validated using the same logic.
    checkConnectionURL(connectorConfig.getConnectURI(), securityConfig);
    this.table = Preconditions.checkNotNull(table, "table");
    this.keyColumn = Preconditions.checkNotNull(keyColumn, "keyColumn");
    this.valueColumn = Preconditions.checkNotNull(valueColumn, "valueColumn");
    this.tsColumn = tsColumn;
    this.filter = filter;
    this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
  }

  /**
   * Check the given URL whether it contains non-allowed properties.
   *
   * This method should be in sync with the following methods:
   *
   * - {@code org.apache.druid.server.lookup.jdbc.JdbcDataFetcher.checkConnectionURL()}
   * - {@code org.apache.druid.firehose.sql.MySQLFirehoseDatabaseConnector.findPropertyKeysFromConnectURL()}
   * - {@code org.apache.druid.firehose.sql.PostgresqlFirehoseDatabaseConnector.findPropertyKeysFromConnectURL()}
   *
   * @see JdbcAccessSecurityConfig#getAllowedProperties()
   */
  private static void checkConnectionURL(String url, JdbcAccessSecurityConfig securityConfig)
  {
    Preconditions.checkNotNull(url, "connectorConfig.connectURI");

    if (!securityConfig.isEnforceAllowedProperties()) {
      // You don't want to do anything with properties.
      return;
    }

    @Nullable final Properties properties; // null when url has an invalid format

    if (url.startsWith(ConnectionUriUtils.MYSQL_PREFIX)) {
      try {
        NonRegisteringDriver driver = new NonRegisteringDriver();
        properties = driver.parseURL(url, null);
      }
      catch (SQLException e) {
        throw new RuntimeException(e);
      }
      catch (Throwable e) {
        if (Throwables.isThrowable(e, NoClassDefFoundError.class)
            || Throwables.isThrowable(e, ClassNotFoundException.class)) {
          if (e.getMessage().contains("com/mysql/jdbc/NonRegisteringDriver")) {
            throw new RuntimeException(
                "Failed to find MySQL driver class. Please check the MySQL connector version 5.1.48 is in the classpath",
                e
            );
          }
        }
        throw new RuntimeException(e);
      }
    } else if (url.startsWith(ConnectionUriUtils.POSTGRES_PREFIX)) {
      try {
        properties = Driver.parseURL(url, null);
      }
      catch (Throwable e) {
        if (Throwables.isThrowable(e, NoClassDefFoundError.class)
            || Throwables.isThrowable(e, ClassNotFoundException.class)) {
          if (e.getMessage().contains("org/postgresql/Driver")) {
            throw new RuntimeException(
                "Failed to find PostgreSQL driver class. "
                + "Please check the PostgreSQL connector version 42.2.14 is in the classpath",
                e
            );
          }
        }
        throw new RuntimeException(e);
      }
    } else {
      if (securityConfig.isAllowUnknownJdbcUrlFormat()) {
        properties = new Properties();
      } else {
        // unknown format but it is not allowed
        throw new IAE("Unknown JDBC connection scheme: %s", url.split(":")[1]);
      }
    }

    if (properties == null) {
      // There is something wrong with the URL format.
      throw new IAE("Invalid URL format [%s]", url);
    }

    final Set<String> propertyKeys = Sets.newHashSetWithExpectedSize(properties.size());
    properties.forEach((k, v) -> propertyKeys.add((String) k));

    ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
        propertyKeys,
        securityConfig.getSystemPropertyPrefixes(),
        securityConfig.getAllowedProperties()
    );
  }

  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  public String getTable()
  {
    return table;
  }

  public String getKeyColumn()
  {
    return keyColumn;
  }

  public String getValueColumn()
  {
    return valueColumn;
  }

  public String getFilter()
  {
    return filter;
  }

  public String getTsColumn()
  {
    return tsColumn;
  }

  @Override
  public long getPollMs()
  {
    return pollPeriod.toStandardDuration().getMillis();
  }

  @Override
  public String toString()
  {
    return "JdbcExtractionNamespace{" +
           "connectorConfig=" + connectorConfig +
           ", table='" + table + '\'' +
           ", keyColumn='" + keyColumn + '\'' +
           ", valueColumn='" + valueColumn + '\'' +
           ", tsColumn='" + tsColumn + '\'' +
           ", filter='" + filter + '\'' +
           ", pollPeriod=" + pollPeriod +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JdbcExtractionNamespace that = (JdbcExtractionNamespace) o;

    return Objects.equals(connectorConfig, that.connectorConfig) &&
           Objects.equals(table, that.table) &&
           Objects.equals(filter, that.filter) &&
           Objects.equals(keyColumn, that.keyColumn) &&
           Objects.equals(valueColumn, that.valueColumn) &&
           Objects.equals(tsColumn, that.tsColumn) &&
           Objects.equals(pollPeriod, that.pollPeriod);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        connectorConfig,
        table,
        filter,
        keyColumn,
        valueColumn,
        tsColumn,
        pollPeriod
    );
  }
}
