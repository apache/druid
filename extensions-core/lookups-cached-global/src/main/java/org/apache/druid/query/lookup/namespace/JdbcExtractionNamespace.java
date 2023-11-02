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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.utils.ConnectionUriUtils;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
@JsonTypeName("jdbc")
public class JdbcExtractionNamespace implements ExtractionNamespace
{
  private static final Logger LOG = new Logger(JdbcExtractionNamespace.class);

  long DEFAULT_MAX_HEAP_PERCENTAGE = 10L;
  long DEFAULT_LOOKUP_LOAD_TIME_SECONDS = 120;

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
  @JsonProperty
  private final long maxHeapPercentage;
  @JsonProperty
  private final long loadTimeoutSeconds;
  @JsonProperty
  private final int jitterSeconds;

  @JsonCreator
  public JdbcExtractionNamespace(
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @NotNull @JsonProperty(value = "table", required = true) final String table,
      @NotNull @JsonProperty(value = "keyColumn", required = true) final String keyColumn,
      @NotNull @JsonProperty(value = "valueColumn", required = true) final String valueColumn,
      @JsonProperty(value = "tsColumn") @Nullable final String tsColumn,
      @JsonProperty(value = "filter") @Nullable final String filter,
      @Min(0) @JsonProperty(value = "pollPeriod") @Nullable final Period pollPeriod,
      @JsonProperty(value = "maxHeapPercentage") @Nullable final Long maxHeapPercentage,
      @JsonProperty(value = "jitterSeconds") @Nullable Integer jitterSeconds,
      @JsonProperty(value = "loadTimeoutSeconds") @Nullable final Long loadTimeoutSeconds,
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
    if (pollPeriod == null) {
      // Warning because if JdbcExtractionNamespace is being used for lookups, any updates to the database will not
      // be picked up after the node starts. So for use cases where nodes start at different times (like streaming
      // ingestion with peons) there can be data inconsistencies across the cluster.
      LOG.warn("No pollPeriod configured for JdbcExtractionNamespace - entries will be loaded only once at startup");
      this.pollPeriod = new Period(0L);
    } else {
      this.pollPeriod = pollPeriod;
    }
    this.jitterSeconds = jitterSeconds == null ? 0 : jitterSeconds;
    this.maxHeapPercentage = maxHeapPercentage == null ? DEFAULT_MAX_HEAP_PERCENTAGE : maxHeapPercentage;
    this.loadTimeoutSeconds = loadTimeoutSeconds == null ? DEFAULT_LOOKUP_LOAD_TIME_SECONDS : loadTimeoutSeconds;
  }

  /**
   * Check the given URL whether it contains non-allowed properties.
   *
   * @see JdbcAccessSecurityConfig#getAllowedProperties()
   * @see ConnectionUriUtils#tryParseJdbcUriParameters(String, boolean)
   */
  private static void checkConnectionURL(String url, JdbcAccessSecurityConfig securityConfig)
  {
    Preconditions.checkNotNull(url, "connectorConfig.connectURI");

    if (!securityConfig.isEnforceAllowedProperties()) {
      // You don't want to do anything with properties.
      return;
    }

    ConnectionUriUtils.throwIfPropertiesAreNotAllowed(
        ConnectionUriUtils.tryParseJdbcUriParameters(url, securityConfig.isAllowUnknownJdbcUrlFormat()),
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
  public long getMaxHeapPercentage()
  {
    return maxHeapPercentage;
  }

  @Override
  public long getJitterMills()
  {
    if (jitterSeconds == 0) {
      return jitterSeconds;
    }
    return 1000L * ThreadLocalRandom.current().nextInt(jitterSeconds + 1);
  }

  @Override
  public long getLoadTimeoutMills()
  {
    return 1000L * loadTimeoutSeconds;
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
           ", jitterSeconds=" + jitterSeconds +
           ", loadTimeoutSeconds=" + loadTimeoutSeconds +
           ", maxHeapPercentage=" + maxHeapPercentage +
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
           Objects.equals(pollPeriod, that.pollPeriod) &&
           Objects.equals(maxHeapPercentage, that.maxHeapPercentage);
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
        pollPeriod,
        maxHeapPercentage
    );
  }
}
