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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Objects;

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
      @Min(0) @JsonProperty(value = "pollPeriod", required = false) @Nullable final Period pollPeriod
  )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    this.table = Preconditions.checkNotNull(table, "table");
    this.keyColumn = Preconditions.checkNotNull(keyColumn, "keyColumn");
    this.valueColumn = Preconditions.checkNotNull(valueColumn, "valueColumn");
    this.tsColumn = tsColumn;
    this.filter = filter;
    this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
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
