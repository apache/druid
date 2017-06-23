/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

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
  private final Period pollPeriod;

  @JsonCreator
  public JdbcExtractionNamespace(
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @NotNull @JsonProperty(value = "table", required = true)
      final String table,
      @NotNull @JsonProperty(value = "keyColumn", required = true)
      final String keyColumn,
      @NotNull @JsonProperty(value = "valueColumn", required = true)
      final String valueColumn,
      @Nullable @JsonProperty(value = "tsColumn", required = false)
      final String tsColumn,
      @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false)
      final Period pollPeriod
  )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    this.table = Preconditions.checkNotNull(table, "table");
    this.keyColumn = Preconditions.checkNotNull(keyColumn, "keyColumn");
    this.valueColumn = Preconditions.checkNotNull(valueColumn, "valueColumn");
    this.tsColumn = tsColumn;
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
    return StringUtils.format(
        "JdbcExtractionNamespace = { connectorConfig = { %s }, table = %s, keyColumn = %s, valueColumn = %s, tsColumn = %s, pollPeriod = %s}",
        connectorConfig.toString(),
        table,
        keyColumn,
        valueColumn,
        tsColumn,
        pollPeriod
    );
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

    if (!connectorConfig.equals(that.connectorConfig)) {
      return false;
    }
    if (!table.equals(that.table)) {
      return false;
    }
    if (!keyColumn.equals(that.keyColumn)) {
      return false;
    }
    if (!valueColumn.equals(that.valueColumn)) {
      return false;
    }
    if (tsColumn != null ? !tsColumn.equals(that.tsColumn) : that.tsColumn != null) {
      return false;
    }
    return pollPeriod.equals(that.pollPeriod);

  }

  @Override
  public int hashCode()
  {
    int result = connectorConfig.hashCode();
    result = 31 * result + table.hashCode();
    result = 31 * result + keyColumn.hashCode();
    result = 31 * result + valueColumn.hashCode();
    result = 31 * result + (tsColumn != null ? tsColumn.hashCode() : 0);
    result = 31 * result + pollPeriod.hashCode();
    return result;
  }
}
