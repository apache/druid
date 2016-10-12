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
import com.google.common.collect.Lists;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
@JsonTypeName("jdbc")
public class JDBCExtractionNamespace implements ExtractionNamespace
{
  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final List<String> keyColumns;
  @JsonProperty
  private final String valueColumn;
  @JsonProperty
  private final String tsColumn;
  @JsonProperty
  private final Period pollPeriod;

  @JsonCreator
  public JDBCExtractionNamespace(
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @NotNull @JsonProperty(value = "table", required = true)
      final String table,
      @NotNull @JsonProperty(value = "keyColumns", required = true)
      final List<String> keyColumns,
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
    this.keyColumns = Preconditions.checkNotNull(keyColumns, "keyColumns");
    this.valueColumn = Preconditions.checkNotNull(valueColumn, "valueColumn");
    this.tsColumn = tsColumn;
    this.pollPeriod = pollPeriod == null ? new Period(0L) : pollPeriod;
  }

  public JDBCExtractionNamespace(
      final MetadataStorageConnectorConfig connectorConfig,
      final String table,
      final String keyColumn,
      final String valueColumn,
      final String tsColumn,
      final Period pollPeriod
  )
  {
    this(connectorConfig, table, keyColumn == null ? null: Lists.newArrayList(keyColumn), valueColumn, tsColumn, pollPeriod);
  }

  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  public String getTable()
  {
    return table;
  }

  public List<String> getKeyColumns()
  {
    return keyColumns;
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
    return String.format(
        "JDBCExtractionNamespace = { connectorConfig = { %s }, table = %s, keyColumns = %s, valueColumn = %s, tsColumn = %s, pollPeriod = %s}",
        connectorConfig.toString(),
        table,
        Arrays.toString(keyColumns.toArray()),
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

    JDBCExtractionNamespace that = (JDBCExtractionNamespace) o;

    if (!connectorConfig.equals(that.connectorConfig)) {
      return false;
    }
    if (!table.equals(that.table)) {
      return false;
    }
    if (!keyColumns.equals(that.keyColumns)) {
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
    result = 31 * result + keyColumns.hashCode();
    result = 31 * result + valueColumn.hashCode();
    result = 31 * result + (tsColumn != null ? tsColumn.hashCode() : 0);
    result = 31 * result + pollPeriod.hashCode();
    return result;
  }
}
