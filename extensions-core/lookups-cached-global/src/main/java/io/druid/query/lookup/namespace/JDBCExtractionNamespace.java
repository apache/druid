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
import com.google.common.collect.ImmutableList;
import io.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 *
 */
@JsonTypeName("jdbc")
public class JDBCExtractionNamespace extends ExtractionNamespace
{
  @JsonProperty
  private final MetadataStorageConnectorConfig connectorConfig;
  @JsonProperty
  private final String table;
  @JsonProperty
  private final List<KeyValueMap> maps;
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
      @JsonProperty(value = "keyColumn", required = true)
      final String keyColumn,
      @JsonProperty(value = "valueColumn", required = true)
      final String valueColumn,
      @JsonProperty(value = "maps", required = true)
      List<KeyValueMap> maps,
      @Nullable @JsonProperty(value = "tsColumn", required = false)
      final String tsColumn,
      @Min(0) @Nullable @JsonProperty(value = "pollPeriod", required = false)
      final Period pollPeriod
  )
  {
    this.connectorConfig = Preconditions.checkNotNull(connectorConfig, "connectorConfig");
    Preconditions.checkNotNull(connectorConfig.getConnectURI(), "connectorConfig.connectURI");
    this.table = Preconditions.checkNotNull(table, "table");
    Preconditions.checkArgument((keyColumn != null && valueColumn != null) || maps != null,
        "Either keyColumn & valueColumn or maps should be specified");
    this.maps = (maps != null) ? maps
                               : ImmutableList.of(new KeyValueMap(KeyValueMap.DEFAULT_MAPNAME, keyColumn, valueColumn));
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

  public List<KeyValueMap> getMaps()
  {
    return maps;
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
        "JDBCExtractionNamespace = { connectorConfig = { %s }, table = %s, key & value Columns = [%s], tsColumn = %s, pollPeriod = %s}",
        connectorConfig.toString(),
        table,
        StringUtils.join(maps, ','),
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
    if (!maps.equals(that.maps)) {
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
    result = 31 * result + maps.hashCode();
    result = 31 * result + (tsColumn != null ? tsColumn.hashCode() : 0);
    result = 31 * result + pollPeriod.hashCode();
    return result;
  }
}
