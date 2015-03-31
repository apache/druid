/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.metadata.MetadataStorageConnectorConfig;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

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
  private final String keyColumn;
  @JsonProperty
  private final String valueColumn;
  @JsonProperty
  private final String tsColumn;
  @JsonProperty
  private final String namespace;

  @JsonCreator
  public JDBCExtractionNamespace(
      @NotNull @JsonProperty(value = "namespace", required = true)
      final String namespace,
      @NotNull @JsonProperty(value = "connectorConfig", required = true)
      final MetadataStorageConnectorConfig connectorConfig,
      @NotNull @JsonProperty(value = "table", required = true)
      final String table,
      @NotNull @JsonProperty(value = "keyColumn", required = true)
      final String keyColumn,
      @NotNull @JsonProperty(value = "valueColumn", required = true)
      final String valueColumn,
      @Nullable @JsonProperty(value = "tsColumn", required = false)
      final String tsColumn
  )
  {
    Preconditions.checkNotNull(connectorConfig);
    Preconditions.checkNotNull(connectorConfig.getConnectURI()); // Should never be null
    Preconditions.checkNotNull(table);
    Preconditions.checkNotNull(keyColumn);
    Preconditions.checkNotNull(valueColumn);
    this.connectorConfig = connectorConfig;
    this.table = table;
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
    this.tsColumn = tsColumn;
    this.namespace = namespace;
  }

  @Override
  public String getNamespace()
  {
    return namespace;
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
  public String toString(){
    return String.format("JDBCExtractionNamespace = { namespace = %s, connectorConfig = { %s }, table = %s, keyColumn = %s, valueColumn = %s, tsColumn = %s}",
                  namespace, connectorConfig.toString(), table, keyColumn, valueColumn, tsColumn
    );
  }
}
