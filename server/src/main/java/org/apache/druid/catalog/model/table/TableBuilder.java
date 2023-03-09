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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Informal table spec builder for tests. Takes the tedium out of building
 * up property maps, column lists, and the various objects that define a
 * table. Not useful outside of tests since Druid code should not create
 * table objects except generically from specs provided by the user.
 */
public class TableBuilder
{
  private final TableId id;
  private final String tableType;
  private TableDefn defn;
  private Map<String, Object> properties = new HashMap<>();
  private List<ColumnSpec> columns = new ArrayList<>();

  public TableBuilder(TableId id, String tableType)
  {
    this.id = id;
    this.tableType = tableType;
  }

  public static TableBuilder datasource(String name, String granularity)
  {
    return new TableBuilder(
        TableId.datasource(name),
        DatasourceDefn.TABLE_TYPE
    ).segmentGranularity(granularity);
  }

  public static TableBuilder external(String name)
  {
    return new TableBuilder(
        TableId.of(TableId.EXTERNAL_SCHEMA, name),
        ExternalTableDefn.TABLE_TYPE
    );
  }

  public static TableBuilder updateFor(TableMetadata table)
  {
    return new TableBuilder(
        table.id(),
        table.spec().type()
    );
  }

  public static TableBuilder copyOf(TableMetadata table)
  {
    return copyOf(table.id(), table.spec());
  }

  public static TableBuilder copyOf(TableId newId, TableSpec from)
  {
    return new TableBuilder(newId, from.type())
        .properties(new HashMap<>(from.properties()))
        .columns(new ArrayList<>(from.columns()));
  }

  public static TableBuilder of(TableId id, TableDefn defn)
  {
    TableBuilder builder = new TableBuilder(id, defn.typeValue());
    builder.defn = defn;
    return builder;
  }

  public TableBuilder copy()
  {
    TableBuilder builder = new TableBuilder(id, tableType);
    builder.defn = defn;
    builder.properties.putAll(properties);
    builder.columns.addAll(columns);
    return builder;
  }

  public TableBuilder properties(Map<String, Object> properties)
  {
    this.properties = properties;
    return this;
  }

  public Map<String, Object> properties()
  {
    return properties;
  }

  public TableBuilder property(String key, Object value)
  {
    this.properties.put(key, value);
    return this;
  }

  public TableBuilder description(String description)
  {
    return property(TableDefn.DESCRIPTION_PROPERTY, description);
  }

  public TableBuilder segmentGranularity(String segmentGranularity)
  {
    return property(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, segmentGranularity);
  }

  public TableBuilder clusterColumns(ClusterKeySpec...clusterKeys)
  {
    return property(DatasourceDefn.CLUSTER_KEYS_PROPERTY, Arrays.asList(clusterKeys));
  }

  public TableBuilder hiddenColumns(List<String> hiddenColumns)
  {
    return property(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY, hiddenColumns);
  }

  public TableBuilder sealed(boolean sealed)
  {
    return property(DatasourceDefn.SEALED_PROPERTY, sealed);
  }

  public TableBuilder hiddenColumns(String...hiddenColumns)
  {
    return hiddenColumns(Arrays.asList(hiddenColumns));
  }

  public TableBuilder inputSource(Map<String, Object> inputSource)
  {
    return property(ExternalTableDefn.SOURCE_PROPERTY, inputSource);
  }

  public TableBuilder inputFormat(Map<String, Object> format)
  {
    return property(ExternalTableDefn.FORMAT_PROPERTY, format);
  }

  public TableBuilder columns(List<ColumnSpec> columns)
  {
    this.columns = columns;
    return this;
  }

  public List<ColumnSpec> columns()
  {
    return columns;
  }

  public TableBuilder column(ColumnSpec column)
  {
    if (Strings.isNullOrEmpty(column.name())) {
      throw new IAE("Column name is required");
    }
    columns.add(column);
    return this;
  }

  public TableBuilder timeColumn()
  {
    return column(Columns.TIME_COLUMN, Columns.LONG);
  }

  public TableBuilder column(String name, String sqlType)
  {
    return column(name, sqlType, null);
  }

  public TableBuilder column(String name, String sqlType, Map<String, Object> properties)
  {
    Preconditions.checkNotNull(tableType);
    return column(new ColumnSpec(name, sqlType, properties));
  }

  public TableSpec buildSpec()
  {
    return new TableSpec(tableType, properties, columns);
  }

  public TableMetadata build()
  {
    return TableMetadata.newTable(id, buildSpec());
  }

  public ResolvedTable buildResolved(ObjectMapper mapper)
  {
    Preconditions.checkNotNull(defn);
    return new ResolvedTable(defn, buildSpec(), mapper);
  }
}
