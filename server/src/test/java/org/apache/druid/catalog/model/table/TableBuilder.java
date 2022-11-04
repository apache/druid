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
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

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
  private TableId id;
  private TableDefn defn;
  private String tableType;
  private Map<String, Object> properties = new HashMap<>();
  private List<ColumnSpec> columns = new ArrayList<>();

  public static TableBuilder datasource(String name, String granularity)
  {
    return new TableBuilder()
        .datasource(name)
        .type(DatasourceDefn.TABLE_TYPE)
        .segmentGranularity(granularity);
  }

  public static TableBuilder external(String type, String name)
  {
    return new TableBuilder()
        .external(name)
        .type(type);
  }

  public static TableBuilder updateFor(TableMetadata table)
  {
    return new TableBuilder()
        .id(table.id())
        .type(table.spec().type());
  }

  public static TableBuilder copyOf(TableMetadata table)
  {
    return copyOf(table.id(), table.spec());
  }

  public static TableBuilder copyOf(TableId newId, TableSpec from)
  {
    return new TableBuilder()
        .id(newId)
        .type(from.type())
        .properties(new HashMap<>(from.properties()))
        .columns(new ArrayList<>(from.columns()));
  }

  public static TableBuilder of(TableDefn defn)
  {
    TableBuilder builder = new TableBuilder();
    builder.defn = defn;
    builder.tableType = defn.typeValue();
    return builder;
  }

  public TableBuilder copy()
  {
    TableBuilder builder = new TableBuilder();
    builder.defn = defn;
    builder.tableType = tableType;
    builder.id = id;
    builder.properties.putAll(properties);
    builder.columns.addAll(columns);
    return builder;
  }

  public TableBuilder id(TableId id)
  {
    this.id = id;
    return this;
  }

  public TableBuilder datasource(String name)
  {
    this.id = TableId.datasource(name);
    return this;
  }

  public TableBuilder external(String name)
  {
    this.id = TableId.of(TableId.EXTERNAL_SCHEMA, name);
    return this;
  }

  public TableBuilder path(String schema, String name)
  {
    this.id = TableId.of(schema, name);
    return this;
  }

  public TableBuilder type(String tableType)
  {
    this.tableType = tableType;
    return this;
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
    return property(AbstractDatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, segmentGranularity);
  }

  public TableBuilder clusterColumns(ClusterKeySpec...clusterKeys)
  {
    return property(AbstractDatasourceDefn.CLUSTER_KEYS_PROPERTY, Arrays.asList(clusterKeys));
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
    return column(Columns.TIME_COLUMN, Columns.TIMESTAMP);
  }

  public TableBuilder column(String name, String sqlType)
  {
    Preconditions.checkNotNull(tableType);
    String colType;
    if (isInputTable(tableType)) {
      colType = ExternalTableDefn.EXTERNAL_COLUMN_TYPE;
    } else if (DatasourceDefn.TABLE_TYPE.equals(tableType)) {
      colType = DatasourceDefn.DatasourceColumnDefn.COLUMN_TYPE;
    } else {
      throw new ISE("Unknown table type: %s", tableType);
    }
    return column(colType, name, sqlType);
  }

  public static boolean isInputTable(String tableType)
  {
    switch (tableType) {
      case InlineTableDefn.TABLE_TYPE:
      case HttpTableDefn.TABLE_TYPE:
      case LocalTableDefn.TABLE_TYPE:
        return true;
      default:
        return false;
    }
  }

  public TableBuilder column(String colType, String name, String sqlType)
  {
    return column(new ColumnSpec(colType, name, sqlType, null));
  }

  public TableBuilder hiddenColumns(List<String> hiddenColumns)
  {
    return property(AbstractDatasourceDefn.HIDDEN_COLUMNS_PROPERTY, hiddenColumns);
  }

  public TableBuilder hiddenColumns(String...hiddenColumns)
  {
    return hiddenColumns(Arrays.asList(hiddenColumns));
  }

  public TableBuilder format(String format)
  {
    return property(FormattedExternalTableDefn.FORMAT_PROPERTY, format);
  }

  public TableBuilder data(List<String> data)
  {
    return property(InlineTableDefn.DATA_PROPERTY, data);
  }

  public TableBuilder data(String...data)
  {
    return data(Arrays.asList(data));
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
