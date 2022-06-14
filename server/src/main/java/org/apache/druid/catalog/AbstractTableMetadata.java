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

package org.apache.druid.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.catalog.AbstractColumnMetadata.DimensionColumn;
import org.apache.druid.catalog.AbstractColumnMetadata.InputColumn;
import org.apache.druid.catalog.AbstractColumnMetadata.MeasureColumn;
import org.apache.druid.catalog.AbstractColumnMetadata.SimpleColumn;
import org.apache.druid.catalog.MetadataCatalog.ColumnMetadata;
import org.apache.druid.catalog.MetadataCatalog.DatasourceMetadata;
import org.apache.druid.catalog.MetadataCatalog.InputSourceMetadata;
import org.apache.druid.catalog.MetadataCatalog.TableMetadata;
import org.apache.druid.catalog.MetadataCatalog.TableType;
import org.apache.druid.catalog.SchemaRegistry.SchemaDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractTableMetadata implements TableMetadata
{
  public static class DatasourceTable extends AbstractTableMetadata implements DatasourceMetadata
  {
    private final String segmentGranularity;
    private final String rollupGranularity;

    @JsonCreator
    public DatasourceTable(
        @JsonProperty("id") TableId id,
        @JsonProperty("updateTime") long updateTime,
        @JsonProperty("segmentGranularity") String segmentGranularity,
        @JsonProperty("rollupGranularity") String rollupGranularity,
        @JsonProperty("columns") List<ColumnMetadata> columns)
    {
      super(id, updateTime, columns);
      this.segmentGranularity = segmentGranularity;
      this.rollupGranularity = rollupGranularity;
    }

    public DatasourceTable(TableId id, long updateTime, DatasourceDefn defn)
    {
      super(id, updateTime, convertColums(defn));
      this.segmentGranularity = defn.segmentGranularity();
      this.rollupGranularity = defn.rollupGranularity();
    }

    private static List<ColumnMetadata> convertColums(DatasourceDefn defn)
    {
      boolean isRollup = defn.isRollupTable();
      List<ColumnMetadata> converted = new ArrayList<>();
      for (ColumnDefn col : defn.columns()) {
        ColumnMetadata mdCol;
        if (col instanceof MeasureColumnDefn) {
          MeasureColumnDefn measureDefn = (MeasureColumnDefn) col;
          mdCol = new MeasureColumn(col.name(), col.sqlType(), measureDefn.aggregateFn());
        } else if (isRollup) {
          mdCol = new DimensionColumn(col.name(), col.sqlType());
        } else {
          mdCol = new SimpleColumn(col.name(), col.sqlType());
        }
        converted.add(mdCol);
      }
      return converted;
    }

    @Override
    public TableType type()
    {
      return TableType.DATASOURCE;
    }

    @Override
    @JsonProperty("segmentGranularity")
    public String segmentGranularity()
    {
      return segmentGranularity;
    }

    @Override
    @JsonIgnore
    public boolean isRollup()
    {
      return !isDetail();
    }

    @Override
    @JsonIgnore
    public boolean isDetail()
    {
      return rollupGranularity == null;
    }

    @Override
    @JsonProperty("rollupGranularity")
    public String rollupGranularity()
    {
      return rollupGranularity;
    }
  }

  public static class InputSourceTable extends AbstractTableMetadata implements InputSourceMetadata
  {
    private final InputSource inputSource;
    private final InputFormat format;

    @JsonCreator
    public InputSourceTable(
        @JsonProperty("id") TableId id,
        @JsonProperty("updateTime") long updateTime,
        @JsonProperty("inputSource") InputSource inputSource,
        @JsonProperty("format") InputFormat format,
        @JsonProperty("columns") List<ColumnMetadata> columns
    )
    {
      super(id, updateTime, columns);
      this.inputSource = inputSource;
      this.format = format;
    }

    public InputSourceTable(TableId id, long updateTime, InputSourceDefn defn)
    {
      super(id, updateTime, convertColums(defn));
      this.inputSource = defn.inputSource();
      this.format = defn.format();
    }

    private static List<ColumnMetadata> convertColums(InputSourceDefn defn)
    {
      List<ColumnMetadata> converted = new ArrayList<>();
      for (ColumnDefn col : defn.columns()) {
        converted.add(new InputColumn(col.name(), col.sqlType()));
      }
      return converted;
    }

    @Override
    public TableType type()
    {
      return TableType.INPUT;
    }

    @JsonProperty("inputSource")
    public InputSource inputSource()
    {
      return inputSource;
    }

    @JsonProperty("format")
    public InputFormat format()
    {
      return format;
    }

    public RowSignature rowSignature()
    {
      RowSignature.Builder builder = RowSignature.builder();
      for (ColumnMetadata col : columns) {
        builder.add(col.name(), ((InputColumn) col).druidType());
      }
      return builder.build();
    }
  }

  protected final TableId id;
  private final long updateTime;
  protected final List<ColumnMetadata> columns;
  private final Map<String, ColumnMetadata> columnIndex = new HashMap<>();

  public AbstractTableMetadata(
      TableId id,
      long updateTime,
      List<ColumnMetadata> columns
  )
  {
    this.id = id;
    this.updateTime = updateTime;
    this.columns = columns;
    for (ColumnMetadata col : columns) {
      columnIndex.put(col.name(), col);
    }
  }

  @Override
  @JsonProperty("id")
  public TableId id()
  {
    return id;
  }

  @Override
  @JsonProperty("updateTime")
  public long updateTime()
  {
    return updateTime;
  }

  @Override
  @JsonProperty("columns")
  public List<ColumnMetadata> columns()
  {
    return columns;
  }

  @Override
  public ColumnMetadata column(String name)
  {
    return columnIndex.get(name);
  }

  public static TableMetadata fromCatalogTable(SchemaDefn schema, TableSpec table)
  {
    return create(schema, table.id(), table.updateTime(), table.defn());
  }

  public static TableMetadata create(
      SchemaDefn schema,
      TableId id,
      long updateTime,
      TableDefn defn)
  {
    if (defn == null) {
      // Useless metadata: adds no information. Should not occur.
      return null;
    }
    TableType tableType = schema.tableType();
    if (tableType == null) {
      if (defn instanceof DatasourceDefn) {
        tableType = TableType.DATASOURCE;
      } else if (defn instanceof InputSourceDefn) {
        tableType = TableType.INPUT;
      } else {
        // TODO: other types
        return null;
      }
    }
    switch (tableType) {
      case DATASOURCE:
        if (!(defn instanceof DatasourceDefn)) {
          // Wrong type. Too late to fix it now. Ignore it.
          return null;
        }
        return new DatasourceTable(id, updateTime, (DatasourceDefn) defn);
      case INPUT:
        if (!(defn instanceof InputSourceDefn)) {
          return null;
        }
        return new InputSourceTable(id, updateTime, (InputSourceDefn) defn);
      case VIEW:
        // Not yet
        return null;
      default:
        // Don't know what this is, so we don't know how to use it.
        // Ignore it.
        return null;
    }
  }
}
