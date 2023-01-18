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

package org.apache.druid.catalog.model.facade;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TypeParser;
import org.apache.druid.catalog.model.TypeParser.ParsedType;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Convenience wrapper on top of a resolved table (a table spec and its corresponding
 * definition.) To be used by consumers of catalog objects that work with specific
 * datasource properties rather than layers that work with specs generically.
 */
public class DatasourceFacade extends TableFacade
{
  private static final Logger LOG = new Logger(DatasourceFacade.class);

  public static class ColumnFacade
  {
    public enum Kind
    {
      ANY,
      TIME,
      DIMENSION,
      MEASURE
    }

    private final ColumnSpec spec;
    private final ParsedType type;

    public ColumnFacade(ColumnSpec spec)
    {
      this.spec = spec;
      if (Columns.isTimeColumn(spec.name()) && spec.sqlType() == null) {
        // For __time only, force a type if type is null.
        this.type = TypeParser.TIME_TYPE;
      } else {
        this.type = TypeParser.parse(spec.sqlType());
      }
    }

    public ColumnSpec spec()
    {
      return spec;
    }

    public ParsedType type()
    {
      return type;
    }

    public boolean hasType()
    {
      return type.kind() != ParsedType.Kind.ANY;
    }

    public boolean isTime()
    {
      return type.kind() == ParsedType.Kind.TIME;
    }

    public boolean isMeasure()
    {
      return type.kind() == ParsedType.Kind.MEASURE;
    }

    public ColumnType druidType()
    {
      switch (type.kind()) {
        case DIMENSION:
          return Columns.druidType(spec.sqlType());
        case TIME:
          return ColumnType.LONG;
        case MEASURE:
          return type.measure().storageType;
        default:
          return null;
      }
    }

    public String sqlStorageType()
    {
      if (isTime()) {
        // Time is special: its storage type is BIGINT, but SQL requires the
        // type of TIMESTAMP to allow insertion validation.
        return Columns.TIMESTAMP;
      } else {
        return Columns.sqlType(druidType());
      }
    }

    @Override
    public String toString()
    {
      return "{spec=" + spec + ", type=" + type + "}";
    }
  }

  private final List<ColumnFacade> columns;
  private final Map<String, ColumnFacade> columnIndex;
  private final boolean hasRollup;

  public DatasourceFacade(ResolvedTable resolved)
  {
    super(resolved);
    this.columns = resolved.spec().columns()
        .stream()
        .map(col -> new ColumnFacade(col))
        .collect(Collectors.toList());
    boolean hasMeasure = false;
    for (ColumnFacade col : columns) {
      if (col.isMeasure()) {
        hasMeasure = true;
        break;
      }
    }
    this.hasRollup = hasMeasure;
    ImmutableMap.Builder<String, ColumnFacade> builder = ImmutableMap.builder();
    for (ColumnFacade col : columns) {
      builder.put(col.spec.name(), col);
    }
    columnIndex = builder.build();
  }

  public String segmentGranularityString()
  {
    return stringProperty(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY);
  }

  public Granularity segmentGranularity()
  {
    String definedGranularity = segmentGranularityString();
    return definedGranularity == null ? null : CatalogUtils.asDruidGranularity(definedGranularity);
  }

  public Integer targetSegmentRows()
  {
    return intProperty(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY);
  }

  public List<ClusterKeySpec> clusterKeys()
  {
    Object value = property(DatasourceDefn.CLUSTER_KEYS_PROPERTY);
    if (value == null) {
      return Collections.emptyList();
    }
    try {
      return jsonMapper().convertValue(value, ClusterKeySpec.CLUSTER_KEY_LIST_TYPE_REF);
    }
    catch (Exception e) {
      LOG.error("Failed to convert a catalog %s property of value [%s]",
          DatasourceDefn.CLUSTER_KEYS_PROPERTY,
          value
      );
      return Collections.emptyList();
    }
  }

  @SuppressWarnings("unchecked")
  public List<String> hiddenColumns()
  {
    Object value = property(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY);
    return value == null ? Collections.emptyList() : (List<String>) value;
  }

  public boolean isSealed()
  {
    return booleanProperty(DatasourceDefn.SEALED_PROPERTY);
  }

  public List<ColumnFacade> columnFacades()
  {
    return columns;
  }

  public ColumnFacade column(String name)
  {
    return columnIndex.get(name);
  }

  public boolean hasRollup()
  {
    return hasRollup;
  }

  public String rollupGrain()
  {
    ColumnFacade col = columnIndex.get(Columns.TIME_COLUMN);
    return col == null ? null : col.type().timeGrain();
  }
}
