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

package org.apache.druid.sql.calcite.table;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.DataSourceInformation;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a SQL table that models a Druid datasource.
 * <p>
 * Once the catalog code is merged, this class will combine physical information
 * from the segment cache with logical information from the catalog to produce
 * the SQL-user's view of the table. The resulting merged view is used to plan
 * queries and is the source of table information in the {@code INFORMATION_SCHEMA}.
 */
public class DatasourceTable extends DruidTable
{
  /**
   * The physical metadata for a datasource, derived from the list of segments
   * published in the Coordinator. Used only for datasources, since only
   * datasources are computed from segments.
   */
  public static class PhysicalDatasourceMetadata extends DataSourceInformation
  {
    private final TableDataSource tableDataSource;
    private final boolean joinable;
    private final boolean broadcast;

    public PhysicalDatasourceMetadata(
        final TableDataSource tableDataSource,
        final RowSignature rowSignature,
        final boolean isJoinable,
        final boolean isBroadcast
    )
    {
      super(tableDataSource.getName(), rowSignature);
      this.tableDataSource = Preconditions.checkNotNull(tableDataSource, "dataSource");
      this.joinable = isJoinable;
      this.broadcast = isBroadcast;
    }

    public TableDataSource dataSource()
    {
      return tableDataSource;
    }

    public boolean isJoinable()
    {
      return joinable;
    }

    public boolean isBroadcast()
    {
      return broadcast;
    }

    public Map<String, EffectiveColumnMetadata> toEffectiveColumns()
    {
      Map<String, EffectiveColumnMetadata> columns = new HashMap<>();
      for (int i = 0; i < getRowSignature().size(); i++) {
        String colName = getRowSignature().getColumnName(i);
        ColumnType colType = getRowSignature().getColumnType(i).get();

        EffectiveColumnMetadata colMetadata = EffectiveColumnMetadata.fromPhysical(colName, colType);
        columns.put(colName, colMetadata);
      }
      return columns;
    }

    public EffectiveMetadata toEffectiveMetadata()
    {
      return new EffectiveMetadata(null, toEffectiveColumns(), false);
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
      if (!super.equals(o)) {
        return false;
      }

      PhysicalDatasourceMetadata that = (PhysicalDatasourceMetadata) o;

      return Objects.equals(tableDataSource, that.tableDataSource);
    }

    @Override
    public int hashCode()
    {
      int result = tableDataSource != null ? tableDataSource.hashCode() : 0;
      result = 31 * result + super.hashCode();
      return result;
    }

    @Override
    public String toString()
    {
      return "DatasourceMetadata{" +
             "dataSource=" + tableDataSource +
             ", rowSignature=" + getRowSignature() +
             '}';
    }
  }

  public static class EffectiveColumnMetadata
  {
    protected final String name;
    protected final ColumnType type;

    public EffectiveColumnMetadata(String name, ColumnType type)
    {
      this.name = name;
      this.type = type;
    }

    public String name()
    {
      return name;
    }

    public ColumnType druidType()
    {
      return type;
    }

    public static EffectiveColumnMetadata fromPhysical(String name, ColumnType type)
    {
      return new EffectiveColumnMetadata(name, type);
    }

    @Override
    public String toString()
    {
      return "Column{" +
             "name=" + name +
             ", type=" + type.asTypeString() +
             "}";
    }
  }

  public static class EffectiveMetadata
  {
    private final DatasourceFacade catalogMetadata;
    private final boolean isEmpty;
    private final Map<String, EffectiveColumnMetadata> columns;

    public EffectiveMetadata(
        final DatasourceFacade catalogMetadata,
        final Map<String, EffectiveColumnMetadata> columns,
        final boolean isEmpty
    )
    {
      this.catalogMetadata = catalogMetadata;
      this.isEmpty = isEmpty;
      this.columns = columns;
    }

    public DatasourceFacade catalogMetadata()
    {
      return catalogMetadata;
    }

    public EffectiveColumnMetadata column(String name)
    {
      return columns.get(name);
    }

    public boolean isEmpty()
    {
      return isEmpty;
    }

    @Override
    public String toString()
    {
      return getClass().getSimpleName() + "{" +
             "empty=" + isEmpty +
             ", columns=" + columns +
             "}";
    }
  }

  private final PhysicalDatasourceMetadata physicalMetadata;
  private final EffectiveMetadata effectiveMetadata;

  public DatasourceTable(
      final PhysicalDatasourceMetadata physicalMetadata
  )
  {
    this(
        physicalMetadata.getRowSignature(),
        physicalMetadata,
        physicalMetadata.toEffectiveMetadata()
    );
  }

  public DatasourceTable(
      final RowSignature rowSignature,
      final PhysicalDatasourceMetadata physicalMetadata,
      final EffectiveMetadata effectiveMetadata
  )
  {
    super(rowSignature);
    this.physicalMetadata = physicalMetadata;
    this.effectiveMetadata = effectiveMetadata;
  }

  @Override
  public DataSource getDataSource()
  {
    return physicalMetadata.dataSource();
  }

  @Override
  public boolean isJoinable()
  {
    return physicalMetadata.isJoinable();
  }

  @Override
  public boolean isBroadcast()
  {
    return physicalMetadata.isBroadcast();
  }

  public EffectiveMetadata effectiveMetadata()
  {
    return effectiveMetadata;
  }

  @Override
  public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable table)
  {
    return LogicalTableScan.create(context.getCluster(), table, context.getTableHints());
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
    DatasourceTable that = (DatasourceTable) o;

    if (!Objects.equals(physicalMetadata, that.physicalMetadata)) {
      return false;
    }
    return Objects.equals(getRowSignature(), that.getRowSignature());
  }

  @Override
  public int hashCode()
  {
    return physicalMetadata.hashCode();
  }

  @Override
  public String toString()
  {
    // Don't include the row signature: it is the same as in
    // effectiveMetadata.
    return "DruidTable{physicalMetadata=" +
           (physicalMetadata == null ? "null" : physicalMetadata.toString()) +
           ", effectiveMetadata=" + effectiveMetadata +
           '}';
  }
}
