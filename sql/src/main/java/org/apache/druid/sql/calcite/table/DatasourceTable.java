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
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.DataSourceInformation;

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

  private final PhysicalDatasourceMetadata physicalMetadata;

  public DatasourceTable(
      final PhysicalDatasourceMetadata physicalMetadata
  )
  {
    super(physicalMetadata.getRowSignature());
    this.physicalMetadata = physicalMetadata;
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
    // physicalMetadata.
    return "DruidTable{" +
           physicalMetadata +
           '}';
  }
}
