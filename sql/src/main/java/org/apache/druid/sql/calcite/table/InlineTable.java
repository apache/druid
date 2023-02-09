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
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.RowSignature;

/**
 * Represents a specialized table used within Druid's Calcite-based planner.
 * Used in {@link org.apache.druid.sql.calcite.rule.DruidLogicalValuesRule DruidLogicalValuesRule}
 * to implement trivial {@code SELECT 1} style queries that return constant values represented
 * by this inline table.
 */
public class InlineTable extends DruidTable
{
  private final DataSource dataSource;

  public InlineTable(
      final InlineDataSource dataSource,
      final RowSignature rowSignature
  )
  {
    super(rowSignature);
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
  }

  @Override
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @Override
  public boolean isJoinable()
  {
    return false;
  }

  @Override
  public boolean isBroadcast()
  {
    return false;
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable table)
  {
    return LogicalTableScan.create(context.getCluster(), table);
  }

  @Override
  public String toString()
  {
    return "DatasourceMetadata{" +
           "dataSource=" + dataSource +
           ", rowSignature=" + getRowSignature() +
           '}';
  }
}
