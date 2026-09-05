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

package org.apache.druid.sql.calcite.schema;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.Schema;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.server.system.table.TaskTableDescriptor;
import org.apache.druid.sql.calcite.table.DruidTable;

/** Native-query representation of {@code sys.tasks}. */
class NativeTasksTable extends DruidTable
{
  private static final DataSource DATA_SOURCE = new SystemTableDataSource("tasks");

  NativeTasksTable()
  {
    super(TaskTableDescriptor.ROW_SIGNATURE);
  }

  @Override
  public DataSource getDataSource()
  {
    return DATA_SOURCE;
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
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.SYSTEM_TABLE;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table)
  {
    return LogicalTableScan.create(context.getCluster(), table, context.getTableHints());
  }
}
