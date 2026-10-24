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

import org.apache.druid.sql.calcite.table.DruidTable;

/**
 * Capability implemented by a traditional system table that can also be represented as a Druid
 * {@link org.apache.druid.query.SystemTableDataSource}. The capability is discovered from the table resolved through
 * {@link SystemSchemaProvider}, so datasource planning inherits the provider's table-visibility authorization.
 */
interface SystemTableDataSourceTable
{
  /**
   * Returns the representation used after the SQL engine selects system-table datasource planning. The returned table
   * supplies the {@code DataSource} and row signature needed to translate the Calcite relational plan. It does not read
   * rows itself; the selected query engine obtains them from the corresponding system-table data provider.
   */
  DruidTable asDataSourceTable();
}
