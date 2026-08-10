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
 * Capability implemented by a traditional system table that also has a native-query representation.
 * The capability is discovered from the table resolved through {@link SystemSchemaProvider}, so native planning
 * inherits the provider's table-visibility authorization.
 */
interface NativeSystemTable
{
  /**
   * Returns the representation used by the SQL planner after native system-table planning has been selected.
   * The returned table supplies the native {@code DataSource} and row signature needed to translate the Calcite
   * relational plan into a native Druid query. It does not read the system-table rows itself; those rows are supplied
   * by the corresponding component-side system-table data provider when the native query executes.
   */
  DruidTable asNativeTable();
}
