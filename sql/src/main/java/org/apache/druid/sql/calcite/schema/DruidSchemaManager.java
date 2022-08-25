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

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.Map;
import java.util.Set;

/**
 * This interface provides a map of datasource names to {@link DruidTable}
 * objects, used by the {@link DruidSchema} class as the SQL planner's
 * view of Druid datasource schemas. If a non-default implementation is
 * provided, the segment metadata polling-based view of the Druid tables
 * will not be built in DruidSchema.
 */
@ExtensionPoint
@UnstableApi
public interface DruidSchemaManager
{
  /**
   * Return all tables known to this schema manager. Deprecated because getting
   * all tables is never actually needed in the current code. Calcite asks for
   * the information for a single table (via {@link #getTable(String)}, or
   * the list of all table <i>names</i> (via {@link #getTableNames()}. This
   * method was originally used to allow Calcite's {@link
   * org.apache.calcite.schema.impl.AbstractSchema AbstractSchema} class to do
   * the lookup. The current code implements the operations directly. For
   * backward compatibility, the default methods emulate the old behavior.
   * Newer implementations should omit this method and implement the other two.
   */
  @Deprecated
  Map<String, DruidTable> getTables();

  default DruidTable getTable(String name)
  {
    return getTables().get(name);
  }

  default Set<String> getTableNames()
  {
    return getTables().keySet();
  }
}
