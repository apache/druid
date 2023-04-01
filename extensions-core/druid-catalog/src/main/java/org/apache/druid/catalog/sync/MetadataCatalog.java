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

package org.apache.druid.catalog.sync;

import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Client view of the metadata catalog. Implementations can be local
 * (with the DB on the same node), or remote (if the DB is on another
 * node.) Any caching that is desired is done behind this interface.
 * <p>
 * This interface <i>does not</i> interpolate physical data from
 * segments. That work is done by a layer on top of this one: a
 * layer which also has visibility to the segment caching logic.
 */
public interface MetadataCatalog
{
  interface CatalogSource
  {
    List<TableMetadata> tablesForSchema(String dbSchema);
    TableMetadata table(TableId id);
    ResolvedTable resolveTable(TableId id);
  }

  interface CatalogUpdateProvider
  {
    void register(CatalogUpdateListener listener);
  }

  /**
   * Resolves a table given a {@link TableId} with the schema and
   * table name. Does not do security checks: the caller is responsible.
   *
   * @return the table metadata, if any exists, else {@code null} if
   * no metadata is available. Note that a datasource can exist without
   * metadata. Views and input sources exist <i>only</i> if their
   * metadata exists. System tables never have metadata.
   */
  @Nullable TableMetadata getTable(TableId tableId);
  @Nullable ResolvedTable resolveTable(TableId tableId);

  /**
   * List of tables defined within the given schema. Does not filter the
   * tables by permissions: the caller is responsible for that.
   *
   * @param schemaName
   * @return
   */
  List<TableMetadata> tables(String schemaName);

  Set<String> tableNames(String schemaName);
}
