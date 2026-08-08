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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Write side of the catalog, the counterpart to the read-only {@link CatalogResolver}. Catalog metadata is owned by
 * the Coordinator, so an implementation of this interface makes a remote call; it is bound by the {@code druid-catalog}
 * extension and defaults to {@link #NOT_AVAILABLE} when that extension is absent.
 * <p>
 * The methods are deliberately semantic rather than a generic "apply this edit request", so that the extension's edit
 * request types need not be visible here. Each corresponds to exactly one atomic Coordinator operation, which is why
 * every DDL statement maps to a single call: the catalog stores properties and columns as separate blobs with separate
 * update paths, so a statement that changed both would not be atomic.
 */
public interface CatalogTableWriter
{
  CatalogTableWriter NOT_AVAILABLE = new UnavailableCatalogTableWriter();

  /**
   * Create a table.
   *
   * @param ifNotExists if the table already exists, do nothing rather than failing
   * @param replace     overwrite any existing spec for this table
   */
  void createTable(TableId tableId, TableSpec spec, boolean ifNotExists, boolean replace);

  /**
   * Append the given columns to the table's column list, failing if any of them already exists. Both this and
   * {@link #alterColumns} match columns by name; they differ only in which outcome is an error, and both decide that
   * inside the Coordinator's update transaction so that concurrent callers cannot both win.
   */
  void addColumns(TableId tableId, List<ColumnSpec> columns);

  /**
   * Update the given columns in place, failing if any of them does not already exist. See {@link #addColumns}.
   */
  void alterColumns(TableId tableId, List<ColumnSpec> columns);

  /**
   * Remove the named columns from the table's column list. Segments are unaffected.
   */
  void dropColumns(TableId tableId, List<String> columns);

  /**
   * Merge the given properties into the table's properties. A null value removes the property.
   */
  void updateProperties(TableId tableId, Map<String, Object> properties);

  /**
   * Append a projection to the table's projections.
   *
   * @param ifNotExists if a projection of the same name exists, do nothing rather than failing
   */
  void addProjection(TableId tableId, DatasourceProjectionMetadata projection, boolean ifNotExists);

  /**
   * Remove the named projection from the table's projections.
   *
   * @param ifExists if no projection of that name exists, do nothing rather than failing
   */
  void dropProjection(TableId tableId, String projectionName, boolean ifExists);

  /**
   * Read a table's current metadata directly from the Coordinator, bypassing any local cache, or null if the table
   * has no catalog entry. Used for pre-checks that must not race against a stale cache.
   */
  @Nullable
  TableMetadata readTable(TableId tableId);

  /**
   * Stand-in used when the {@code druid-catalog} extension is not loaded. Every operation fails with an explanation
   * rather than silently doing nothing.
   */
  class UnavailableCatalogTableWriter implements CatalogTableWriter
  {
    @Override
    public void createTable(TableId tableId, TableSpec spec, boolean ifNotExists, boolean replace)
    {
      throw notAvailable();
    }

    @Override
    public void addColumns(TableId tableId, List<ColumnSpec> columns)
    {
      throw notAvailable();
    }

    @Override
    public void alterColumns(TableId tableId, List<ColumnSpec> columns)
    {
      throw notAvailable();
    }

    @Override
    public void dropColumns(TableId tableId, List<String> columns)
    {
      throw notAvailable();
    }

    @Override
    public void updateProperties(TableId tableId, Map<String, Object> properties)
    {
      throw notAvailable();
    }

    @Override
    public void addProjection(TableId tableId, DatasourceProjectionMetadata projection, boolean ifNotExists)
    {
      throw notAvailable();
    }

    @Override
    public void dropProjection(TableId tableId, String projectionName, boolean ifExists)
    {
      throw notAvailable();
    }

    @Nullable
    @Override
    public TableMetadata readTable(TableId tableId)
    {
      throw notAvailable();
    }

    private static DruidException notAvailable()
    {
      return DruidException.forPersona(DruidException.Persona.USER)
                           .ofCategory(DruidException.Category.UNSUPPORTED)
                           .build(
                               "Catalog DDL statements require the [druid-catalog] extension, which is not loaded"
                           );
    }
  }
}
