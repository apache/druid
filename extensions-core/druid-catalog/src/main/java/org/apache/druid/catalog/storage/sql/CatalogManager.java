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

package org.apache.druid.catalog.storage.sql;

import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.CatalogException.DuplicateKeyException;
import org.apache.druid.catalog.CatalogException.NotFoundException;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.sync.CatalogUpdateListener;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Manages catalog data. Used in Coordinator, which will be in either
 * an leader or standby state. The Coordinator calls the {@link #start()}
 * method when it becomes the leader.
 *
 * Performs detailed CRUD operations on the catalog tables table.
 * Higher-level operations appear elsewhere.
 */
public interface CatalogManager
{
  /**
   * Perform a transform (edit) of the table metadata. The metadata
   * argument provided contains the table ID and the portion of the
   * spec requested for the given operation: either properties or
   * columns.
   * <p>
   * The return value is either the updated spec to be written to the
   * DB, or {@link null}, which indicates that no change was needed.
   * When non-null, the spec will contain a revised version of either
   * the columns or the properties, as determined by the specific operation.
   */
  interface TableTransform
  {
    @Nullable TableSpec apply(TableMetadata metadata) throws CatalogException;
  }

  /**
   * Start the catalog manager within a Druid run. Called from lifecycle
   * management and when a coordinator becomes the leader node.
   */
  void start();

  /**
   * Register a listener for catalog events.
   */
  void register(CatalogUpdateListener listener);

  /**
   * Create a table entry.
   *
   * @return the version of the newly created table. Call
   * {@link TableMetadata#asUpdate(long)} if you want a new
   * {@link TableMetadata} with the new version.
   * @throws {@link DuplicateKeyException} if the row is a duplicate
   * (schema, name) pair. This generally indicates a code error,
   * or since our code is perfect, a race condition or a DB
   * update outside of Druid. In any event, the error is not
   * retryable: the user should pick another name, or update the
   * existing table
   */
  long create(TableMetadata table) throws DuplicateKeyException;

  /**
   * Update a table definition.
   * <p>
   * The table must be at the {@code oldVersion}. Use this for optimistic-locking
   * style updates.
   *
   * @throws NotFoundException if either the table does not exist, the table is
   * not in the active state, or the version does not match. If the exception
   * is thrown, the application should re-request the data, which will reveal
   * the actual state at that moment
   */
  long update(TableMetadata table, long oldVersion) throws NotFoundException;

  /**
   * Replace a table definition.
   * <p>
   * Use this when the desire to replace whatever exists with the new information,
   * such as configuration-as-code style updates.
   */
  long replace(TableMetadata table) throws NotFoundException;

  /**
   * Update the table properties incrementally using the transform provided. Performs the update
   * in a transaction to ensure the read and write are atomic.
   *
   * @param id        the table to update
   * @param transform the transform to apply to the table properties
   * @return          the update timestamp (version) of the updated record, or 0 if
   *                  the transform returns {@code null}, which indicates no change
   *                  is needed
   */
  long updateProperties(TableId id, TableTransform transform) throws CatalogException;

  /**
   * Update the table columns incrementally using the transform provided. Performs the update
   * in a transaction to ensure the read and write are atomic.
   *
   * @param id        the table to update
   * @param transform the transform to apply to the table columns
   * @return          the update timestamp (version) of the updated record, or 0 if
   *                  the transform returns {@code null}, which indicates no change
   *                  is needed
   */
  long updateColumns(TableId id, TableTransform transform) throws CatalogException;

  /**
   * Move the table to the deleting state. No version check: fine
   * if the table is already in the deleting state. Does nothing if the
   * table does not exist.
   *
   * @return new table update timestamp, or 0 if the table does not
   * exist
   */
  long markDeleting(TableId id);

  /**
   * Read the table record for the given ID.
   *
   * @return the table record, or {@code null} if the entry is not
   * found in the DB.
   */
  TableMetadata read(TableId id) throws NotFoundException;

  /**
   * Delete the table record for the given ID. Essentially does a
   * "DELETE IF EXISTS". There is no version check. Delete should be
   * called only when there are no segments left for the table: use
   * {@link #markDeleting(TableId)} to indicates that the segments are
   * being deleted. Call this method after deletion is complete.
   * <p>
   * Does not cascade deletes yet. Eventually, should delete all entries
   * for the table.
   *
   * @return {@code true} if the table exists and was deleted,
   * {@code false} if the table did not exist.
   */
  void delete(TableId id) throws NotFoundException;

  /**
   * Returns a list of the ids (schema, name) of all tables in the catalog.
   */
  List<TableId> allTablePaths();

  /**
   * Returns a list of the names of all tables within the given schema.
   */
  List<String> tableNamesInSchema(String dbSchema);

  /**
   * Returns a list of the table metadata for all tables within the given
   * schema.
   */
  List<TableMetadata> tablesInSchema(String dbSchema);
}
