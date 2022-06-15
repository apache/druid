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

package org.apache.druid.metadata.catalog;

import org.apache.druid.catalog.TableDefn;
import org.apache.druid.catalog.TableId;
import org.apache.druid.catalog.TableSpec;
import org.apache.druid.metadata.catalog.CatalogManager.DuplicateKeyException;
import org.apache.druid.metadata.catalog.CatalogManager.NotFoundException;
import org.apache.druid.metadata.catalog.CatalogManager.OutOfDateException;

import javax.annotation.Nullable;

import java.util.List;

/**
 * The Table Manager performs detailed CRUD operations on the
 * catalog tables table. Higher-level operations appear
 * elsewhere.
 */
public interface TableDefnManager
{
  interface Listener
  {
    void added(TableSpec table);
    void updated(TableSpec table);
    void deleted(TableId id);
  }

  void register(Listener listener);
  void createTableDefnTable();

  /**
   * Create a table entry.
   *
   * @return the version of the newly created table. Call
   * {@link TableSpec#asUpdate(long)} if you want a new
   * {@link TableSpec} with the new version.
   * @throws {@link DuplicateKeyException} if the row is a duplicate
   * (schema, name) pair. This generally indicates a code error,
   * or since our code is perfect, a race condition or a DB
   * update outside of Druid. In any event, the error is not
   * retryable: the user should pick another name, or update the
   * existing table
   */
  long create(TableSpec table) throws DuplicateKeyException;

  /**
   * Update a table definition, but only if the database entry is at
   * the given {@code oldVersion}.
   */
  long updateDefn(TableId id, TableDefn defn, long oldVersion) throws OutOfDateException;

  /**
   * Update a table definition, overwriting any current content.
   * This is a potential race conditions if this is a partial update
   * because of the possibility of another user doing an update since the
   * read. Fine when the goal is to replace the entire definition.
   */
  long updateDefn(TableId id, TableDefn defn) throws NotFoundException;

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
  @Nullable TableSpec read(TableId id);

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
  boolean delete(TableId id);

  List<TableId> list();
  List<String> list(String dbSchema);
  List<TableSpec> listDetails(String dbSchema);
}
