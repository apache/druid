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

import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Function;

/**
 * Manages catalog data. Used in Coordinator, which will be in either
 * an leader or standby state. The Coordinator calls the {@link #start()}
 * method when it becomes the leader, and calls {@link #stop()} when
 * it loses leadership, or shuts down.
 *
 * Performs detailed CRUD operations on the catalog tables table.
 * Higher-level operations appear elsewhere.
 */
public interface CatalogManager
{
  /**
   * Thrown with an "optimistic lock" fails: the version of a
   * catalog object being updated is not the same as that of
   * the expected version.
   */
  class OutOfDateException extends Exception
  {
    public OutOfDateException(String msg)
    {
      super(msg);
    }
  }

  class NotFoundException extends Exception
  {
    public NotFoundException(String msg)
    {
      super(msg);
    }
  }

  /**
   * Indicates an attempt to insert a duplicate key into a table.
   * This could indicate a logic error, or a race condition. It is
   * generally not retryable: it us unrealistic to expect the other
   * thread to helpfully delete the record it just added.
   */
  class DuplicateKeyException extends Exception
  {
    public DuplicateKeyException(String msg, Exception e)
    {
      super(msg, e);
    }
  }

  interface Listener
  {
    void added(TableMetadata table);
    void updated(TableMetadata table);
    void deleted(TableId id);
  }

  void start();


  void register(Listener listener);
  void createTableDefnTable();

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
   * If {@code oldVersion == 0}, overwrites any current content.
   * This is a potential race conditions if this is a partial update
   * because of the possibility of another user doing an update since the
   * read. Fine when the goal is to replace the entire definition.
   * Else, only does the update is at the given version.
   * <p>
   * Retryable only if the version is given, and an
   * {@code OutOfDateException} is thrown.
   */
  long update(TableMetadata table, long oldVersion) throws OutOfDateException, NotFoundException;

  /**
   * Update the table spec incrementally using the transform provided. Performs the update
   * in a transaction to ensure the read and write are atomic.
   *
   * @param id        the table to update
   * @param transform the transform to apply to the table spec
   * @return          the update timestamp (version) of the updated record
   */
  long updatePayload(TableId id, Function<TableSpec, TableSpec> transform) throws NotFoundException;

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
  @Nullable TableMetadata read(TableId id);

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
  List<TableMetadata> listDetails(String dbSchema);

  void stop();
}
