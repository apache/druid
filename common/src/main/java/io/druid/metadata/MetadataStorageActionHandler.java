/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.google.common.base.Optional;

import io.druid.java.util.common.Pair;

import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public interface MetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
{
  /**
   * Creates a new entry.
   * 
   * @param id entry id
   * @param timestamp timestamp this entry was created
   * @param dataSource datasource associated with this entry
   * @param entry object representing this entry
   * @param active active or inactive flag
   * @param status status object associated wit this object, can be null
   * @throws EntryExistsException
   */
  public void insert(
      @NotNull String id,
      @NotNull DateTime timestamp,
      @NotNull String dataSource,
      @NotNull EntryType entry,
      boolean active,
      @Nullable StatusType status
  ) throws EntryExistsException;


  /**
   * Sets or updates the status for any active entry with the given id.
   * Once an entry has been set inactive, its status cannot be updated anymore.
   *
   * @param entryId entry id
   * @param active active
   * @param status status
   * @return true if the status was updated, false if the entry did not exist of if the entry was inactive
   */
  public boolean setStatus(String entryId, boolean active, StatusType status);

  /**
   * Retrieves the entry with the given id.
   *
   * @param entryId entry id
   * @return optional entry, absent if the given id does not exist
   */
  public Optional<EntryType> getEntry(String entryId);

  /**
   * Retrieve the status for the entry with the given id.
   *
   * @param entryId entry id
   * @return optional status, absent if entry does not exist or status is not set
   */
  public Optional<StatusType> getStatus(String entryId);

  /**
   * Return all active entries with their respective status
   *
   * @return list of (entry, status) pairs
   */
  public List<Pair<EntryType, StatusType>> getActiveEntriesWithStatus();

  /**
   * Return all statuses for inactive entries created on or later than the given timestamp
   *
   * @param timestamp timestamp
   * @return list of statuses
   */
  public List<StatusType> getInactiveStatusesSince(DateTime timestamp);

  /**
   * Add a lock to the given entry
   *
   * @param entryId entry id
   * @param lock lock to add
   * @return true if the lock was added
   */
  public boolean addLock(String entryId, LockType lock);

  /**
   * Remove the lock with the given lock id.
   *
   * @param lockId lock id
   */
  public void removeLock(long lockId);

  /**
   * Add a log to the entry with the given id.
   *
   * @param entryId entry id
   * @param log log to add
   * @return true if the log was added
   */
  public boolean addLog(String entryId, LogType log);

  /**
   * Returns the logs for the entry with the given id.
   *
   * @param entryId entry id
   * @return list of logs
   */
  public List<LogType> getLogs(String entryId);

  /**
   * Returns the locks for the given entry
   *
   * @param entryId entry id
   * @return map of lockId to lock
   */
  public Map<Long, LockType> getLocks(String entryId);
}
