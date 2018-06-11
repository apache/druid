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
  void insert(
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
  boolean setStatus(String entryId, boolean active, StatusType status);

  /**
   * Retrieves the entry with the given id.
   *
   * @param entryId entry id
   * @return optional entry, absent if the given id does not exist
   */
  Optional<EntryType> getEntry(String entryId);

  /**
   * Retrieve the status for the entry with the given id.
   *
   * @param entryId entry id
   * @return optional status, absent if entry does not exist or status is not set
   */
  Optional<StatusType> getStatus(String entryId);

  /**
   * Return all active entries with their respective status
   *
   * @return list of (entry, status) pairs
   */
  List<Pair<EntryType, StatusType>> getActiveEntriesWithStatus();

  default List<StatusType> getInactiveStatusesSince(DateTime timestamp)
  {
    return getInactiveStatusesSince(timestamp, null);
  }

  /**
   * Return up to {@code maxNumStatuses} statuses for inactive entries created on or later than the given timestamp
   *
   * @param timestamp timestamp
   * @param maxNumStatuses maxNumStatuses
   * @return list of statuses
   */
  List<StatusType> getInactiveStatusesSince(DateTime timestamp, @Nullable Integer maxNumStatuses);

  /**
   * Return createdDate and dataSource for the given id
   *
   * @return a pair of createdDate and dataSource or null if an entry for the given id is not found
   */
  @Nullable
  Pair<DateTime, String> getCreatedDateAndDataSource(String entryId);

  /**
   * Add a lock to the given entry
   *
   * @param entryId entry id
   * @param lock lock to add
   * @return true if the lock was added
   */
  boolean addLock(String entryId, LockType lock);

  /**
   * Replace an existing lock with a new lock.
   *
   * @param entryId   entry id
   * @param oldLockId lock to be replaced
   * @param newLock   lock to be added
   *
   * @return true if the lock is replaced
   */
  boolean replaceLock(String entryId, long oldLockId, LockType newLock);

  /**
   * Remove the lock with the given lock id.
   *
   * @param lockId lock id
   */
  void removeLock(long lockId);

  /**
   * Add a log to the entry with the given id.
   *
   * @param entryId entry id
   * @param log log to add
   * @return true if the log was added
   */
  boolean addLog(String entryId, LogType log);

  /**
   * Returns the logs for the entry with the given id.
   *
   * @param entryId entry id
   * @return list of logs
   */
  List<LogType> getLogs(String entryId);

  /**
   * Returns the locks for the given entry
   *
   * @param entryId entry id
   * @return map of lockId to lock
   */
  Map<Long, LockType> getLocks(String entryId);

  /**
   * Returns the lock id for the given entry and the lock.
   *
   * @return lock id if found. Otherwise null.
   */
  @Nullable
  Long getLockId(String entryId, LockType lock);
}
