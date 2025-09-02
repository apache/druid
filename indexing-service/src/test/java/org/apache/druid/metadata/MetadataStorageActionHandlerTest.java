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

package org.apache.druid.metadata;

import com.google.common.base.Optional;
import org.apache.druid.indexer.TaskIdentifier;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.task.Task;
import org.joda.time.DateTime;
import org.junit.Before;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests the default methods of the interface {@link MetadataStorageActionHandler}.
 * Required only for coverage as these methods are already being tested in
 * {@code SQLMetadataStorageActionHandlerTest}.
 */
public class MetadataStorageActionHandlerTest
{

  private MetadataStorageActionHandler handler;

  @Before
  public void setup()
  {
    this.handler = new MetadataStorageActionHandler()
    {
      @Override
      public void insert(
          String id,
          DateTime timestamp,
          String dataSource,
          @NotNull Task entry,
          boolean active,
          @Nullable TaskStatus status,
          String type,
          String groupId
      )
      {

      }

      @Override
      public boolean setStatus(String entryId, boolean active, TaskStatus status)
      {
        return false;
      }

      @Override
      public Optional<Task> getEntry(String entryId)
      {
        return null;
      }

      @Override
      public Optional<TaskStatus> getStatus(String entryId)
      {
        return null;
      }

      @Nullable
      @Override
      public TaskInfo<Task, TaskStatus> getTaskInfo(String entryId)
      {
        return null;
      }

      @Override
      public List<TaskInfo<Task, TaskStatus>> getTaskInfos(
          Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups,
          @Nullable String datasource
      )
      {
        return Collections.emptyList();
      }

      @Override
      public List<TaskInfo<TaskIdentifier, TaskStatus>> getTaskStatusList(
          Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups,
          @Nullable String datasource
      )
      {
        return Collections.emptyList();
      }

      @Override
      public boolean addLock(String entryId, TaskLock lock)
      {
        return false;
      }

      @Override
      public boolean replaceLock(String entryId, long oldLockId, TaskLock newLock)
      {
        return false;
      }

      @Override
      public void removeLock(long lockId)
      {

      }

      @Override
      public void removeTasksOlderThan(long timestamp)
      {

      }

      @Override
      public Map<Long, TaskLock> getLocks(String entryId)
      {
        return Collections.emptyMap();
      }

      @Override
      public Long getLockId(String entryId, TaskLock lock)
      {
        return 0L;
      }

      @Override
      public void populateTaskTypeAndGroupIdAsync()
      {

      }
    };
  }
}
