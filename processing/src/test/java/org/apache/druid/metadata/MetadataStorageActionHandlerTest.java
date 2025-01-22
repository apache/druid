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
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskIdentifier;
import org.apache.druid.indexer.TaskInfo;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
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

  private MetadataStorageActionHandler<String, String, String, String> handler;

  @Before
  public void setup()
  {
    this.handler = new MetadataStorageActionHandler<>()
    {
      @Override
      public void insert(
          String id,
          DateTime timestamp,
          String dataSource,
          String entry,
          boolean active,
          @Nullable String status,
          String type,
          String groupId
      )
      {

      }

      @Override
      public boolean setStatus(String entryId, boolean active, String status)
      {
        return false;
      }

      @Override
      public Optional<String> getEntry(String entryId)
      {
        return null;
      }

      @Override
      public Optional<String> getStatus(String entryId)
      {
        return null;
      }

      @Nullable
      @Override
      public TaskInfo<String, String> getTaskInfo(String entryId)
      {
        return null;
      }

      @Override
      public List<TaskInfo<String, String>> getTaskInfos(
          Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups,
          @Nullable String datasource
      )
      {
        return Collections.emptyList();
      }

      @Override
      public List<TaskInfo<TaskIdentifier, String>> getTaskStatusList(
          Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups,
          @Nullable String datasource
      )
      {
        return Collections.emptyList();
      }

      @Override
      public boolean addLock(String entryId, String lock)
      {
        return false;
      }

      @Override
      public boolean replaceLock(String entryId, long oldLockId, String newLock)
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
      public Map<Long, String> getLocks(String entryId)
      {
        return Collections.emptyMap();
      }

      @Override
      public Long getLockId(String entryId, String lock)
      {
        return 0L;
      }

      @Override
      public void populateTaskTypeAndGroupIdAsync()
      {

      }
    };
  }

  @Test
  public void testAddLogThrowsUnsupportedException()
  {
    Exception exception = Assert.assertThrows(
        DruidException.class,
        () -> handler.addLog("abcd", "logentry")
    );
    Assert.assertEquals(
        "Task actions are not logged anymore.",
        exception.getMessage()
    );
  }

  @Test
  public void testGetLogsThrowsUnsupportedException()
  {
    Exception exception = Assert.assertThrows(
        DruidException.class,
        () -> handler.getLogs("abcd")
    );
    Assert.assertEquals(
        "Task actions are not logged anymore.",
        exception.getMessage()
    );
  }
}
