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

package org.apache.druid.indexing.overlord;

import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataStorageActionHandler;
import org.apache.druid.metadata.MetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class MetadataTaskStorageTest
{
  private MetadataTaskStorage metadataTaskStorage;
  private MetadataStorageConnector metadataStorageConnector;
  private MetadataStorageActionHandler storageActionHandler;
  private MetadataStorageActionHandlerFactory factory;
  private final List<TaskInfo<NoopTask, TaskStatus>> fullLIst = Collections.singletonList(
      new TaskInfo<>(
          "id1",
          DateTimes.of("2017-12-01"),
          TaskStatus.running("id1"),
          "datasourceName",
          NoopTask.create("id1", 0)
      )
  );

  @Before
  public void setup()
  {
    metadataStorageConnector = EasyMock.mock(MetadataStorageConnector.class);
    factory = EasyMock.mock(MetadataStorageActionHandlerFactory.class);
    storageActionHandler = EasyMock.mock(MetadataStorageActionHandler.class);
    EasyMock.expect(factory.create(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(storageActionHandler);
    EasyMock.expect(storageActionHandler.getActiveTaskInfo("datasourceName", null)).andReturn(fullLIst).anyTimes();
    EasyMock.expect(storageActionHandler.getActiveTaskInfo("datasourceName", 0))
            .andReturn(Collections.emptyList()).anyTimes();
    EasyMock.expect(storageActionHandler.getActiveTaskInfo("datasourceName", Integer.MAX_VALUE))
            .andReturn(fullLIst).anyTimes();
    EasyMock.replay(factory, storageActionHandler);
  }

  @Test
  public void testGetActiveTasks()
  {
    TaskStorageConfig taskStorageConfig = new TaskStorageConfig(null, null);
    metadataTaskStorage = new MetadataTaskStorage(metadataStorageConnector, taskStorageConfig, factory);
    List<TaskInfo<Task, TaskStatus>> datasourceName = metadataTaskStorage.getActiveTaskInfo("datasourceName", null);
    Assert.assertEquals(1, datasourceName.size());
  }

  @Test
  public void testGetActiveTasks0()
  {
    TaskStorageConfig taskStorageConfig = new TaskStorageConfig(null, 0);
    metadataTaskStorage = new MetadataTaskStorage(metadataStorageConnector, taskStorageConfig, factory);
    List<TaskInfo<Task, TaskStatus>> datasourceName = metadataTaskStorage.getActiveTaskInfo("datasourceName", null);
    Assert.assertEquals(0, datasourceName.size());
  }

  @Test
  public void testGetActiveTasksNull()
  {
    TaskStorageConfig taskStorageConfig = new TaskStorageConfig(null, null);
    metadataTaskStorage = new MetadataTaskStorage(metadataStorageConnector, taskStorageConfig, factory);
    List<TaskInfo<Task, TaskStatus>> datasourceName = metadataTaskStorage.getActiveTaskInfo("datasourceName", 0);
    Assert.assertEquals(0, datasourceName.size());
  }
}
