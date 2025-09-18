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

import com.google.common.base.Optional;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.MetadataStorageActionHandler;
import org.apache.druid.metadata.MetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class MetadataTaskStorageUpdateTaskTest
{
  private MetadataStorageConnector metadataStorageConnector;
  private TaskStorageConfig taskStorageConfig;
  private MetadataStorageActionHandlerFactory handlerFactory;
  private MetadataStorageActionHandler handler;
  private MetadataTaskStorage metadataTaskStorage;

  @Before
  public void setUp()
  {
    metadataStorageConnector = EasyMock.createMock(MetadataStorageConnector.class);
    taskStorageConfig = EasyMock.createMock(TaskStorageConfig.class);
    handlerFactory = EasyMock.createMock(MetadataStorageActionHandlerFactory.class);
    handler = EasyMock.createMock(MetadataStorageActionHandler.class);

    EasyMock.expect(handlerFactory.create()).andReturn(handler);
    EasyMock.replay(handlerFactory);

    metadataTaskStorage = new MetadataTaskStorage(
        metadataStorageConnector,
        taskStorageConfig,
        handlerFactory
    );
  }

  @Test
  public void testUpdateTaskSuccess()
  {
    final NoopTask task = NoopTask.create();

    EasyMock.expect(handler.getEntry(task.getId())).andReturn(Optional.of(task));
    handler.update(task.getId(), task);
    EasyMock.expectLastCall();

    EasyMock.replay(handler);

    metadataTaskStorage.updateTask(task);

    EasyMock.verify(handler);
  }

  @Test(expected = ISE.class)
  public void testUpdateTaskNotFound()
  {
    final NoopTask task = NoopTask.create();

    EasyMock.expect(handler.getEntry(task.getId())).andReturn(Optional.absent());

    EasyMock.replay(handler);

    metadataTaskStorage.updateTask(task);
  }

  @Test(expected = DruidException.class)
  public void testUpdateTaskDruidException()
  {
    final NoopTask task = NoopTask.create();
    final DruidException druidException = DruidException.defensive("Test DruidException");

    EasyMock.expect(handler.getEntry(task.getId())).andReturn(Optional.of(task));
    handler.update(task.getId(), task);
    EasyMock.expectLastCall().andThrow(druidException);

    EasyMock.replay(handler);

    metadataTaskStorage.updateTask(task);
  }

  @Test(expected = RuntimeException.class)
  public void testUpdateTaskGenericException()
  {
    final NoopTask task = NoopTask.create();
    final Exception genericException = new Exception("Test generic exception");

    EasyMock.expect(handler.getEntry(task.getId())).andReturn(Optional.of(task));
    handler.update(task.getId(), task);
    EasyMock.expectLastCall().andThrow(genericException);

    EasyMock.replay(handler);

    metadataTaskStorage.updateTask(task);
  }
}
