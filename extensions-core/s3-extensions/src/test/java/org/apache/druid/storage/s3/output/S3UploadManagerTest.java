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

package org.apache.druid.storage.s3.output;

import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class S3UploadManagerTest
{
  private S3UploadManager s3UploadManager;

  @Before
  public void setup()
  {
    ScheduledExecutorFactory executorFactory = EasyMock.mock(ScheduledExecutorFactory.class);
    EasyMock.expect(executorFactory.create(EasyMock.anyInt(), EasyMock.anyString()))
            .andReturn(Execs.scheduledSingleThreaded("UploadThreadPool-%d"));
    EasyMock.replay(executorFactory);
    s3UploadManager = new S3UploadManager(executorFactory, new DruidProcessingConfigTest.MockRuntimeInfo(10, 0, 0));
  }

  @Test
  public void testDefault()
  {
    Assert.assertEquals(100, s3UploadManager.getMaxConcurrentNumChunks());
    Assert.assertEquals(0, s3UploadManager.getCurrentNumChunksOnDisk());
  }

  @Test
  public void testUpdateChunkSize()
  {
    // Verify that updating chunk size to a smaller or equal value doesn't change the maxConcurrentNumChunks.
    s3UploadManager.updateChunkSizeIfGreater(5L * 1024 * 1024L);
    Assert.assertEquals(100, s3UploadManager.getMaxConcurrentNumChunks());
    Assert.assertEquals(0, s3UploadManager.getCurrentNumChunksOnDisk());

    // Verify that updating chunk size to a greater value changes the maxConcurrentNumChunks.
    s3UploadManager.updateChunkSizeIfGreater(5L * 1024 * 1024 * 1024L);
    Assert.assertEquals(1, s3UploadManager.getMaxConcurrentNumChunks());
    Assert.assertEquals(0, s3UploadManager.getCurrentNumChunksOnDisk());
  }
}
