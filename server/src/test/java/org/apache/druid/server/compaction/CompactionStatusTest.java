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

package org.apache.druid.server.compaction;

import org.junit.Assert;
import org.junit.Test;

public class CompactionStatusTest
{
  @Test
  public void testCompleteConstant()
  {
    CompactionStatus status = CompactionStatus.COMPLETE;

    Assert.assertEquals(CompactionStatus.State.COMPLETE, status.getState());
    Assert.assertNull(status.getReason());
    Assert.assertTrue(status.isComplete());
    Assert.assertFalse(status.isSkipped());
  }

  @Test
  public void testPendingFactoryMethod()
  {
    CompactionStatus status = CompactionStatus.pending("needs compaction: %d segments", 5);

    Assert.assertEquals(CompactionStatus.State.PENDING, status.getState());
    Assert.assertEquals("needs compaction: 5 segments", status.getReason());
    Assert.assertFalse(status.isComplete());
    Assert.assertFalse(status.isSkipped());
  }

  @Test
  public void testSkippedFactoryMethod()
  {
    CompactionStatus status = CompactionStatus.skipped("already compacted");

    Assert.assertEquals(CompactionStatus.State.SKIPPED, status.getState());
    Assert.assertEquals("already compacted", status.getReason());
    Assert.assertFalse(status.isComplete());
    Assert.assertTrue(status.isSkipped());
  }

  @Test
  public void testRunningFactoryMethod()
  {
    CompactionStatus status = CompactionStatus.running("task-123");

    Assert.assertEquals(CompactionStatus.State.RUNNING, status.getState());
    Assert.assertEquals("task-123", status.getReason());
    Assert.assertFalse(status.isComplete());
    Assert.assertFalse(status.isSkipped());
  }
}
