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

package org.apache.druid.segment.realtime;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FireDepartmentMetricsTest
{
  private FireDepartmentMetrics metrics;

  @Before
  public void setup()
  {
    metrics = new FireDepartmentMetrics();
  }

  @Test
  public void testSnapshotBeforeProcessing()
  {
    Assert.assertEquals(0L, metrics.snapshot().messageGap());
  }

  @Test
  public void testSnapshotAfterProcessingOver()
  {
    metrics.reportMessageMaxTimestamp(10);
    metrics.markProcessingDone(30L);
    Assert.assertEquals(20, metrics.snapshot().messageGap());
  }

  @Test
  public void testSnapshotBeforeProcessingOver()
  {
    metrics.reportMessageMaxTimestamp(10);
    long current = System.currentTimeMillis();
    long messageGap = metrics.snapshot().messageGap();
    Assert.assertTrue("Message gap: " + messageGap, messageGap >= (current - 10));
  }

  @Test
  public void testProcessingOverAfterSnapshot()
  {
    metrics.reportMessageMaxTimestamp(10);
    metrics.snapshot();
    metrics.markProcessingDone(20);
    Assert.assertEquals(10, metrics.snapshot().messageGap());
  }

  @Test
  public void testProcessingOverWithSystemTime()
  {
    metrics.reportMessageMaxTimestamp(10);
    long current = System.currentTimeMillis();
    metrics.markProcessingDone();
    long completionTime = metrics.processingCompletionTime();
    Assert.assertTrue(
        "Completion time: " + completionTime,
        completionTime >= current && completionTime < (current + 10_000)
    );
  }

}
