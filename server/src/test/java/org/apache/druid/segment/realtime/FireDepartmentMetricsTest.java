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
    FireDepartmentMetrics snapshot = metrics.snapshot();
    Assert.assertEquals(0L, snapshot.messageGap());
    // invalid value
    Assert.assertTrue(0 > snapshot.maxSegmentHandoffTime());
  }

  @Test
  public void testSnapshotAfterProcessingOver()
  {
    metrics.reportMessageMaxTimestamp(System.currentTimeMillis() - 20L);
    metrics.reportMaxSegmentHandoffTime(7L);
    FireDepartmentMetrics snapshot = metrics.snapshot();
    Assert.assertTrue(snapshot.messageGap() >= 20L);
    Assert.assertEquals(7, snapshot.maxSegmentHandoffTime());
  }

  @Test
  public void testProcessingOverAfterSnapshot()
  {
    metrics.reportMessageMaxTimestamp(10);
    metrics.reportMaxSegmentHandoffTime(7L);
    // Should reset to invalid value
    metrics.snapshot();
    metrics.markProcessingDone();
    FireDepartmentMetrics snapshot = metrics.snapshot();
    // Message gap must be invalid after processing is done
    Assert.assertTrue(0 > snapshot.messageGap());
    // value must be invalid
    Assert.assertTrue(0 > snapshot.maxSegmentHandoffTime());
  }
}
