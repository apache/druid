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
import org.junit.Test;

public class SegmentGenerationMetricsTest
{
  @Test
  public void testSnapshotBeforeProcessing()
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics();
    assertMessageGapAggregateMetricsReset(metrics);
    SegmentGenerationMetrics snapshot = metrics.snapshot();
    assertMessageGapAggregateMetricsReset(metrics);
    // invalid value
    Assert.assertTrue(0 > snapshot.maxSegmentHandoffTime());
  }

  @Test
  public void testSnapshotAfterProcessingOver()
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics();

    metrics.reportMessageGap(20L);
    metrics.reportMessageMaxTimestamp(20L);
    metrics.reportMaxSegmentHandoffTime(7L);

    SegmentGenerationMetrics snapshot = metrics.snapshot();

    Assert.assertTrue(snapshot.messageGap() >= 20L);
    final SegmentGenerationMetrics.MessageGapStats messageGapStats = snapshot.getMessageGapStats();
    Assert.assertEquals(20L, messageGapStats.min());
    Assert.assertEquals(20L, messageGapStats.max());
    Assert.assertEquals(messageGapStats.min(), messageGapStats.max());
    Assert.assertEquals(20L, messageGapStats.avg(), 0);
    Assert.assertEquals(7, snapshot.maxSegmentHandoffTime());
  }

  @Test
  public void testProcessingOverAfterSnapshot()
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics();

    metrics.reportMessageMaxTimestamp(10);
    metrics.reportMessageGap(1);
    metrics.reportMaxSegmentHandoffTime(7L);
    // Should reset to invalid value
    metrics.snapshot();
    metrics.markProcessingDone();
    SegmentGenerationMetrics snapshot = metrics.snapshot();

    // Latest message gap must be invalid after processing is done
    Assert.assertEquals(-1, snapshot.messageGap());

    final SegmentGenerationMetrics.MessageGapStats messageGapStats = snapshot.getMessageGapStats();
    Assert.assertEquals(0, messageGapStats.count());
    Assert.assertEquals(Long.MIN_VALUE, messageGapStats.max());
    Assert.assertEquals(Long.MAX_VALUE, messageGapStats.min());
    Assert.assertEquals(Double.NaN, messageGapStats.avg(), 0);
    // value must be invalid
    Assert.assertTrue(0 > snapshot.maxSegmentHandoffTime());
  }

  @Test
  public void testMessageGapAggregateMetrics()
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics();

    for (int i = 0; i < 5; ++i) {
      metrics.reportMessageGap(i * 30);
    }
    metrics.reportMessageMaxTimestamp(10L);
    metrics.reportMaxSegmentHandoffTime(7L);
    metrics.markProcessingDone();
    SegmentGenerationMetrics snapshot = metrics.snapshot();
    // Latest message gap must be invalid after processing is done
    Assert.assertEquals(-1, snapshot.messageGap());

    final SegmentGenerationMetrics.MessageGapStats messageGapStats = snapshot.getMessageGapStats();
    Assert.assertEquals(5, messageGapStats.count());
    Assert.assertEquals(0, messageGapStats.min());
    Assert.assertEquals(120, messageGapStats.max());
    Assert.assertEquals(60.0, messageGapStats.avg(), 0);

    Assert.assertEquals(7L, snapshot.maxSegmentHandoffTime());
  }

  private static void assertMessageGapAggregateMetricsReset(final SegmentGenerationMetrics metrics)
  {
    final SegmentGenerationMetrics.MessageGapStats messageGapStats = metrics.getMessageGapStats();
    Assert.assertEquals(0, messageGapStats.count());
    Assert.assertEquals(Long.MIN_VALUE, messageGapStats.max());
    Assert.assertEquals(Long.MAX_VALUE, messageGapStats.min());
    Assert.assertEquals(Double.NaN, messageGapStats.avg(), 0);
  }
}
