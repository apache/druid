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

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(JUnitParamsRunner.class)
public class SegmentGenerationMetricsTest
{
  @Test
  @Parameters({"true", "false"})
  public void testSnapshotBeforeProcessing(boolean messageGapAggStats)
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics(messageGapAggStats);
    assertMessageGapAggregateMetricsReset(metrics);
    SegmentGenerationMetrics snapshot = metrics.snapshot();
    assertMessageGapAggregateMetricsReset(metrics);
    // invalid value
    Assert.assertTrue(0 > snapshot.maxSegmentHandoffTime());
  }

  @Test
  @Parameters({"true", "false"})
  public void testSnapshotAfterProcessingOver(boolean messageGapAggStats)
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics(messageGapAggStats);

    metrics.reportMessageGap(20L);
    metrics.reportMessageMaxTimestamp(20L);
    metrics.reportMaxSegmentHandoffTime(7L);

    SegmentGenerationMetrics snapshot = metrics.snapshot();

    Assert.assertTrue(snapshot.messageGap() >= 20L);
    if (messageGapAggStats) {
      Assert.assertEquals(20L, snapshot.minMessageGap());
      Assert.assertEquals(20L, snapshot.maxMessageGap());
      Assert.assertEquals(snapshot.minMessageGap(), snapshot.maxMessageGap());
      Assert.assertEquals(20L, snapshot.avgMessageGap(), 0);
      Assert.assertEquals(7, snapshot.maxSegmentHandoffTime());
    } else {
      assertMessageGapAggregateMetricsReset(snapshot);
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void testProcessingOverAfterSnapshot(boolean messageGapAggStats)
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics(messageGapAggStats);

    metrics.reportMessageMaxTimestamp(10);
    metrics.reportMessageGap(1);
    metrics.reportMaxSegmentHandoffTime(7L);
    // Should reset to invalid value
    metrics.snapshot();
    metrics.markProcessingDone();
    SegmentGenerationMetrics snapshot = metrics.snapshot();

    // Latest message gap must be invalid after processing is done
    Assert.assertEquals(-1, snapshot.messageGap());
    // Message gap aggregate metrics are persisted past snapshot if enabled
    if (messageGapAggStats) {
      Assert.assertEquals(0, snapshot.numMessageGapEvent());
      Assert.assertEquals(Long.MIN_VALUE, snapshot.maxMessageGap());
      Assert.assertEquals(Long.MAX_VALUE, snapshot.minMessageGap());
      Assert.assertEquals(0, snapshot.avgMessageGap(), 0);
    } else {
      assertMessageGapAggregateMetricsReset(snapshot);
      assertMessageGapAggregateMetricsReset(metrics);
    }
    // value must be invalid
    Assert.assertTrue(0 > snapshot.maxSegmentHandoffTime());
  }

  @Test
  @Parameters({"true", "false"})
  public void testMessageGapAggregateMetrics(boolean messageGapAggStats)
  {
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics(messageGapAggStats);

    for (int i = 0; i < 5; ++i) {
      metrics.reportMessageGap(i * 30);
    }
    metrics.reportMessageMaxTimestamp(10L);
    metrics.reportMaxSegmentHandoffTime(7L);
    metrics.markProcessingDone();
    SegmentGenerationMetrics snapshot = metrics.snapshot();
    // Latest message gap must be invalid after processing is done
    Assert.assertEquals(-1, snapshot.messageGap());

    if (messageGapAggStats) {
      Assert.assertEquals(5, snapshot.numMessageGapEvent());
      Assert.assertEquals(0, snapshot.minMessageGap());
      Assert.assertEquals(120, snapshot.maxMessageGap());
      Assert.assertEquals(60.0, snapshot.avgMessageGap(), 0);
    } else {
      assertMessageGapAggregateMetricsReset(snapshot);
    }

    Assert.assertEquals(7L, snapshot.maxSegmentHandoffTime());
  }

  private static void assertMessageGapAggregateMetricsReset(final SegmentGenerationMetrics metrics)
  {
    Assert.assertEquals(0, metrics.numMessageGapEvent());
    Assert.assertEquals(Long.MIN_VALUE, metrics.maxMessageGap());
    Assert.assertEquals(Long.MAX_VALUE, metrics.minMessageGap());
    Assert.assertEquals(0, metrics.avgMessageGap(), 0);
  }
}
