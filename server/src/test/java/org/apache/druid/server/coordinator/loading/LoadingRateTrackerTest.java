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

package org.apache.druid.server.coordinator.loading;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class LoadingRateTrackerTest
{
  private LoadingRateTracker tracker;

  @Before
  public void setup()
  {
    tracker = new LoadingRateTracker();
  }

  @Test
  public void testCurrentRateIsZeroWhenEmpty()
  {
    Assert.assertEquals(0, tracker.getMovingAverageLoadRateKilobytesPerSecond());

    tracker.updateProgress(1000, 10);
    Assert.assertEquals(1000 / 10, tracker.getMovingAverageLoadRateKilobytesPerSecond());
    Assert.assertEquals(8 * 1000 / 10, tracker.getMovingAverageLoadRateKbps());

    tracker.reset();
    Assert.assertEquals(0, tracker.getMovingAverageLoadRateKilobytesPerSecond());
  }

  @Test
  public void testCurrentRateAfter2Updates()
  {
    tracker.updateProgress(1000, 10);
    Assert.assertEquals(1000 / 10, tracker.getMovingAverageLoadRateKilobytesPerSecond());

    tracker.updateProgress(1000, 5);
    Assert.assertEquals(2000 / 15, tracker.getMovingAverageLoadRateKilobytesPerSecond());
    Assert.assertEquals(8 * 2000 / 15, tracker.getMovingAverageLoadRateKbps());
  }

  @Test
  public void testCurrentRateIsAMovingAverage()
  {
    final Random random = new Random(1001);
    final int windowSize = LoadingRateTracker.MOVING_AVERAGE_WINDOW_SIZE;
    final long minEntrySizeBytes = LoadingRateTracker.MIN_ENTRY_SIZE_BYTES;

    // Add updates to fill up the window size
    long[] updateBytes = new long[windowSize];
    long[] updateMillis = new long[windowSize];

    long totalBytes = 0;
    long totalMillis = 0;
    for (int i = 0; i < windowSize; ++i) {
      updateBytes[i] = minEntrySizeBytes + random.nextInt((int) minEntrySizeBytes);
      updateMillis[i] = 1 + random.nextInt(1000);

      totalBytes += updateBytes[i];
      totalMillis += updateMillis[i];

      tracker.updateProgress(updateBytes[i], updateMillis[i]);
      Assert.assertEquals(totalBytes / totalMillis, tracker.getMovingAverageLoadRateKilobytesPerSecond());
    }

    // Add another update
    long latestUpdateBytes = 1;
    long latestUpdateMillis = 1 + random.nextInt(1000);
    tracker.updateProgress(latestUpdateBytes, latestUpdateMillis);

    // Verify that the average window has moved
    totalBytes = totalBytes - updateBytes[0] + latestUpdateBytes;
    totalMillis = totalMillis - updateMillis[0] + latestUpdateMillis;
    Assert.assertEquals(totalBytes / totalMillis, tracker.getMovingAverageLoadRateKilobytesPerSecond());
  }

  @Test
  public void testCurrentRateWindowMovesOnlyAfterMinSizeUpdates()
  {
    final Random random = new Random(1001);

    long totalBytes = 0;
    long totalMillis = 0;

    final int windowSize = LoadingRateTracker.MOVING_AVERAGE_WINDOW_SIZE;
    final long minEntrySizeBytes = LoadingRateTracker.MIN_ENTRY_SIZE_BYTES;

    for (int i = 0; i < windowSize * 10; ++i) {
      long updateBytes = 1 + random.nextInt((int) minEntrySizeBytes / 100);
      long updateMillis = 1 + random.nextInt(1000);

      totalBytes += updateBytes;
      totalMillis += updateMillis;

      tracker.updateProgress(updateBytes, updateMillis);

      // Verify that the average window doesn't move
      Assert.assertEquals(totalBytes / totalMillis, tracker.getMovingAverageLoadRateKilobytesPerSecond());
    }
  }

  @Test
  public void testCurrentRateIsUnchangedWithZeroProgressTime()
  {
    tracker.updateProgress(1000, 10);
    Assert.assertEquals(1000 / 10, tracker.getMovingAverageLoadRateKilobytesPerSecond());

    tracker.updateProgress(1000, 0);
    Assert.assertEquals(1000 / 10, tracker.getMovingAverageLoadRateKilobytesPerSecond());
  }
}
