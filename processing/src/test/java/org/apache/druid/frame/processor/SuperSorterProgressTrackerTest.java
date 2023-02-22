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

package org.apache.druid.frame.processor;

import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This class primarily tests the invalid calls to the methods of {@link SuperSorterProgressTracker}. Tests regarding
 * the correctness of the tracking is primarily present in {@link SuperSorterProgressSnapshotTest}
 */
public class SuperSorterProgressTrackerTest
{
  SuperSorterProgressTracker superSorterProgressTracker;

  @Before
  public void setup()
  {
    superSorterProgressTracker = new SuperSorterProgressTracker();
  }

  @Test
  public void testSetTotalMergingLevelsMultipleTimes()
  {
    ISE exception = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.setTotalMergingLevels(5);
      superSorterProgressTracker.setTotalMergingLevels(5);
    });
    Assert.assertEquals("Total merging levels already defined for the merge sort.", exception.getMessage());
  }

  @Test
  public void testSetTotalMergingLevelsWithConflictingDataWithTotalMergers()
  {
    superSorterProgressTracker.setTotalMergersForLevel(3, 8);
    ISE exception = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.setTotalMergingLevels(2);
    });
    Assert.assertEquals(
        "Max level found in levelToTotalBatches is 3 (0-indexed). Cannot set totalMergingLevels to 2",
        exception.getMessage()
    );
  }

  @Test
  public void testSetTotalMergingLevelsWithConflictingDataWithMergedBatches()
  {
    superSorterProgressTracker.addMergedBatchesForLevel(3, 8);
    ISE exception = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.setTotalMergingLevels(3);
    });
    Assert.assertEquals(
        "Max level found in levelToMergedBatches is 3 (0-indexed). Cannot set totalMergingLevels to 3",
        exception.getMessage()
    );
  }

  @Test
  public void testSetTotalMergersForLevelIncorrectly()
  {
    // Init state
    superSorterProgressTracker.setTotalMergingLevels(4);
    superSorterProgressTracker.setTotalMergersForLevel(2, 8);
    superSorterProgressTracker.setTotalMergersForLevel(3, 8);

    // Test throws exception when level is negative
    ISE exception1 = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.setTotalMergersForLevel(-1, 4);
    });
    Assert.assertEquals(
        "Unable to set 4 total mergers for level -1. Level must be non-negative",
        exception1.getMessage()
    );

    // Test throws when trying to set total mergers for level greater than permitted by the totalMergingLevels
    ISE exception2 = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.setTotalMergersForLevel(4, 1);
    });
    Assert.assertEquals(
        "Cannot set total mergers for level 4. Valid levels range from 0 to 3",
        exception2.getMessage()
    );

    // Test throws when trying to set total mergers for a level multiple times. Only works if the ultimate level is known
    ISE exception3 = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.setTotalMergersForLevel(2, 16);
    });
    Assert.assertEquals(
        "Total mergers are already present for the level 2",
        exception3.getMessage()
    );

    // The above check doesn't work for the ultimate level since that will be overridden later
    superSorterProgressTracker.setTotalMergersForLevel(3, 16);
  }

  @Test
  public void testSetMergersForUltimateLevelMultipleTimes()
  {
    ISE exception = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.setTotalMergersForUltimateLevel(5);
      superSorterProgressTracker.setTotalMergersForUltimateLevel(5);
    });
    Assert.assertEquals("Cannot set mergers for final level more than once", exception.getMessage());
  }

  @Test
  public void testAddMergedBatchesForLevelIncorrectly()
  {
    // Init state
    superSorterProgressTracker.setTotalMergingLevels(4);

    ISE exception = Assert.assertThrows(ISE.class, () -> {
      superSorterProgressTracker.addMergedBatchesForLevel(5, 4);
    });
    Assert.assertEquals(
        "Cannot add merged batches for level 5. Valid levels range from 0 to 3",
        exception.getMessage()
    );
  }
}
