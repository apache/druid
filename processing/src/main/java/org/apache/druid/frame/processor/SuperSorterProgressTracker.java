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

import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.ISE;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A thread-safe class that keeps track of the progress of an n-way, multilevel merge sort. The logic for this
 * class is coupled to the implementation of {@link SuperSorter}, which it is designed to track.
 *
 * It keeps a track of the following things:
 * 1. Total levels to merge (Cannnot be modified once set)
 * 2. Total mergers to merge in each level
 * 3. Total merged batches in each level
 *
 * There are a few states in which this progress tracker can be in (depending on the SuperSorter's progres):
 * 1. Unknown everything                                   | totalMergingLevels = -1, mergersForUltimateLevel = UNKNOWN_TOTAL
 * 2. Known total levels, but unknown output partitions    | totalMergingLevels = set, mergersForUltimateLevelSet = UNKNOWN_TOTAL
 * 3. Unknown total levels, but known output partitions    | totalMergingLevels = -1, mergersForUltimateLevelSet != UNKNOWN_TOTAL
 * 4. Known everything                                     | totalMergingLevels = set, mergersForUltimateLevelSet != UNKNOWN_TOTAL
 *
 * Progress can only be quantified if the total levels are known
 *
 * snapshot function does the housekeeping to hide this operational complexity away from classes other than {@link SuperSorter}
 * and {@link SuperSorterProgressTracker}
 */
public class SuperSorterProgressTracker
{
  private int totalMergingLevels = SuperSorter.UNKNOWN_LEVEL;

  // the value of total batches in levelToTotalBatches for the ultimate level get overridden by this member. This is
  // because the ultimate level is blocked on getting the output partitions, which may happen during the progress of the
  // SuperSorter
  private long totalMergersForUltimateLevel = SuperSorter.UNKNOWN_TOTAL;

  // level -> number of mergers in that level, might have some value for the ultimate level, but that is to be ignored
  private final Map<Integer, Long> levelToTotalBatches;

  // level -> number of mergers completed in that level
  private final Map<Integer, Long> levelToMergedBatches;

  // This is set to true if the SuperSorter doesn't need to do any work. For example, if there is no input to sort
  private boolean isTriviallyComplete = false;

  /**
   * Snapshot of a SuperSorter that is trivially sorted. All the maps have been cleared up. While this is not required
   * since isTriviallyComplete=true ensures that progressDigest=1.0, it produces a better output
   */
  private static final SuperSorterProgressSnapshot TRIVIALLY_COMPLETE_SNAPSHOT = new SuperSorterProgressSnapshot(
      SuperSorter.UNKNOWN_LEVEL,
      Collections.emptyMap(),
      Collections.emptyMap(),
      SuperSorter.UNKNOWN_TOTAL,
      true
  );

  public SuperSorterProgressTracker()
  {
    this.levelToMergedBatches = new HashMap<>();
    this.levelToTotalBatches = new HashMap<>();
  }

  /**
   * Set total merging levels for the SuperSorter it is tracking. Can be set only once
   */
  public synchronized void setTotalMergingLevels(final int totalMergingLevels)
  {
    if (this.totalMergingLevels != SuperSorter.UNKNOWN_LEVEL) {
      throw new ISE("Total merging levels already defined for the merge sort.");
    }
    levelToMergedBatches.keySet().stream().max(Ordering.natural()).ifPresent(max -> {
      if (max >= totalMergingLevels) {
        throw new ISE(
            "Max level found in levelToMergedBatches is %d (0-indexed). Cannot set totalMergingLevels to %d",
            max,
            totalMergingLevels
        );
      }
    });
    levelToTotalBatches.keySet().stream().max(Ordering.natural()).ifPresent(max -> {
      if (max >= totalMergingLevels) {
        throw new ISE(
            "Max level found in levelToTotalBatches is %d (0-indexed). Cannot set totalMergingLevels to %d",
            max,
            totalMergingLevels
        );
      }
    });

    this.totalMergingLevels = totalMergingLevels;
  }

  /**
   * Sets the total mergers for a level. Can be set only once, except for the ultimate level (if total levels are known)
   * because they get overridden by totalMergersForUltimateLevel
   */
  public synchronized void setTotalMergersForLevel(final int level, final long totalMergers)
  {
    if (level < 0) {
      throw new ISE("Unable to set %d total mergers for level %d. Level must be non-negative", totalMergers, level);
    }
    if (totalMergingLevels != SuperSorter.UNKNOWN_LEVEL && level >= totalMergingLevels) {
      throw new ISE(
          "Cannot set total mergers for level %d. Valid levels range from 0 to %d",
          level,
          totalMergingLevels - 1
      );
    }
    if (totalMergingLevels != SuperSorter.UNKNOWN_LEVEL
        && level < totalMergingLevels - 1 // This condition is only present for levels excluding the ultimate level
        && levelToTotalBatches.containsKey(level)) {
      throw new ISE("Total mergers are already present for the level %d", level);
    }
    levelToTotalBatches.put(level, totalMergers);
  }

  /**
   * Sets the number of mergers in the ultimate level (number of mergers = number of output partitions).
   * Can only be set once
   */
  public synchronized void setTotalMergersForUltimateLevel(final long totalMergersForUltimateLevel)
  {
    if (this.totalMergersForUltimateLevel != SuperSorter.UNKNOWN_TOTAL) {
      throw new ISE("Cannot set mergers for final level more than once");
    }
    this.totalMergersForUltimateLevel = totalMergersForUltimateLevel;
  }

  /**
   * This method is designed to be called during the course of the sorting. The batches once merged for a particular
   * level can be marked as such through this.
   */
  public synchronized void addMergedBatchesForLevel(final int level, final long additionalMergedBatches)
  {
    if (totalMergingLevels != SuperSorter.UNKNOWN_LEVEL && level >= totalMergingLevels) {
      throw new ISE(
          "Cannot add merged batches for level %d. Valid levels range from 0 to %d",
          level,
          totalMergingLevels - 1
      );
    }
    levelToMergedBatches.compute(level, (l, mergedBatchesSoFar) -> mergedBatchesSoFar == null
                                                                   ? additionalMergedBatches
                                                                   : additionalMergedBatches + mergedBatchesSoFar);
  }

  /**
   * If the SuperSorter is trivially done without doing any work (for eg - empty input), the tracker can be marked as
   * trivially complete. Once a tracker is marked as complete, the snapshots will always report back the progress
   * digest as 1. Any modification to the state of the tracker (eg: calling setTotalMergersForLevel()) would proceed
   * as regular, but
   */
  public synchronized void markTriviallyComplete()
  {
    this.isTriviallyComplete = true;
  }

  /**
   * @return Aggregates the information present in the tracker object and returns a
   * {@link SuperSorterProgressSnapshot} representing the same information
   */
  public synchronized SuperSorterProgressSnapshot snapshot()
  {
    if (isTriviallyComplete) {
      return TRIVIALLY_COMPLETE_SNAPSHOT; // Return a cleaned up snapshot
    }
    final Map<Integer, Long> levelToTotalBatchesCopy = new HashMap<>(this.levelToTotalBatches);
    final Map<Integer, Long> levelToMergedBatchesCopy = new HashMap<>(this.levelToMergedBatches);

    if (this.totalMergingLevels != SuperSorter.UNKNOWN_LEVEL
        && totalMergersForUltimateLevel != SuperSorter.UNKNOWN_TOTAL) {
      levelToTotalBatchesCopy.put(this.totalMergingLevels - 1, totalMergersForUltimateLevel);
    } else if (this.totalMergingLevels
               != SuperSorter.UNKNOWN_LEVEL) { // Override whatever was written in the original map
      levelToTotalBatchesCopy.put(this.totalMergingLevels - 1, SuperSorter.UNKNOWN_TOTAL);
    }
    return new SuperSorterProgressSnapshot(
        this.totalMergingLevels,
        levelToTotalBatchesCopy,
        levelToMergedBatchesCopy,
        totalMergersForUltimateLevel,
        isTriviallyComplete
    );
  }
}
