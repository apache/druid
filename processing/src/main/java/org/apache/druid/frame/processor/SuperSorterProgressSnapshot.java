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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Like {@link SuperSorterProgressTracker}, but immutable.
 */
public class SuperSorterProgressSnapshot
{
  private final int totalMergingLevels;
  private final Map<Integer, Long> levelToTotalBatches;
  private final Map<Integer, Long> levelToMergedBatches;
  private final long totalMergersForUltimateLevel;
  private final boolean isTriviallyComplete;

  @JsonCreator
  public SuperSorterProgressSnapshot(
      @JsonProperty("totalMergingLevels") final int totalMergingLevels,
      @JsonProperty("levelToTotalBatches") final Map<Integer, Long> levelToTotalBatches,
      @JsonProperty("levelToMergedBatches") final Map<Integer, Long> levelToMergedBatches,
      @JsonProperty("totalMergersForUltimateLevel") final long totalMergersForUltimateLevel,
      @JsonProperty("triviallyComplete") final boolean isTriviallyComplete
  )
  {
    this.totalMergingLevels = totalMergingLevels;
    this.levelToTotalBatches = levelToTotalBatches;
    this.levelToMergedBatches = levelToMergedBatches;
    this.totalMergersForUltimateLevel = totalMergersForUltimateLevel;
    this.isTriviallyComplete = isTriviallyComplete;
  }

  @JsonProperty(value = "totalMergingLevels")
  public int getTotalMergingLevels()
  {
    return totalMergingLevels;
  }

  @JsonProperty(value = "levelToTotalBatches")
  public Map<Integer, Long> getLevelToTotalBatches()
  {
    return levelToTotalBatches;
  }

  @JsonProperty(value = "levelToMergedBatches")
  public Map<Integer, Long> getLevelToMergedBatches()
  {
    return levelToMergedBatches;
  }

  @JsonProperty("totalMergersForUltimateLevel")
  public long getTotalMergersForUltimateLevel()
  {
    return totalMergersForUltimateLevel;
  }

  @JsonProperty("triviallyComplete")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isTriviallyComplete()
  {
    return isTriviallyComplete;
  }

  @Nullable
  @JsonProperty(value = "progressDigest")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Double getProgressDigest()
  {
    // Progress is complete if the inputs are sorted trivially without doing any work
    if (isTriviallyComplete) {
      return 1.0;
    }

    if (totalMergingLevels == SuperSorter.UNKNOWN_LEVEL) {
      return null;
    }

    double progress = 0.0;

    for (int level = 0; level < totalMergingLevels; ++level) {
      final Long mergedBatches = levelToMergedBatches.getOrDefault(level, 0L);
      final Long totalBatches = levelToTotalBatches.getOrDefault(level, SuperSorter.UNKNOWN_TOTAL);
      if (mergedBatches != null && totalBatches != null && totalBatches > 0) {
        final double levelProgress = mergedBatches.doubleValue() / totalBatches.doubleValue();
        progress += levelProgress / totalMergingLevels;
      }
    }

    // We use a delta of 0.000001 to round up in case the progress is 1.
    // The following operation maps the results from range of [0.999999, +inf] to 1 (+inf is not possible, but that is
    // the behaviour of the following statement)
    return ((progress + 1e-6) > 1.0) ? 1.0 : progress;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SuperSorterProgressSnapshot that = (SuperSorterProgressSnapshot) o;
    return totalMergingLevels == that.totalMergingLevels
           && totalMergersForUltimateLevel == that.totalMergersForUltimateLevel
           && isTriviallyComplete == that.isTriviallyComplete
           && Objects.equals(levelToTotalBatches, that.levelToTotalBatches)
           && Objects.equals(levelToMergedBatches, that.levelToMergedBatches);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        totalMergingLevels,
        levelToTotalBatches,
        levelToMergedBatches,
        totalMergersForUltimateLevel,
        isTriviallyComplete
    );
  }
}
