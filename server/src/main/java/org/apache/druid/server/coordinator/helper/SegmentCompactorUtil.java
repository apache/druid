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

package org.apache.druid.server.coordinator.helper;

import com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 * Util class used by {@link DruidCoordinatorSegmentCompactor} and {@link CompactionSegmentSearchPolicy}.
 */
class SegmentCompactorUtil
{
  private static final Period LOOKUP_PERIOD = new Period("P1D");
  private static final Duration LOOKUP_DURATION = LOOKUP_PERIOD.toStandardDuration();
  // Allow compaction of segments if totalSize(segments) <= remainingBytes * ALLOWED_MARGIN_OF_COMPACTION_SIZE
  private static final double ALLOWED_MARGIN_OF_COMPACTION_SIZE = .1;

  static boolean isCompactible(long remainingBytes, long currentTotalBytes, long additionalBytes)
  {
    return remainingBytes * (1 + ALLOWED_MARGIN_OF_COMPACTION_SIZE) >= currentTotalBytes + additionalBytes;
  }

  static boolean isProperCompactionSize(long targetCompactionSizeBytes, long totalBytesOfSegmentsToCompact)
  {
    return targetCompactionSizeBytes * (1 - ALLOWED_MARGIN_OF_COMPACTION_SIZE) <= totalBytesOfSegmentsToCompact &&
           targetCompactionSizeBytes * (1 + ALLOWED_MARGIN_OF_COMPACTION_SIZE) >= totalBytesOfSegmentsToCompact;
  }

  /**
   * Return an interval for looking up for timeline.
   * If {@code totalInterval} is larger than {@link #LOOKUP_PERIOD}, it returns an interval of {@link #LOOKUP_PERIOD}
   * and the end of {@code totalInterval}.
   */
  static Interval getNextLoopupInterval(Interval totalInterval)
  {
    final Duration givenDuration = totalInterval.toDuration();
    return givenDuration.isLongerThan(LOOKUP_DURATION) ?
           new Interval(LOOKUP_PERIOD, totalInterval.getEnd()) :
           totalInterval;
  }

  /**
   * Removes {@code smallInterval} from {@code largeInterval}.  The end of both intervals should be same.
   *
   * @return an interval of {@code largeInterval} - {@code smallInterval}.
   */
  static Interval removeIntervalFromEnd(Interval largeInterval, Interval smallInterval)
  {
    Preconditions.checkArgument(
        largeInterval.getEnd().equals(smallInterval.getEnd()),
        "end should be same. largeInterval[%s] smallInterval[%s]",
        largeInterval,
        smallInterval
    );
    return new Interval(largeInterval.getStart(), smallInterval.getStart());
  }

  private SegmentCompactorUtil() {}
}
