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

package org.apache.druid.server.coordinator.cost;

import org.apache.commons.math3.util.FastMath;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.server.coordinator.CostBalancerStrategy;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Basic unit for cost computation for SortingCostBalancerStrategy
 */
public class SortingCostComputer
{
  /**
   * HALF_LIFE_HOURS defines how fast joint cost function tends to 0 as distance between segments' intervals increasing.
   * The value of 1 day means that cost function of co-locating two segments which have 1 days between their intervals
   * is 0.5 of the cost, if the intervals are adjacent. If the distance is 2 days, then 0.25, etc.
   */
  private static final double HALF_LIFE_HOURS = 24.0;
  private static final double LAMBDA = Math.log(2) / HALF_LIFE_HOURS;
  static final double NORMALIZATION_FACTOR = 1 / (LAMBDA * LAMBDA);
  private static final double MILLIS_FACTOR = TimeUnit.HOURS.toMillis(1) / LAMBDA;

  /**
   * LIFE_THRESHOLD is used to avoid calculations for segments that are "far"
   * from each other and thus cost ~ 0 for these segments
   */
  private static final long LIFE_THRESHOLD = TimeUnit.DAYS.toMillis(30);

  private static final long BUCKET_LENGTH = TimeUnit.DAYS.toMillis(372);

  private final ArrayList<Bucket> sortedBuckets = new ArrayList<>();
  private final ArrayList<Interval> sortedIntervals = new ArrayList<>();
  private final ArrayList<Pair<Double, Double>> adhocNormalizedIntervals = new ArrayList<>();

  SortingCostComputer(Set<DataSegment> segments)
  {
    final NavigableMap<Interval, Bucket.Builder> buckets = new TreeMap<>(Comparators.intervalsByStartThenEnd());

    final HashSet<SegmentId> adhocSegments = new HashSet<>();

    for (DataSegment segment : segments) {
      if (isAdhoc(segment)) {
        if (!adhocSegments.add(segment.getId())) {
          throw new ISE("expect new segment");
        }
        Interval interval = segment.getInterval();
        adhocNormalizedIntervals.add(Pair.of(interval.getStartMillis() / MILLIS_FACTOR,
                                             interval.getEndMillis() / MILLIS_FACTOR));
      } else {
        Bucket.Builder builder = buckets.computeIfAbsent(getBucketInterval(segment), Bucket::builder);
        builder.addSegment(segment);
      }
    }

    for (Map.Entry<Interval, Bucket.Builder> entry : buckets.entrySet()) {
      sortedIntervals.add(entry.getKey());
      sortedBuckets.add(entry.getValue().build());
    }
  }

  public double cost(DataSegment segment)
  {
    double cost = 0.0;
    int index = Collections.binarySearch(sortedIntervals, segment.getInterval(), Comparators.intervalsByStartThenEnd());
    index = (index >= 0) ? index : -index - 1;

    for (ListIterator<Bucket> it = sortedBuckets.listIterator(index); it.hasNext(); ) {
      Bucket bucket = it.next();
      if (!bucket.inCalculationInterval(segment)) {
        break;
      }
      // O(logN) -> N segments per bucket
      cost += bucket.cost(segment);
    }

    for (ListIterator<Bucket> it = sortedBuckets.listIterator(index); it.hasPrevious(); ) {
      Bucket bucket = it.previous();
      if (!bucket.inCalculationInterval(segment)) {
        break;
      }
      // O(logN) -> N segments per bucket
      cost += bucket.cost(segment);
    }

    double start = segment.getInterval().getStartMillis() / MILLIS_FACTOR;
    double end = segment.getInterval().getEndMillis() / MILLIS_FACTOR;

    // O(M) -> M adhoc buckets
    for (Pair<Double, Double> adhoc : adhocNormalizedIntervals) {
      cost += CostBalancerStrategy.intervalCost(adhoc.rhs - adhoc.lhs, start - adhoc.lhs, end - adhoc.lhs);
    }
    return cost * NORMALIZATION_FACTOR;
  }

  private static Interval getBucketInterval(DataSegment segment)
  {
    DateTime startYear = segment.getInterval().getStart().withMonthOfYear(1).withDayOfMonth(1).withMillisOfDay(0);
    long startTime = startYear.getMillis();
    long endTime = startTime + BUCKET_LENGTH;
    return new Interval(startTime, endTime);
  }

  private boolean isAdhoc(DataSegment segment)
  {
    Interval bucketInterval = getBucketInterval(segment);
    Interval segmentInterval = segment.getInterval();
    return !(bucketInterval.getStartMillis() <= segmentInterval.getStartMillis()
             && bucketInterval.getEndMillis() > segmentInterval.getEndMillis());
  }

  static class Bucket
  {
    private final Interval calculationInterval;

    private final long START;
    private final long[] start;
    private final long[] end;

    private final double[] startValSum;
    private final double[] startExpSum;
    private final double[] startExpInvSum;

    private final double[] endValSum;
    private final double[] endExpSum;
    private final double[] endExpInvSum;

    Bucket(Interval interval, List<Pair<Long, Long>> intervals)
    {
      this.calculationInterval = new Interval(
          interval.getStart().minus(LIFE_THRESHOLD),
          interval.getEnd().plus(LIFE_THRESHOLD)
      );

      int n = intervals.size();
      start = new long[n];
      end = new long[n];
      for (int i = 0; i < n; i++) {
        start[i] = intervals.get(i).lhs;
        end[i] = intervals.get(i).rhs;
      }
      Arrays.sort(start);
      Arrays.sort(end);

      START = calculationInterval.getStartMillis();

      startValSum = new double[n + 1];
      startExpSum = new double[n + 1];
      for (int i = 0; i < n; i++) {
        double startVal = getVal(start[i]);
        startValSum[i + 1] = startValSum[i] + startVal;
        startExpSum[i + 1] = startExpSum[i] + FastMath.exp(startVal);
      }
      startExpInvSum = new double[n + 1];
      for (int i = n; i > 0; i--) {
        double startVal = getVal(start[i - 1]);
        startExpInvSum[n - i + 1] = startExpInvSum[n - i] + FastMath.exp(-startVal);
      }

      endValSum = new double[n + 1];
      endExpSum = new double[n + 1];
      for (int i = 0; i < n; i++) {
        double endVal = getVal(end[i]);
        endValSum[i + 1] = endValSum[i] + endVal;
        endExpSum[i + 1] = endExpSum[i] + FastMath.exp(endVal);
      }
      endExpInvSum = new double[n + 1];
      for (int i = n; i > 0; i--) {
        double endVal = getVal(end[i - 1]);
        endExpInvSum[n - i + 1] = endExpInvSum[n - i] + FastMath.exp(-endVal);
      }
    }

    boolean inCalculationInterval(DataSegment dataSegment)
    {
      return calculationInterval.overlaps(dataSegment.getInterval());
    }

    double cost(DataSegment dataSegment)
    {
      // avoid calculation for segments outside of LIFE_THRESHOLD
      if (!inCalculationInterval(dataSegment)) {
        throw new ISE("Segment is not within calculation interval");
      }

      long x = Math.max(dataSegment.getInterval().getStartMillis(), calculationInterval.getStartMillis());
      long y = Math.min(dataSegment.getInterval().getEndMillis(), calculationInterval.getEndMillis());

      return solve(x, y, end, endValSum, endExpSum, endExpInvSum)
             - solve(x, y, start, startValSum, startExpSum, startExpInvSum);
    }

    // Sum of cost (<val, END>, <x, y>) for all val in vals
    private double solve(long x, long y, long[] vals, double[] sum, double[] expSum, double[] expInvSum)
    {
      int n = vals.length;

      double xVal = getVal(x);
      double xExp = FastMath.exp(xVal);
      double xExpInv = FastMath.exp(-xVal);

      double yVal = getVal(y);
      double yExp = FastMath.exp(yVal);
      double yExpInv = FastMath.exp(-yVal);

      double cost = 0.0;

      int l = lowerBound(0, n - 1, x, vals) + 1;
      int r = upperBound(0, n - 1, y, vals);

      // val < x , y
      cost += expSum[l] * xExpInv;
      cost -= expSum[l] * yExpInv;
      cost += n * yExpInv;
      cost -= n * xExpInv;

      // x <= val <= y
      cost += 2 * (sum[r] - sum[l]);
      cost -= 2 * (r - l) * xVal;
      cost += (expInvSum[n - l] - expInvSum[n - r]) * xExp;
      cost -= (expSum[r] - expSum[l]) * yExpInv;

      // x, y < val
      cost += (expInvSum[n - r] - expInvSum[0]) * xExp;
      cost -= (expInvSum[n - r] - expInvSum[0]) * yExp;
      cost += 2 * (n - r) * yVal;
      cost -= 2 * (n - r) * xVal;

      return cost;
    }

    private double getVal(long millis)
    {
      return millis / MILLIS_FACTOR - START / MILLIS_FACTOR;
    }

    /**
     * Return last index in array that is strictly less than x
     */
    private int lowerBound(int l, int r, long x, long[] a)
    {
      if (l == r) {
        return a[r] < x ? r : l - 1;
      }
      int m = (l + r + 1) / 2;
      if (a[m] < x) {
        return lowerBound(m, r, x, a);
      } else {
        return lowerBound(l, m - 1, x, a);
      }
    }

    /**
     * Return first index in the array that is strictly greater than x
     */
    private int upperBound(int l, int r, long x, long[] a)
    {
      if (l == r) {
        return a[l] > x ? l : r + 1;
      }
      int m = (l + r) / 2;
      if (a[m] > x) {
        return upperBound(l, m, x, a);
      } else {
        return upperBound(m + 1, r, x, a);
      }
    }

    public static Builder builder(Interval interval)
    {
      return new Builder(interval);
    }

    static class Builder
    {
      protected final Interval interval;
      private final Set<SegmentId> segmentSet = new HashSet<>();

      public Builder(Interval interval)
      {
        this.interval = interval;
      }

      public Builder addSegment(DataSegment dataSegment)
      {
        if (!interval.contains(dataSegment.getInterval().getStartMillis())) {
          throw new ISE("Failed to add segment to bucket: interval is not covered by this bucket");
        }

        if (!segmentSet.add(dataSegment.getId())) {
          throw new ISE("expect new segment");
        }

        return this;
      }

      public Bucket build()
      {
        List<Pair<Long, Long>> intervals = new ArrayList<>();

        for (SegmentId segment : segmentSet) {
          Interval i = segment.getInterval();
          intervals.add(Pair.of(i.getStartMillis(), i.getEndMillis()));
        }

        return new Bucket(interval, intervals);
      }
    }
  }
}
