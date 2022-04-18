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

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.commons.math3.util.FastMath;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.server.coordinator.CostBalancerStrategy;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * SegmentsCostCache provides faster way to calculate cost function proposed in {@link CostBalancerStrategy}.
 * See https://github.com/apache/druid/pull/2972 for more details about the cost function.
 *
 * Joint cost for two segments (you can make formulas below readable by copy-pasting to
 * https://www.codecogs.com/latex/eqneditor.php):
 *
 *        cost(Y, Y) = \int_{x_0}^{x_1} \int_{y_0}^{y_1} e^{-\lambda |x-y|}dxdy
 * or
 *        cost(Y, Y) = e^{y_0 + y_1} (e^{x_0} - e^{x_1})(e^{y_0} - e^{y_1})  (*)
 *                                                                          if x_0 <= x_1 <= y_0 <= y_1
 * (*) lambda coefficient is omitted for simplicity.
 *
 * For a group of segments {S_xi}, i = {0, n} total joint cost with segment S_y could be calculated as:
 *
 *        cost(Y, Y) = \sum cost(X_i, Y) =  e^{y_0 + y_1} (e^{y_0} - e^{y_1}) \sum (e^{xi_0} - e^{xi_1})
 *                                                                          if xi_0 <= xi_1 <= y_0 <= y_1
 * and
 *        cost(Y, Y) = \sum cost(X_i, Y) = (e^{y_0} - e^{y_1}) \sum e^{xi_0 + xi_1} (e^{xi_0} - e^{xi_1})
 *                                                                          if y_0 <= y_1 <= xi_0 <= xi_1
 *
 * SegmentsCostCache stores pre-computed sums for a group of segments {S_xi}:
 *
 *      1) \sum (e^{xi_0} - e^{xi_1})                      ->  leftSum
 *      2) \sum e^{xi_0 + xi_1} (e^{xi_0} - e^{xi_1})      ->  rightSum
 *
 * so that calculation of joint cost function for segment S_y became a O(1 + m) complexity task, where m
 * is the number of segments in {S_xi} that overlaps S_y.
 *
 * Segments are stored in buckets. Bucket is a subset of segments contained in SegmentsCostCache, so that
 * startTime of all segments inside a bucket are in the same time interval (with some granularity):
 *
 *  |------------------------|--------------------------|-----------------------|--------  ....
 *  t_0                    t_0+D                     t_0 + 2D                t0 + 3D       ....
 *      S_x1  S_x2  S_x3          S_x4  S_x5  S_x6          S_x7  S_x8  S_x9
 *         bucket1                  bucket2                    bucket3
 *
 *
 * Reasons to store segments in Buckets:
 *
 * Unlike SegmentsCostCache updates are fast, and we can make do without buckets ideally.
 * Unfortunately, large values for (time - bucketStart) cause overflows
 * A threshold (say 1Y) is used for bucketing, and this allows us to compute cost within O(logN) for a bucket
 *
 * If the interval duration exeeds it, we have to use CostBalancerStrategy#intervalCost over all the intervals
 * This scenario has a complexity of O(M) where M is the number of "adhoc" buckets.
 */
public class SegmentsCostCacheV3
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

  // The max interval that can be added to a bucket
  private static final long INTERVAL_THRESHOLD = TimeUnit.DAYS.toMillis(366);
  // exp(BUCKET_INTERVAL + INTERVAL_THRESHOLD + 2 * LIFE_THRESHOLD) must be within limits
  private static final long BUCKET_INTERVAL = TimeUnit.DAYS.toMillis(90);
  private static final DurationGranularity BUCKET_GRANULARITY = new DurationGranularity(BUCKET_INTERVAL, 0);

  private static final Comparator<Bucket> BUCKET_INTERVAL_COMPARATOR =
      Comparator.comparing(Bucket::getInterval, Comparators.intervalsByStartThenEnd());

  private static final Ordering<Bucket> BUCKET_ORDERING = Ordering.from(BUCKET_INTERVAL_COMPARATOR);

  private final ArrayList<Bucket> sortedBuckets;
  private final ArrayList<Interval> intervals;
  private final ArrayList<Pair<Double, Double>> adhocNormalizedIntervals;

  private final int allGranularitySegmentCount;
  private double allGranularitySegmentCost = -1;

  SegmentsCostCacheV3(ArrayList<Bucket> sortedBuckets,
                      ArrayList<Pair<Double, Double>> adhocNormalizedIntervals,
                      int allGranularitySegmentCount)
  {
    this.sortedBuckets = Preconditions.checkNotNull(sortedBuckets, "buckets should not be null");
    Preconditions.checkArgument(
        BUCKET_ORDERING.isOrdered(sortedBuckets),
        "buckets must be ordered by interval"
    );
    this.intervals = sortedBuckets.stream().map(Bucket::getInterval).collect(Collectors.toCollection(ArrayList::new));
    this.adhocNormalizedIntervals = Preconditions.checkNotNull(adhocNormalizedIntervals, "adhocIntervals should not be null");
    this.allGranularitySegmentCount = allGranularitySegmentCount;
  }

  public double cost(DataSegment segment)
  {
    boolean allGranularity = isAllGranularity(segment);
    if (allGranularity && allGranularitySegmentCost >= 0) {
      return allGranularitySegmentCost;
    }

    double cost = 0.0;
    int index = Collections.binarySearch(intervals, segment.getInterval(), Comparators.intervalsByStartThenEnd());
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

    // O(1) -> for ALL granularity segments
    double allStart = JodaUtils.MIN_INSTANT / MILLIS_FACTOR;
    double allEnd = JodaUtils.MAX_INSTANT / MILLIS_FACTOR;
    cost += allGranularitySegmentCount * CostBalancerStrategy.intervalCost(allEnd - allStart, start - allStart, end - allStart);

    // O(M) -> M adhoc buckets
    for (Pair<Double, Double> adhoc : adhocNormalizedIntervals) {
      cost += CostBalancerStrategy.intervalCost(adhoc.rhs - adhoc.lhs, start - adhoc.lhs, end - adhoc.lhs);
    }

    cost *= NORMALIZATION_FACTOR;

    // store cost for all granularity adhoc bucket for faster computation
    if (allGranularity) {
      allGranularitySegmentCost = cost;
    }

    return cost;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  private static boolean isAllGranularity(DataSegment segment)
  {
    return segment.getInterval().getStartMillis() == JodaUtils.MIN_INSTANT
           && segment.getInterval().getEndMillis() == JodaUtils.MAX_INSTANT;
  }


  public static class Builder
  {
    private final NavigableMap<Interval, Bucket.Builder> buckets = new TreeMap<>(Comparators.intervalsByStartThenEnd());

    private final HashSet<SegmentId> allGranularitySegments = new HashSet<>();
    private final HashSet<SegmentId> adhocSegments = new HashSet<>();

    public Builder addSegment(DataSegment segment)
    {
      if (isAllGranularity(segment)) {
        if (!allGranularitySegments.add(segment.getId())) {
          throw new ISE("expect new segment");
        }
      } else if (isAdhoc(segment)) {
        if (!adhocSegments.add(segment.getId())) {
          throw new ISE("expect new segment");
        }
      } else {
        Bucket.Builder builder = buckets.computeIfAbsent(getBucketInterval(segment), Bucket::builder);
        builder.addSegment(segment);
      }
      return this;
    }

    public Builder removeSegment(DataSegment segment)
    {
      if (isAllGranularity(segment)) {
        allGranularitySegments.remove(segment.getId());
      }
      if (isAdhoc(segment)) {
        adhocSegments.remove(segment.getId());
      } else {
        Interval interval = getBucketInterval(segment);
        buckets.computeIfPresent(
            interval,
            // If there are no move segments, returning null in computeIfPresent() removes the interval from the buckets
            // map
            (i, builder) -> builder.removeSegment(segment).isEmpty() ? null : builder
        );
      }
      return this;
    }

    public boolean isEmpty()
    {
      return buckets.isEmpty() && allGranularitySegments.isEmpty() && adhocSegments.isEmpty();
    }

    public SegmentsCostCacheV3 build()
    {
      final int allGranularitySegmentCount = allGranularitySegments.size();

      final ArrayList<Pair<Double, Double>> adhocNormalizedIntervals = new ArrayList<>();
      for (SegmentId segment : adhocSegments) {
        double normalizedStart = segment.getInterval().getStartMillis() / MILLIS_FACTOR;
        double normalizedEnd = segment.getInterval().getEndMillis() / MILLIS_FACTOR;
        adhocNormalizedIntervals.add(Pair.of(normalizedStart, normalizedEnd));
      }

      return new SegmentsCostCacheV3(
          buckets
              .values()
              .stream()
              .map(Bucket.Builder::build)
              .collect(Collectors.toCollection(ArrayList::new)),
          adhocNormalizedIntervals,
          allGranularitySegmentCount
      );
    }

    private static Interval getBucketInterval(DataSegment segment)
    {
      return BUCKET_GRANULARITY.bucket(segment.getInterval().getStart());
    }

    private boolean isAdhoc(DataSegment segment)
    {
      double duration = segment.getInterval().getEndMillis() / MILLIS_FACTOR
                        - segment.getInterval().getStartMillis() / MILLIS_FACTOR;
      return duration > INTERVAL_THRESHOLD / MILLIS_FACTOR;
    }
  }

  static class Bucket
  {
    private final Interval interval;
    private final Interval calculationInterval;

    private final long START;
    private final long END;
    private final double END_VAL;
    private final double END_EXP;
    private final double END_EXP_INV;

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
      this.interval = Preconditions.checkNotNull(interval, "interval");

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

      START = Math.max(interval.getStartMillis(), start[0]);
      END = Math.min(interval.getEndMillis(), end[n - 1]);

      END_VAL = getVal(END);
      END_EXP = FastMath.exp(END_VAL);
      END_EXP_INV = FastMath.exp(-END_VAL);

      startValSum = new double[n + 1];
      startExpSum = new double[n + 1];
      startExpInvSum = new double[n + 1];
      for (int i = 0; i < n; i++) {
        double startVal = getVal(start[i]);
        startValSum[i + 1] = startValSum[i] + startVal;
        startExpSum[i + 1] = startExpSum[i] + FastMath.exp(startVal);
        startExpInvSum[i + 1] = startExpInvSum[i] + FastMath.exp(-startVal);
      }

      endValSum = new double[n + 1];
      endExpSum = new double[n + 1];
      endExpInvSum = new double[n + 1];
      for (int i = 0; i < n; i++) {
        double endVal = getVal(end[i]);
        endValSum[i + 1] = endValSum[i] + endVal;
        endExpSum[i + 1] = endExpSum[i] + FastMath.exp(endVal);
        endExpInvSum[i + 1] = endExpInvSum[i] + FastMath.exp(-endVal);
      }
    }

    Interval getInterval()
    {
      return interval;
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

      // The following bounds help avoid overflow. The cost beyond LIFE_THRESHOLD is insignificant anyway
      long x = Math.max(dataSegment.getInterval().getStartMillis(), START - LIFE_THRESHOLD);
      long y = Math.min(dataSegment.getInterval().getEndMillis(), END + LIFE_THRESHOLD);
      double cost = 0;

      cost += solve(x, y, start, startValSum, startExpSum, startExpInvSum);
      cost -= solve(x, y, end, endValSum, endExpSum, endExpInvSum);

      return cost;
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

      double cost = 0;

      if (END < x) {

        // val , END , x , y
        cost += expSum[n] * yExpInv;
        cost -= expSum[n] * xExpInv;
        cost += n * END_EXP * xExpInv;
        cost -= n * END_EXP * yExpInv;

      } else if (END > y) {

        int l = lowerBound(0, n - 1, x, vals);
        int r = upperBound(0, n - 1, y, vals);

        // val < j , y , E
        cost += 2 * (l + 1) * yVal;
        cost -= 2 * (l + 1) * xVal;
        cost += expSum[l + 1] * yExpInv;
        cost -= expSum[l + 1] * xExpInv;
        cost += (l + 1) * xExp * END_EXP_INV;
        cost -= (l + 1) * yExp * END_EXP_INV;

        // x <= val <= y , E
        cost += 2 * (r - l - 1) * yVal;
        cost -= 2 * (sum[r] - sum[l + 1]);
        cost += (r - l - 1) * xExp * END_EXP_INV;
        cost -= xExp * (expInvSum[r] - expInvSum[l + 1]);
        cost -= (r - l - 1) * yExp * END_EXP_INV;
        cost += (expSum[r] - expSum[l + 1]) * yExpInv;

        // x , y < val , E
        cost += (n - r) * xExp * END_EXP_INV;
        cost -= xExp * (expInvSum[n] - expInvSum[r]);
        cost -= (n - r) * yExp * END_EXP_INV;
        cost += yExp * (expInvSum[n] - expInvSum[r]);

      } else {

        int l = lowerBound(0, n - 1, x, vals);

        // val < x , END , y
        cost += 2 * (l + 1) * END_VAL;
        cost -= 2 * (l + 1) * xVal;
        cost += expSum[l + 1] * yExpInv;
        cost -= expSum[l + 1] * xExpInv;
        cost -= (l + 1) * END_EXP * yExpInv;
        cost += (l + 1) * xExp * END_EXP_INV;

        // x <= val , END , y
        cost += 2 * (n - l - 1) * END_VAL;
        cost -= 2 * (sum[n] - sum[l + 1]);
        cost += (n - l - 1) * xExp * END_EXP_INV;
        cost -= xExp * (expInvSum[n] - expInvSum[l + 1]);
        cost += (expSum[n] - expSum[l + 1]) * yExpInv;
        cost -= (n - l - 1) * END_EXP * yExpInv;
      }

      return cost;
    }

    private double getVal(long millis)
    {
      return millis / MILLIS_FACTOR - START / MILLIS_FACTOR;
    }

    private int lowerBound(int l, int r, long x, long[] a)
    {
      if (l == r) {
        return a[l] < x ? r : l - 1;
      }
      int m = (l + r + 1) / 2;
      if (a[m] < x) {
        return lowerBound(m, r, x, a);
      } else {
        return lowerBound(l, m - 1, x, a);
      }
    }

    private int upperBound(int l, int r, long x, long[] a)
    {
      if (l == r) {
        return a[r] > x ? l : r + 1;
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

      public Builder removeSegment(DataSegment dataSegment)
      {
        segmentSet.remove(dataSegment.getId());

        return this;
      }

      public boolean isEmpty()
      {
        return segmentSet.isEmpty();
      }

      public Bucket build()
      {
        long bucketEndMillis = interval.getEndMillis();

        List<Pair<Long, Long>> intervals = new ArrayList<>();

        for (SegmentId segment : segmentSet) {
          Interval i = segment.getInterval();
          intervals.add(Pair.of(i.getStartMillis(), i.getEndMillis()));
          bucketEndMillis = Math.max(bucketEndMillis, i.getEndMillis());
        }

        return new Bucket(Intervals.utc(interval.getStartMillis(), bucketEndMillis), intervals);
      }
    }
  }
}
