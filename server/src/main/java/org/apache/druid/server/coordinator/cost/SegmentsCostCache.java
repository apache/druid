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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.server.coordinator.CostBalancerStrategy;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
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
 * Reasons to store segments in Buckets:
 *
 *     1) Cost function tends to 0 as distance between segments' intervals increases; buckets
 *        are used to avoid redundant 0 calculations for thousands of times
 *     2) To reduce number of calculations when segment is added or removed from SegmentsCostCache
 *     3) To avoid infinite values during exponents calculations
 *
 */
public class SegmentsCostCache
{
  /**
   * HALF_LIFE_DAYS defines how fast joint cost function tends to 0 as distance between segments' intervals increasing.
   * The value of 1 day means that cost function of co-locating two segments which have 1 days between their intervals
   * is 0.5 of the cost, if the intervals are adjacent. If the distance is 2 days, then 0.25, etc.
   */
  private static final double HALF_LIFE_DAYS = 1.0;
  private static final double LAMBDA = Math.log(2) / HALF_LIFE_DAYS;
  private static final double MILLIS_FACTOR = TimeUnit.DAYS.toMillis(1) / LAMBDA;

  /**
   * LIFE_THRESHOLD is used to avoid calculations for segments that are "far"
   * from each other and thus cost(Y,Y) ~ 0 for these segments
   */
  private static final long LIFE_THRESHOLD = TimeUnit.DAYS.toMillis(30);

  /**
   * Bucket interval defines duration granularity for segment buckets. Number of buckets control the trade-off
   * between updates (add/remove segment operation) and joint cost calculation:
   *        1) updates complexity is increasing when number of buckets is decreasing (as buckets contain more segments)
   *        2) joint cost calculation complexity is increasing with increasing of buckets number
   */
  private static final long BUCKET_INTERVAL = TimeUnit.DAYS.toMillis(15);
  private static final DurationGranularity BUCKET_GRANULARITY = new DurationGranularity(BUCKET_INTERVAL, 0);

  private static final Comparator<Interval> INTERVAL_COMPARATOR = Comparators.intervalsByStartThenEnd();

  private static final Comparator<DataSegment> SEGMENT_INTERVAL_COMPARATOR =
      Comparator.comparing(DataSegment::getInterval, Comparators.intervalsByStartThenEnd());

  private static final Comparator<Bucket> BUCKET_INTERVAL_COMPARATOR =
      Comparator.comparing(Bucket::getInterval, Comparators.intervalsByStartThenEnd());

  private static final Ordering<Interval> INTERVAL_ORDERING = Ordering.from(Comparators.intervalsByStartThenEnd());
  private static final Ordering<DataSegment> SEGMENT_ORDERING = Ordering.from(SEGMENT_INTERVAL_COMPARATOR);
  private static final Ordering<Bucket> BUCKET_ORDERING = Ordering.from(BUCKET_INTERVAL_COMPARATOR);

  private final ArrayList<Bucket> sortedBuckets;
  private final ArrayList<Interval> intervals;

  SegmentsCostCache(ArrayList<Bucket> sortedBuckets)
  {
    this.sortedBuckets = Preconditions.checkNotNull(sortedBuckets, "buckets should not be null");
    this.intervals = sortedBuckets.stream().map(Bucket::getInterval).collect(Collectors.toCollection(ArrayList::new));
    Preconditions.checkArgument(
        BUCKET_ORDERING.isOrdered(sortedBuckets),
        "buckets must be ordered by interval"
    );
  }

  public double cost(DataSegment segment)
  {
    double cost = 0.0;
    int index = Collections.binarySearch(intervals, segment.getInterval(), Comparators.intervalsByStartThenEnd());
    index = (index >= 0) ? index : -index - 1;

    for (ListIterator<Bucket> it = sortedBuckets.listIterator(index); it.hasNext(); ) {
      Bucket bucket = it.next();
      if (!bucket.inCalculationInterval(segment)) {
        break;
      }
      cost += bucket.cost(segment);
    }

    for (ListIterator<Bucket> it = sortedBuckets.listIterator(index); it.hasPrevious(); ) {
      Bucket bucket = it.previous();
      if (!bucket.inCalculationInterval(segment)) {
        break;
      }
      cost += bucket.cost(segment);
    }

    return cost;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private NavigableMap<Interval, Bucket.Builder> buckets = new TreeMap<>(Comparators.intervalsByStartThenEnd());

    public Builder addSegment(DataSegment segment)
    {
      Bucket.Builder builder = buckets.computeIfAbsent(getBucketInterval(segment), Bucket::builder);
      builder.addSegment(segment);
      return this;
    }

    public Builder removeSegment(DataSegment segment)
    {
      Interval interval = getBucketInterval(segment);
      buckets.computeIfPresent(
          interval,
          // If there are no move segments, returning null in computeIfPresent() removes the interval from the buckets
          // map
          (i, builder) -> builder.removeSegment(segment).isEmpty() ? null : builder
      );
      return this;
    }

    public boolean isEmpty()
    {
      return buckets.isEmpty();
    }

    public SegmentsCostCache build()
    {
      return new SegmentsCostCache(
          buckets
              .values()
              .stream()
              .map(Bucket.Builder::build)
              .collect(Collectors.toCollection(ArrayList::new))
      );
    }

    private static Interval getBucketInterval(DataSegment segment)
    {
      return BUCKET_GRANULARITY.bucket(segment.getInterval().getStart());
    }
  }

  static class Bucket
  {
    private final Interval interval;
    private final Interval calculationInterval;
    private final ArrayList<Interval> sortedIntervals;
    private final double[] leftSum;
    private final double[] rightSum;

    Bucket(Interval interval, ArrayList<Interval> sortedIntervals, double[] leftSum, double[] rightSum)
    {
      this.interval = Preconditions.checkNotNull(interval, "interval");
      this.sortedIntervals = Preconditions.checkNotNull(sortedIntervals, "sortedSegments");
      this.leftSum = Preconditions.checkNotNull(leftSum, "leftSum");
      this.rightSum = Preconditions.checkNotNull(rightSum, "rightSum");
      Preconditions.checkArgument(sortedIntervals.size() == leftSum.length && sortedIntervals.size() == rightSum.length);
      Preconditions.checkArgument(INTERVAL_ORDERING.isOrdered(sortedIntervals));
      this.calculationInterval = new Interval(
          interval.getStart().minus(LIFE_THRESHOLD),
          interval.getEnd().plus(LIFE_THRESHOLD)
      );
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
      // cost is calculated relatively to bucket start (which is considered as 0)
      double t0 = convertStart(dataSegment.getInterval(), interval);
      double t1 = convertEnd(dataSegment.getInterval(), interval);

      // avoid calculation for segments outside of LIFE_THRESHOLD
      if (!inCalculationInterval(dataSegment)) {
        throw new ISE("Segment is not within calculation interval");
      }

      int index = Collections.binarySearch(sortedIntervals, dataSegment.getInterval(), INTERVAL_COMPARATOR);
      index = (index >= 0) ? index : -index - 1;
      return addLeftCost(dataSegment, t0, t1, index) + rightCost(dataSegment, t0, t1, index);
    }

    private double addLeftCost(DataSegment dataSegment, double t0, double t1, int index)
    {
      double leftCost = 0.0;
      // add to cost all left-overlapping segments
      int leftIndex = index - 1;
      while (leftIndex >= 0
             && sortedIntervals.get(leftIndex).overlaps(dataSegment.getInterval())) {
        double start = convertStart(sortedIntervals.get(leftIndex), interval);
        double end = convertEnd(sortedIntervals.get(leftIndex), interval);
        leftCost += CostBalancerStrategy.intervalCost(end - start, t0 - start, t1 - start);
        --leftIndex;
      }
      // add left-non-overlapping segments
      if (leftIndex >= 0) {
        leftCost += leftSum[leftIndex] * (FastMath.exp(-t1) - FastMath.exp(-t0));
      }
      return leftCost;
    }

    private double rightCost(DataSegment dataSegment, double t0, double t1, int index)
    {
      double rightCost = 0.0;
      // add all right-overlapping segments
      int rightIndex = index;
      while (rightIndex < sortedIntervals.size() &&
             sortedIntervals.get(rightIndex).overlaps(dataSegment.getInterval())) {
        double start = convertStart(sortedIntervals.get(rightIndex), interval);
        double end = convertEnd(sortedIntervals.get(rightIndex), interval);
        rightCost += CostBalancerStrategy.intervalCost(t1 - t0, start - t0, end - t0);
        ++rightIndex;
      }
      // add right-non-overlapping segments
      if (rightIndex < sortedIntervals.size()) {
        rightCost += rightSum[rightIndex] * (FastMath.exp(t0) - FastMath.exp(t1));
      }
      return rightCost;
    }

    private static double convertStart(Interval interval, Interval reference)
    {
      return toLocalInterval(interval.getStartMillis(), reference);
    }

    private static double convertEnd(Interval interval, Interval reference)
    {
      return toLocalInterval(interval.getEndMillis(), reference);
    }

    private static double toLocalInterval(long millis, Interval interval)
    {
      return millis / MILLIS_FACTOR - interval.getStartMillis() / MILLIS_FACTOR;
    }

    public static Builder builder(Interval interval)
    {
      return new Builder(interval);
    }

    static class Builder
    {
      protected final Interval interval;
      private final SegmentTreap treap = new SegmentTreap();

      public Builder(Interval interval)
      {
        this.interval = interval;
      }

      public Builder addSegment(DataSegment dataSegment)
      {
        if (!interval.contains(dataSegment.getInterval().getStartMillis())) {
          throw new ISE("Failed to add segment to bucket: interval is not covered by this bucket");
        }

        // all values are pre-computed relatively to bucket start (which is considered as 0)
        double t0 = convertStart(dataSegment.getInterval(), interval);
        double t1 = convertEnd(dataSegment.getInterval(), interval);

        double leftValue = FastMath.exp(t0) - FastMath.exp(t1);
        double rightValue = FastMath.exp(-t1) - FastMath.exp(-t0);

        SegmentAndSum segmentAndSum = new SegmentAndSum(dataSegment, leftValue, rightValue);

        // left/right value should be added to left/right sums for elements greater/lower than current segment
        treap.update(segmentAndSum, Pair.of(leftValue, 0.0), true);
        treap.update(segmentAndSum, Pair.of(0.0, rightValue), false);

        // leftSum_i = leftValue_i + \sum leftValue_j = leftValue_i + leftSum_{i-1} , j < i
        SegmentAndSum lower = treap.lower(segmentAndSum).val;
        if (lower != null) {
          segmentAndSum.leftSum = leftValue + lower.leftSum;
        }

        // rightSum_i = rightValue_i + \sum rightValue_j = rightValue_i + rightSum_{i+1} , j > i
        SegmentAndSum higher = treap.upper(segmentAndSum).val;
        if (higher != null) {
          segmentAndSum.rightSum = rightValue + higher.rightSum;
        }

        if (!treap.insert(segmentAndSum)) {
          throw new ISE("expect new segment");
        }
        return this;
      }

      public Builder removeSegment(DataSegment dataSegment)
      {
        SegmentAndSum segmentAndSum = new SegmentAndSum(dataSegment, 0.0, 0.0);

        if (!treap.remove(segmentAndSum)) {
          return this;
        }

        double t0 = convertStart(dataSegment.getInterval(), interval);
        double t1 = convertEnd(dataSegment.getInterval(), interval);

        double leftValue = FastMath.exp(t0) - FastMath.exp(t1);
        double rightValue = FastMath.exp(-t1) - FastMath.exp(-t0);

        treap.update(segmentAndSum, Pair.of(-leftValue, 0.0), true);
        treap.update(segmentAndSum, Pair.of(0.0, -rightValue), false);

        return this;
      }

      public boolean isEmpty()
      {
        return treap.isEmpty();
      }

      public Bucket build()
      {
        ArrayList<Interval> intervalsList = new ArrayList<>();
        double[] leftSum = new double[treap.root.size];
        double[] rightSum = new double[treap.root.size];
        int i = 0;
        for (SegmentAndSum segmentAndSum : treap.toList()) {
          intervalsList.add(segmentAndSum.dataSegment.getInterval());
          leftSum[i] = segmentAndSum.leftSum;
          rightSum[i] = segmentAndSum.rightSum;
          ++i;
        }
        long bucketEndMillis = intervalsList
            .stream()
            .mapToLong(interval -> interval.getEndMillis())
            .max()
            .orElseGet(interval::getEndMillis);
        return new Bucket(Intervals.utc(interval.getStartMillis(), bucketEndMillis), intervalsList, leftSum, rightSum);
      }
    }
  }

  static class SegmentAndSum implements Comparable<SegmentAndSum>
  {
    private final DataSegment dataSegment;
    private double leftSum;
    private double rightSum;

    SegmentAndSum(DataSegment dataSegment, double leftSum, double rightSum)
    {
      this.dataSegment = dataSegment;
      this.leftSum = leftSum;
      this.rightSum = rightSum;
    }

    @Override
    public int compareTo(SegmentAndSum o)
    {
      int c = Comparators.intervalsByStartThenEnd().compare(dataSegment.getInterval(), o.dataSegment.getInterval());
      return (c != 0) ? c : dataSegment.compareTo(o.dataSegment);
    }

    @Override
    public boolean equals(Object obj)
    {
      throw new UnsupportedOperationException("Use SegmentAndSum.compareTo()");
    }

    @Override
    public int hashCode()
    {
      throw new UnsupportedOperationException();
    }
  }


  abstract static class Treap<X extends Comparable<X>, Y>
  {
    protected TreapNode root;
    protected final TreapNode NULL;

    public Treap()
    {
      NULL = new TreapNode(null);
      NULL.left = NULL.right = NULL;
      NULL.priority = Double.POSITIVE_INFINITY;
      root = NULL;
    }

    public boolean isEmpty()
    {
      return NULL.equals(root);
    }

    public boolean contains(X val)
    {
      return contains(val, root);
    }

    public TreapNode lower(X val)
    {
      return lower(val, root);
    }

    public TreapNode upper(X val)
    {
      return upper(val, root);
    }

    public TreapNode floor(X val)
    {
      return floor(val, root);
    }

    public TreapNode ceil(X val)
    {
      return ceil(val, root);
    }

    public boolean insert(X val)
    {
      int oldSize = root.size;
      root = insert(new TreapNode(val), root);
      return root.size > oldSize;
    }

    public boolean remove(X val)
    {
      int oldSize = root.size;
      root = remove(val, root);
      return root.size < oldSize;
    }

    public TreapNode getMin()
    {
      TreapNode node = root;
      while (!NULL.equals(node.left)) {
        node = node.left;
      }
      return node;
    }

    public TreapNode getMax()
    {
      TreapNode node = root;
      while (!NULL.equals(node.right)) {
        node = node.right;
      }
      return node;
    }

    public Y query()
    {
      return root.sum;
    }

    public void update(X val, Y lazy, boolean dir)
    {
      if (dir) {
        root = update(root, val, null, lazy);
      } else {
        root = update(root, null, val, lazy);
      }
    }

    protected abstract Y getVal(X val);

    protected abstract X setVal(X val, Y lazy);

    protected abstract Y add(Y a, Y b);

    protected abstract Y multiply(int a, Y b);

    protected abstract Y zero();

    private boolean contains(X val, TreapNode node)
    {
      if (NULL.equals(node)) {
        return false;
      }
      final int cmp = val.compareTo(node.val);
      if (cmp < 0) {
        return contains(val, node.left);
      }
      if (cmp > 0) {
        return contains(val, node.right);
      }
      return true;
    }

    private TreapNode lower(X val, TreapNode node)
    {
      if (NULL.equals(node)) {
        return node;
      }
      final int cmp = val.compareTo(node.val);
      if (cmp <= 0) {
        return lower(val, node.left);
      } else {
        TreapNode ret = lower(val, node.right);
        return (NULL.equals(ret)) ? node : ret;
      }
    }

    private TreapNode upper(X val, TreapNode node)
    {
      if (NULL.equals(node)) {
        return node;
      }
      final int cmp = val.compareTo(node.val);
      if (cmp >= 0) {
        return upper(val, node.right);
      } else {
        TreapNode ret = upper(val, node.left);
        return (NULL.equals(ret)) ? node : ret;
      }
    }

    private TreapNode floor(X val, TreapNode node)
    {
      if (NULL.equals(node)) {
        return node;
      }
      final int cmp = val.compareTo(node.val);
      if (cmp < 0) {
        return floor(val, node.left);
      } else {
        TreapNode ret = floor(val, node.right);
        return (NULL.equals(ret)) ? node : ret;
      }
    }

    private TreapNode ceil(X val, TreapNode node)
    {
      if (NULL.equals(node)) {
        return node;
      }
      final int cmp = val.compareTo(node.val);
      if (cmp > 0) {
        return ceil(val, node.right);
      } else {
        TreapNode ret = ceil(val, node.left);
        return (NULL.equals(ret)) ? node : ret;
      }
    }

    private TreapNode insert(TreapNode val, TreapNode node)
    {
      if (NULL.equals(node)) {
        return val;
      }
      Pair<TreapNode, TreapNode> pair = split(node, val.val);
      node = merge(pair.lhs, val);
      node = merge(node, pair.rhs);
      return node;
    }

    private TreapNode remove(X val, TreapNode node)
    {
      if (NULL.equals(node)) {
        return node;
      }
      Pair<TreapNode, TreapNode> pair = split(node, val);
      TreapNode lower = lower(val, pair.lhs);
      if (NULL.equals(lower)) {
        return pair.rhs;
      }
      return merge(split(pair.lhs, lower.val).lhs, pair.rhs);
    }

    private Pair<TreapNode, TreapNode> split(TreapNode node, X val)
    {
      if (NULL.equals(node)) {
        return Pair.of(NULL, NULL);
      }
      node.lazyPropogate();
      final int cmp = val.compareTo(node.val);
      Pair<TreapNode, TreapNode> pair;
      if (cmp < 0) {
        pair = split(node.left, val);
        node.left = pair.rhs;
        pair = Pair.of(pair.lhs, node);
      } else {
        pair = split(node.right, val);
        node.right = pair.lhs;
        pair = Pair.of(node, pair.rhs);
      }
      node.recompute();
      return pair;
    }

    private TreapNode merge(TreapNode left, TreapNode right)
    {
      if (NULL.equals(left)) {
        return right;
      }
      if (NULL.equals(right)) {
        return left;
      }
      left.lazyPropogate();
      right.lazyPropogate();
      TreapNode node;
      if (left.priority < right.priority) {
        left.right = merge(left.right, right);
        node = left;
      } else {
        right.left = merge(left, right.left);
        node = right;
      }
      node.recompute();
      return node;
    }

    private TreapNode update(TreapNode node, @Nullable X begin, @Nullable X end, Y lazy)
    {
      TreapNode left = NULL;
      TreapNode right = NULL;
      if (begin != null) {
        Pair<TreapNode, TreapNode> pair = split(node, begin);
        left = pair.lhs;
        node = pair.rhs;
      }
      if (end != null) {
        Pair<TreapNode, TreapNode> pair = split(node, end);
        node = pair.lhs;
        right = pair.rhs;
      }
      node.lazy = add(node.lazy, lazy);
      node = merge(left, node);
      node = merge(node, right);
      return node;
    }

    class TreapNode
    {
      X val;
      Y sum;
      Y lazy;
      TreapNode left;
      TreapNode right;
      double priority;
      int size;

      TreapNode(@Nullable X val)
      {
        this(val, NULL, NULL);
        if (val != null) {
          sum = getVal(val);
          size = 1;
        }
      }

      TreapNode(@Nullable X val, @Nullable TreapNode left, @Nullable TreapNode right)
      {
        this.val = val;
        this.left = left;
        this.right = right;
        this.priority = Math.random();
        this.sum = zero();
        this.lazy = zero();
      }

      void recompute()
      {
        if (NULL.equals(this)) {
          return;
        }
        size = 1 + left.size + right.size;
        sum = getVal(val);
        left.lazyPropogate();
        right.lazyPropogate();
        sum = add(sum, add(left.sum, right.sum));
      }

      void lazyPropogate()
      {
        if (NULL.equals(this)) {
          return;
        }
        val = setVal(val, lazy);
        sum = add(sum, multiply(size, lazy));
        if (!NULL.equals(left)) {
          left.lazy = add(left.lazy, lazy);
        }
        if (!NULL.equals(right)) {
          right.lazy = add(right.lazy, lazy);
        }
        lazy = zero();
      }

      @Override
      public boolean equals(Object that)
      {
        return this == that;
      }
    }
  }

  public static class TestX implements Comparable<TestX>
  {
    final String a;
    double b;

    public TestX(String a, double b)
    {
      this.a = a;
      this.b = b;
    }

    public String getA()
    {
      return a;
    }

    public double getB()
    {
      return b;
    }

    public void setB(double b)
    {
      this.b = b;
    }

    @Override
    public int compareTo(TestX that)
    {
      return a.compareTo(that.getA());
    }
  }

  public static class SegmentTreap extends Treap<SegmentAndSum, Pair<Double, Double>>
  {

    static final Pair<Double, Double> ZERO = Pair.of(0.0, 0.0);

    @Override
    protected Pair<Double, Double> getVal(SegmentAndSum val)
    {
      return Pair.of(val.leftSum, val.rightSum);
    }

    @Override
    protected SegmentAndSum setVal(SegmentAndSum val, Pair<Double, Double> lazy)
    {
      val.leftSum += lazy.lhs;
      val.rightSum += lazy.rhs;
      return val;
    }

    @Override
    protected Pair<Double, Double> zero()
    {
      return ZERO;
    }

    @Override
    protected Pair<Double, Double> add(Pair<Double, Double> a, Pair<Double, Double> b)
    {
      return Pair.of(a.lhs + b.lhs, a.rhs + b.rhs);
    }

    @Override
    protected Pair<Double, Double> multiply(int a, Pair<Double, Double> b)
    {
      return Pair.of(a * b.lhs, a * b.rhs);
    }

    public List<SegmentAndSum> toList()
    {
      List<SegmentAndSum> list = new ArrayList<>();
      accumulate(list, root);
      return list;
    }

    private void accumulate(List<SegmentAndSum> list, TreapNode node)
    {
      if (NULL.equals(node)) {
        return;
      }
      node.lazyPropogate();
      accumulate(list, node.left);
      list.add(node.val);
      accumulate(list, node.right);
    }
  }

  public static class TestTreap extends Treap<TestX, Double>
  {
    @Override
    protected Double getVal(TestX val)
    {
      return val.getB();
    }

    @Override
    protected TestX setVal(TestX val, Double lazy)
    {
      val.setB(val.getB() + lazy);
      return val;
    }

    @Override
    protected Double add(Double a, Double b)
    {
      return Double.sum(a, b);
    }

    @Override
    protected Double multiply(int a, Double b)
    {
      return a * b;
    }

    @Override
    protected Double zero()
    {
      return 0.0;
    }

    public void print()
    {
      print(this.root);
      System.out.println();
    }

    private void print(TreapNode node)
    {
      if (NULL.equals(node)) {
        return;
      }
      print(node.left);
      System.out.println(node.val.getA() + ", " + node.val.getB() + ", " + node.sum + ", " + node.lazy + ", " + node.priority);
      print(node.right);
    }
  }
}
