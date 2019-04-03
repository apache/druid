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

package org.apache.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FixedBucketsHistogram
{
  public static final byte SERIALIZATION_VERSION = 0x01;

  /**
   * Serialization header format:
   *
   * byte: serialization version, must be 0x01
   * byte: encoding mode, 0x01 for full, 0x02 for sparse
   */
  public static final int SERDE_HEADER_SIZE = Byte.BYTES +
                                              Byte.BYTES;

  /**
   * Common serialization fields format:
   *
   * double: lowerLimit
   * double: upperLimit
   * int: numBuckets
   * byte: outlier handling mode (0x00 for `ignore`, 0x01 for `overflow`, and 0x02 for `clip`)
   * long: count, total number of values contained in the histogram, excluding outliers
   * long: lowerOutlierCount
   * long: upperOutlierCount
   * long: missingValueCount
   * double: max
   * double: min
   */
  public static final int COMMON_FIELDS_SIZE = Double.BYTES +
                                                Double.BYTES +
                                                Integer.BYTES +
                                                Byte.BYTES +
                                                Long.BYTES * 4 +
                                                Double.BYTES +
                                                Double.BYTES;

  /**
   * Full serialization format:
   *
   * serialization header
   * common fields
   * array of longs: bucket counts for the histogram
   */
  public static final byte FULL_ENCODING_MODE = 0x01;

  /**
   * Sparse serialization format:
   *
   * serialization header
   * common fields
   * int: number of following (bucketNum, count) pairs
   * sequence of (int, long) pairs:
   *  int: bucket number
   *  count: bucket count
   */
  public static final byte SPARSE_ENCODING_MODE = 0x02;

  /**
   * Determines how the the histogram handles outliers.
   *
   * Ignore:   do not track outliers at all
   * Overflow: track outlier counts in upperOutlierCount and lowerOutlierCount.
   *           The min and max do not take outlier values into account.
   * Clip:     Clip outlier values to either the start or end of the histogram's range, adding them to the first or
   *           last bucket, respectively. The min and max are affected by such clipped outlier values.
   */
  public enum OutlierHandlingMode
  {
    IGNORE,
    OVERFLOW,
    CLIP;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static OutlierHandlingMode fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }

    public byte[] getCacheKey()
    {
      return new byte[] {(byte) this.ordinal()};
    }
  }

  @JsonIgnore
  private final OutlierHandler outlierHandler;

  /**
   * Locking is needed when the non-buffer aggregator is used within a realtime ingestion task.
   *
   * The following areas are locked:
   * - Add value
   * - Merge histograms
   * - Compute percentiles
   * - Serialization
   */
  @JsonIgnore
  private final ReadWriteLock readWriteLock;

  private double lowerLimit;
  private double upperLimit;
  private int numBuckets;

  private long upperOutlierCount = 0;
  private long lowerOutlierCount = 0;
  private long missingValueCount = 0;
  private long[] histogram;
  private double bucketSize;
  private OutlierHandlingMode outlierHandlingMode;

  private long count = 0;
  private double max = Double.NEGATIVE_INFINITY;
  private double min = Double.POSITIVE_INFINITY;

  public FixedBucketsHistogram(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      OutlierHandlingMode outlierHandlingMode
  )
  {
    Preconditions.checkArgument(
        upperLimit > lowerLimit,
        "Upper limit [%s] must be greater than lower limit [%s].",
        upperLimit,
        lowerLimit
    );
    Preconditions.checkArgument(
        numBuckets > 0,
        "numBuckets must be > 0, got [%s] instead.",
        numBuckets
    );
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.numBuckets = numBuckets;
    this.outlierHandlingMode = outlierHandlingMode;

    this.histogram = new long[numBuckets];
    this.bucketSize = (upperLimit - lowerLimit) / numBuckets;
    this.readWriteLock = new ReentrantReadWriteLock(true);

    this.outlierHandler = makeOutlierHandler();
  }

  @VisibleForTesting
  protected FixedBucketsHistogram(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      OutlierHandlingMode outlierHandlingMode,
      long[] histogram,
      long count,
      double max,
      double min,
      long lowerOutlierCount,
      long upperOutlierCount,
      long missingValueCount
  )
  {
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;
    this.numBuckets = numBuckets;
    this.outlierHandlingMode = outlierHandlingMode;
    this.histogram = histogram;
    this.count = count;
    this.max = max;
    this.min = min;
    this.upperOutlierCount = upperOutlierCount;
    this.lowerOutlierCount = lowerOutlierCount;
    this.missingValueCount = missingValueCount;

    this.bucketSize = (upperLimit - lowerLimit) / numBuckets;
    this.readWriteLock = new ReentrantReadWriteLock(true);
    this.outlierHandler = makeOutlierHandler();
  }

  @VisibleForTesting
  public FixedBucketsHistogram getCopy()
  {
    // only used for tests/benchmarks
    return new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode,
        Arrays.copyOf(histogram, histogram.length),
        count,
        max,
        min,
        lowerOutlierCount,
        upperOutlierCount,
        missingValueCount
    );
  }

  @JsonProperty
  public double getLowerLimit()
  {
    return lowerLimit;
  }

  @JsonProperty
  public double getUpperLimit()
  {
    return upperLimit;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty
  public long getUpperOutlierCount()
  {
    return upperOutlierCount;
  }

  @JsonProperty
  public long getLowerOutlierCount()
  {
    return lowerOutlierCount;
  }

  @JsonProperty
  public long getMissingValueCount()
  {
    return missingValueCount;
  }

  @JsonProperty
  public long[] getHistogram()
  {
    return histogram;
  }

  @JsonProperty
  public double getBucketSize()
  {
    return bucketSize;
  }

  @JsonProperty
  public long getCount()
  {
    return count;
  }

  @JsonProperty
  public double getMax()
  {
    return max;
  }

  @JsonProperty
  public double getMin()
  {
    return min;
  }

  @JsonProperty
  public OutlierHandlingMode getOutlierHandlingMode()
  {
    return outlierHandlingMode;
  }

  @Override
  public String toString()
  {
    return "{" +
           "lowerLimit=" + lowerLimit +
           ", upperLimit=" + upperLimit +
           ", numBuckets=" + numBuckets +
           ", upperOutlierCount=" + upperOutlierCount +
           ", lowerOutlierCount=" + lowerOutlierCount +
           ", missingValueCount=" + missingValueCount +
           ", histogram=" + Arrays.toString(histogram) +
           ", outlierHandlingMode=" + outlierHandlingMode +
           ", count=" + count +
           ", max=" + max +
           ", min=" + min +
           '}';
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
    FixedBucketsHistogram that = (FixedBucketsHistogram) o;
    return Double.compare(that.getLowerLimit(), getLowerLimit()) == 0 &&
           Double.compare(that.getUpperLimit(), getUpperLimit()) == 0 &&
           getNumBuckets() == that.getNumBuckets() &&
           getUpperOutlierCount() == that.getUpperOutlierCount() &&
           getLowerOutlierCount() == that.getLowerOutlierCount() &&
           getMissingValueCount() == that.getMissingValueCount() &&
           Double.compare(that.getBucketSize(), getBucketSize()) == 0 &&
           getCount() == that.getCount() &&
           Double.compare(that.getMax(), getMax()) == 0 &&
           Double.compare(that.getMin(), getMin()) == 0 &&
           Arrays.equals(getHistogram(), that.getHistogram()) &&
           getOutlierHandlingMode() == that.getOutlierHandlingMode();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getLowerLimit(),
        getUpperLimit(),
        getNumBuckets(),
        getUpperOutlierCount(),
        getLowerOutlierCount(),
        getMissingValueCount(),
        Arrays.hashCode(getHistogram()),
        getBucketSize(),
        getOutlierHandlingMode(),
        getCount(),
        getMax(),
        getMin()
    );
  }

  public ReadWriteLock getReadWriteLock()
  {
    return readWriteLock;
  }

  /**
   * Add a value to the histogram, using the outlierHandler to account for outliers.
   *
   * @param value value to be added.
   */
  public void add(
      double value
  )
  {
    readWriteLock.writeLock().lock();

    try {
      if (value < lowerLimit) {
        outlierHandler.handleOutlierAdd(false);
        return;
      } else if (value >= upperLimit) {
        outlierHandler.handleOutlierAdd(true);
        return;
      }

      count += 1;
      if (value > max) {
        max = value;
      }
      if (value < min) {
        min = value;
      }

      double valueRelativeToRange = value - lowerLimit;
      int targetBucket = (int) (valueRelativeToRange / bucketSize);

      if (targetBucket >= histogram.length) {
        targetBucket = histogram.length - 1;
      }

      histogram[targetBucket] += 1;
    }
    finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Called when the histogram encounters a null value.
   */
  public void incrementMissing()
  {
    readWriteLock.writeLock().lock();

    try {
      missingValueCount++;
    }
    finally {
      readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Merge another histogram into this one. Only the state of this histogram is updated.
   *
   * If the two histograms have identical buckets, a simpler algorithm is used.
   *
   * @param otherHistogram
   */
  public void combineHistogram(FixedBucketsHistogram otherHistogram)
  {
    if (otherHistogram == null) {
      return;
    }

    readWriteLock.writeLock().lock();
    otherHistogram.getReadWriteLock().readLock().lock();

    try {
      missingValueCount += otherHistogram.getMissingValueCount();

      if (bucketSize == otherHistogram.getBucketSize() &&
          lowerLimit == otherHistogram.getLowerLimit() &&
          upperLimit == otherHistogram.getUpperLimit()) {
        combineHistogramSameBuckets(otherHistogram);
      } else {
        combineHistogramDifferentBuckets(otherHistogram);
      }
    }
    finally {
      readWriteLock.writeLock().unlock();
      otherHistogram.getReadWriteLock().readLock().unlock();
    }
  }

  /**
   * Merge another histogram that has the same range and same buckets.
   *
   * @param otherHistogram
   */
  private void combineHistogramSameBuckets(FixedBucketsHistogram otherHistogram)
  {
    long[] otherHistogramArray = otherHistogram.getHistogram();
    for (int i = 0; i < numBuckets; i++) {
      histogram[i] += otherHistogramArray[i];
    }

    count += otherHistogram.getCount();
    max = Math.max(max, otherHistogram.getMax());
    min = Math.min(min, otherHistogram.getMin());

    outlierHandler.handleOutliersForCombineSameBuckets(otherHistogram);
  }

  /**
   * Merge another histogram that has different buckets from mine.
   *
   * First, check if the other histogram falls entirely outside of my defined range. If so, call the appropriate
   * function for optimized handling.
   *
   * Otherwise, this merges the histograms with a more general function.
   *
   * @param otherHistogram
   */
  private void combineHistogramDifferentBuckets(FixedBucketsHistogram otherHistogram)
  {
    if (otherHistogram.getLowerLimit() >= upperLimit) {
      outlierHandler.handleOutliersCombineDifferentBucketsAllUpper(otherHistogram);
    } else if (otherHistogram.getUpperLimit() <= lowerLimit) {
      outlierHandler.handleOutliersCombineDifferentBucketsAllLower(otherHistogram);
    } else {
      simpleInterpolateMerge(otherHistogram);
    }
  }

  /**
   * Get a sum of bucket counts from either the start of a histogram's range or end, up to a specified cutoff value.
   *
   * For example, if I have the following histogram with a range of 0-40, with 4 buckets and
   * per-bucket counts of 5, 2, 10, and 7:
   *
   * |   5   |   2   |   24   |   7   |
   * 0       10      20       30      40
   *
   * Calling this function with a cutoff of 25 and fromStart = true would:
   * - Sum the first two bucket counts 5 + 2
   * - Since the cutoff falls in the third bucket, multiply the third bucket's count by the fraction of the bucket range
   *   covered by the cutoff, in this case the fraction is ((25 - 20) / 10) = 0.5
   * - The total count returned is 5 + 2 + 12
   *
   * @param cutoff Cutoff point within the histogram's range
   * @param fromStart If true, sum the bucket counts starting from the beginning of the histogram range.
   *                  If false, sum from the other direction, starting from the end of the histogram range.
   * @return Sum of bucket counts up to the cutoff point
   */
  private double getCumulativeCount(double cutoff, boolean fromStart)
  {
    int cutoffBucket = (int) ((cutoff - lowerLimit) / bucketSize);
    double count = 0;

    if (fromStart) {
      for (int i = 0; i <= cutoffBucket; i++) {
        if (i == cutoffBucket) {
          double bucketStart = i * bucketSize + lowerLimit;
          double partialCount = ((cutoff - bucketStart) / bucketSize) * histogram[i];
          count += partialCount;
        } else {
          count += histogram[i];
        }
      }
    } else {
      for (int i = cutoffBucket; i < histogram.length; i++) {
        if (i == cutoffBucket) {
          double bucketEnd = ((i + 1) * bucketSize) + lowerLimit;
          double partialCount = ((bucketEnd - cutoff) / bucketSize) * histogram[i];
          count += partialCount;
        } else {
          count += histogram[i];
        }
      }
    }
    return count;
  }

  /**
   * Combines this histogram with another histogram by "rebucketing" the other histogram to match our bucketing scheme,
   * assuming that the original input values are uniformly distributed within each bucket in the other histogram.
   *
   * Suppose we have the following histograms:
   *
   * |   0   |   0   |   0   |   0   |   0   |   0   |
   * 0       1       2       3       4       5       6
   *
   * |   4   |   4   |   0   |   0   |
   * 0       3       6       9       12
   *
   * We will preserve the bucketing scheme of our histogram, and determine what cut-off points within the other
   * histogram align with our bucket boundaries.
   *
   * Using this example, we would effectively rebucket the second histogram to the following:
   *
   * | 1.333 | 1.333 | 1.333 | 1.333 | 1.333 | 1.333 |   0   |   0   |   0   |   0   |   0   |   0   |
   * 0       1       2       3       4       5       6       7       8       9       10      11      12
   *
   * The 0-3 bucket in the second histogram is rebucketed across new buckets with size 1, with the original frequency 4
   * multiplied by the fraction (new bucket size / original bucket size), in this case 1/3.
   *
   * These new rebucketed counts are then added to the base histogram's buckets, with rounding, resulting in:
   *
   * |   1   |   1   |   1   |   1   |   1   |   1   |
   * 0       1       2       3       4       5       6
   *
   * @param otherHistogram other histogram to be merged
   */
  private void simpleInterpolateMerge(FixedBucketsHistogram otherHistogram)
  {
    double rangeStart = Math.max(lowerLimit, otherHistogram.getLowerLimit());
    double rangeEnd = Math.min(upperLimit, otherHistogram.getUpperLimit());

    int myCurBucket = (int) ((rangeStart - lowerLimit) / bucketSize);
    double myNextCursorBoundary = (myCurBucket + 1) * bucketSize + lowerLimit;
    double myCursor = rangeStart;

    int theirCurBucket = (int) ((rangeStart - otherHistogram.getLowerLimit()) / otherHistogram.getBucketSize());
    double theirNextCursorBoundary = (theirCurBucket + 1) * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();
    double theirCursor = rangeStart;

    double intraBucketStride = otherHistogram.getBucketSize() / otherHistogram.getHistogram()[theirCurBucket];

    myNextCursorBoundary = Math.min(myNextCursorBoundary, rangeEnd);
    theirNextCursorBoundary = Math.min(theirNextCursorBoundary, rangeEnd);

    outlierHandler.simpleInterpolateMergeHandleOutliers(otherHistogram, rangeStart, rangeEnd);

    double theirCurrentLowerBucketBoundary = theirCurBucket * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();

    while (myCursor < rangeEnd) {
      double needToConsume = myNextCursorBoundary - myCursor;
      double canConsumeFromOtherBucket = theirNextCursorBoundary - theirCursor;
      double toConsume = Math.min(needToConsume, canConsumeFromOtherBucket);

      // consume one of our bucket's worth of data from the other histogram
      while (needToConsume > 0) {
        double fractional = toConsume / otherHistogram.getBucketSize();
        double fractionalCount = otherHistogram.getHistogram()[theirCurBucket] * fractional;

        if (Math.round(fractionalCount) > 0) {
          // in this chunk from the other histogram bucket, calculate min and max interpolated values that would be added
          double minStride = Math.ceil(
              (theirCursor - (theirCurBucket * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit()))
              / intraBucketStride
          );

          minStride *= intraBucketStride;
          minStride += theirCurrentLowerBucketBoundary;
          if (minStride >= theirCursor) {
            min = Math.min(minStride, min);
            max = Math.max(minStride, max);
          }

          double maxStride = Math.floor(
              (theirCursor + toConsume - (theirCurBucket * otherHistogram.getBucketSize()
                                          + otherHistogram.getLowerLimit())) / intraBucketStride
          );
          maxStride *= intraBucketStride;
          maxStride += theirCurrentLowerBucketBoundary;
          if (maxStride < theirCursor + toConsume) {
            max = Math.max(maxStride, max);
            min = Math.min(maxStride, min);
          }
        }

        histogram[myCurBucket] += Math.round(fractionalCount);
        count += Math.round(fractionalCount);

        needToConsume -= toConsume;
        theirCursor += toConsume;
        myCursor += toConsume;


        if (theirCursor >= rangeEnd) {
          break;
        }

        // we've crossed buckets in the other histogram
        if (theirCursor >= theirNextCursorBoundary) {
          theirCurBucket += 1;
          intraBucketStride = otherHistogram.getBucketSize() / otherHistogram.getHistogram()[theirCurBucket];
          theirCurrentLowerBucketBoundary = theirCurBucket * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();
          theirNextCursorBoundary = (theirCurBucket + 1) * otherHistogram.getBucketSize() + otherHistogram.getLowerLimit();
          theirNextCursorBoundary = Math.min(theirNextCursorBoundary, rangeEnd);
        }

        canConsumeFromOtherBucket = theirNextCursorBoundary - theirCursor;
        toConsume = Math.min(needToConsume, canConsumeFromOtherBucket);
      }

      if (theirCursor >= rangeEnd) {
        break;
      }

      myCurBucket += 1;
      myNextCursorBoundary = (myCurBucket + 1) * bucketSize + lowerLimit;
      myNextCursorBoundary = Math.min(myNextCursorBoundary, rangeEnd);
    }
  }


  /**
   * Estimate percentiles from the histogram bucket counts, using the following process:
   *
   * Suppose we have the following histogram with range 0-10, with bucket size = 2, and the following counts:
   *
   * |   0  |   1  |   2  |   4  |   0  |
   * 0      2      4      6      8      10
   *
   * If we wanted to estimate the 75th percentile:
   * 1. Find the frequency cut-off for the 75th percentile: 0.75 * 7 (the total count for the entire histogram) = 5.25
   * 2. Find the bucket for the cut-off point: in this case, the 6-8 bucket, since the cumulative frequency
   *    from 0-6 is 3 and cumulative frequency from 0-8 is 7
   * 3. The percentile estimate is L + F * W, where:
   *
   *   L = lower boundary of target bucket
   *   F = (frequency cut-off - cumulative frequency up to target bucket) / frequency of target bucket
   *   W = bucket size
   *
   *   In this case:
   *   75th percentile estimate = 6 + ((5.25 - 3) / 4) * 2 = 7.125
   *
   * Based off PercentileBuckets code from Netflix Spectator:
   * - https://github.com/Netflix/spectator/blob/0d083da3a60221de9cc710e314b0749e47a40e67/spectator-api/src/main/java/com/netflix/spectator/api/histogram/PercentileBuckets.java#L105
   *
   * @param pcts Array of percentiles to be estimated. Must be sorted in ascending order.
   * @return Estimates of percentile values, in the same order as the provided input.
   */
  public float[] percentilesFloat(double[] pcts)
  {
    readWriteLock.readLock().lock();

    try {
      float[] results = new float[pcts.length];
      long total = count;

      int pctIdx = 0;

      long prev = 0;
      double prevP = 0.0;
      double prevB = lowerLimit;
      for (int i = 0; i < numBuckets; ++i) {
        long next = prev + histogram[i];
        double nextP = 100.0 * next / total;
        double nextB = (i + 1) * bucketSize + lowerLimit;
        while (pctIdx < pcts.length && nextP >= pcts[pctIdx]) {
          double f = (pcts[pctIdx] - prevP) / (nextP - prevP);
          results[pctIdx] = (float) (f * (nextB - prevB) + prevB);
          ++pctIdx;
        }
        if (pctIdx >= pcts.length) {
          break;
        }
        prev = next;
        prevP = nextP;
        prevB = nextB;
      }

      return results;
    }
    finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Encode the serialized form generated by toBytes() in Base64, used as the JSON serialization format.
   *
   * @return Base64 serialization
   */
  @JsonValue
  public String toBase64()
  {
    byte[] asBytes = toBytes();
    return StringUtils.fromUtf8(StringUtils.encodeBase64(asBytes));
  }

  /**
   * Serialize the histogram, with two possible encodings chosen based on the number of filled buckets:
   *
   * Full: Store the histogram buckets as an array of bucket counts, with size numBuckets
   * Sparse: Store the histogram buckets as a list of (bucketNum, count) pairs.
   */
  public byte[] toBytes()
  {
    readWriteLock.readLock().lock();

    try {
      int nonEmptyBuckets = getNonEmptyBucketCount();
      if (nonEmptyBuckets < (numBuckets / 2)) {
        return toBytesSparse(nonEmptyBuckets);
      } else {
        return toBytesFull(true);
      }
    }
    finally {
      readWriteLock.readLock().unlock();
    }
  }

  /**
   * Write a serialization header containing the serde version byte and full/sparse encoding mode byte.
   *
   * This header is not needed when serializing the histogram for localized internal use within the
   * buffer aggregator implementation.
   *
   * @param buf Destination buffer
   * @param mode Full or sparse mode
   */
  private void writeByteBufferSerdeHeader(ByteBuffer buf, byte mode)
  {
    buf.put(SERIALIZATION_VERSION);
    buf.put(mode);
  }

  /**
   * Serializes histogram fields that are common to both the full and sparse encoding modes.
   *
   * @param buf Destination buffer
   */
  private void writeByteBufferCommonFields(ByteBuffer buf)
  {
    buf.putDouble(lowerLimit);
    buf.putDouble(upperLimit);
    buf.putInt(numBuckets);
    buf.put((byte) outlierHandlingMode.ordinal());

    buf.putLong(count);
    buf.putLong(lowerOutlierCount);
    buf.putLong(upperOutlierCount);
    buf.putLong(missingValueCount);

    buf.putDouble(max);
    buf.putDouble(min);
  }

  /**
   * Serialize the histogram in full encoding mode.
   *
   * For efficiency, the header is not written when the histogram
   * is serialized for localized internal use within the buffer aggregator implementation.
   *
   * @param withHeader If true, include the serialization header
   * @return Serialized histogram with full encoding
   */
  public byte[] toBytesFull(boolean withHeader)
  {
    int size = getFullStorageSize(numBuckets);
    if (withHeader) {
      size += SERDE_HEADER_SIZE;
    }
    ByteBuffer buf = ByteBuffer.allocate(size);
    writeByteBufferFull(buf, withHeader);
    return buf.array();
  }

  /**
   * Helper method for toBytesFull
   *
   * @param buf Destination buffer
   * @param withHeader If true, include the serialization header
   */
  private void writeByteBufferFull(ByteBuffer buf, boolean withHeader)
  {
    if (withHeader) {
      writeByteBufferSerdeHeader(buf, FULL_ENCODING_MODE);
    }

    writeByteBufferCommonFields(buf);

    buf.asLongBuffer().put(histogram);
    buf.position(buf.position() + Long.BYTES * histogram.length);
  }

  /**
   * Serialize the histogram in sparse encoding mode.
   *
   * The serialization header is always written, since the sparse encoding is only used in situations where the
   * header is required.
   *
   * @param nonEmptyBuckets Number of non-empty buckets in the histogram
   * @return Serialized histogram with sparse encoding
   */
  public byte[] toBytesSparse(int nonEmptyBuckets)
  {
    int size = SERDE_HEADER_SIZE + getSparseStorageSize(nonEmptyBuckets);
    ByteBuffer buf = ByteBuffer.allocate(size);
    writeByteBufferSparse(buf, nonEmptyBuckets);
    return buf.array();
  }

  /**
   * Helper method for toBytesSparse
   *
   * @param buf Destination buffer
   * @param nonEmptyBuckets Number of non-empty buckets in the histogram
   */
  public void writeByteBufferSparse(ByteBuffer buf, int nonEmptyBuckets)
  {
    writeByteBufferSerdeHeader(buf, SPARSE_ENCODING_MODE);
    writeByteBufferCommonFields(buf);

    buf.putInt(nonEmptyBuckets);
    int bucketsWritten = 0;
    for (int i = 0; i < numBuckets; i++) {
      if (histogram[i] > 0) {
        buf.putInt(i);
        buf.putLong(histogram[i]);
        bucketsWritten += 1;
      }
      if (bucketsWritten == nonEmptyBuckets) {
        break;
      }
    }
  }

  /**
   * Deserialize a Base64-encoded histogram, used when deserializing from JSON (such as when transferring query results)
   * or when ingesting a pre-computed histogram.
   *
   * @param encodedHistogram Base64-encoded histogram
   * @return Deserialized object
   */
  public static FixedBucketsHistogram fromBase64(String encodedHistogram)
  {
    byte[] asBytes = StringUtils.decodeBase64(encodedHistogram.getBytes(StandardCharsets.UTF_8));
    return fromBytes(asBytes);
  }

  /**
   * General deserialization method for FixedBucketsHistogram.
   *
   * @param bytes
   * @return
   */
  public static FixedBucketsHistogram fromBytes(byte[] bytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return fromByteBuffer(buf);
  }

  /**
   * Deserialization helper method
   *
   * @param buf Source buffer containing serialized histogram
   * @return Deserialized object
   */
  public static FixedBucketsHistogram fromByteBuffer(ByteBuffer buf)
  {
    byte serializationVersion = buf.get();
    Preconditions.checkArgument(
        serializationVersion == SERIALIZATION_VERSION,
        StringUtils.format("Only serialization version %s is supported.", SERIALIZATION_VERSION)
    );
    byte mode = buf.get();
    if (mode == FULL_ENCODING_MODE) {
      return fromByteBufferFullNoSerdeHeader(buf);
    } else if (mode == SPARSE_ENCODING_MODE) {
      return fromBytesSparse(buf);
    } else {
      throw new ISE("Invalid histogram serde mode: %s", mode);
    }
  }

  /**
   * Helper method for deserializing histograms with full encoding mode. Assumes that the serialization header is not
   * present or has already been read.
   *
   * @param buf Source buffer containing serialized full-encoding histogram.
   * @return Deserialized object
   */
  protected static FixedBucketsHistogram fromByteBufferFullNoSerdeHeader(ByteBuffer buf)
  {
    double lowerLimit = buf.getDouble();
    double upperLimit = buf.getDouble();
    int numBuckets = buf.getInt();
    OutlierHandlingMode outlierHandlingMode = OutlierHandlingMode.values()[buf.get()];

    long count = buf.getLong();
    long lowerOutlierCount = buf.getLong();
    long upperOutlierCount = buf.getLong();
    long missingValueCount = buf.getLong();

    double max = buf.getDouble();
    double min = buf.getDouble();

    long histogram[] = new long[numBuckets];
    buf.asLongBuffer().get(histogram);
    buf.position(buf.position() + Long.BYTES * histogram.length);

    return new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode,
        histogram,
        count,
        max,
        min,
        lowerOutlierCount,
        upperOutlierCount,
        missingValueCount
    );
  }

  /**
   * Helper method for deserializing histograms with sparse encoding mode. Assumes that the serialization header is not
   * present or has already been read.
   *
   * @param buf Source buffer containing serialized sparse-encoding histogram.
   * @return Deserialized object
   */
  private static FixedBucketsHistogram fromBytesSparse(ByteBuffer buf)
  {
    double lowerLimit = buf.getDouble();
    double upperLimit = buf.getDouble();
    int numBuckets = buf.getInt();
    OutlierHandlingMode outlierHandlingMode = OutlierHandlingMode.values()[buf.get()];

    long count = buf.getLong();
    long lowerOutlierCount = buf.getLong();
    long upperOutlierCount = buf.getLong();
    long missingValueCount = buf.getLong();

    double max = buf.getDouble();
    double min = buf.getDouble();

    int nonEmptyBuckets = buf.getInt();
    long histogram[] = new long[numBuckets];
    for (int i = 0; i < nonEmptyBuckets; i++) {
      int bucket = buf.getInt();
      long bucketCount = buf.getLong();
      histogram[bucket] = bucketCount;
    }

    return new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode,
        histogram,
        count,
        max,
        min,
        lowerOutlierCount,
        upperOutlierCount,
        missingValueCount
    );
  }

  /**
   * Compute the size in bytes of a full-encoding serialized histogram, without the serialization header
   *
   * @param numBuckets number of buckets
   * @return full serialized size in bytes
   */
  public static int getFullStorageSize(int numBuckets)
  {
    return COMMON_FIELDS_SIZE +
           Long.BYTES * numBuckets;
  }

  /**
   * Compute the size in bytes of a sparse-encoding serialized histogram, without the serialization header
   *
   * @param nonEmptyBuckets number of non-empty buckets
   * @return sparse serialized size in bytes
   */
  public static int getSparseStorageSize(int nonEmptyBuckets)
  {
    return COMMON_FIELDS_SIZE +
           Integer.BYTES +
           (Integer.BYTES + Long.BYTES) * nonEmptyBuckets;
  }

  @VisibleForTesting
  public int getNonEmptyBucketCount()
  {
    int count = 0;
    for (int i = 0; i < numBuckets; i++) {
      if (histogram[i] != 0) {
        count++;
      }
    }
    return count;
  }

  /**
   * Updates the histogram state when an outlier value is encountered. An implementation is provided for each
   * supported outlierHandlingMode.
   */
  private interface OutlierHandler
  {
    /**
     * Handle an outlier when a single numeric value is added to the histogram.
     *
     * @param exceededMax If true, the value was an upper outlier. If false, the value was a lower outlier.
     */
    void handleOutlierAdd(boolean exceededMax);

    /**
     * Merge outlier information from another histogram.
     *
     * Called when merging two histograms that have overlapping ranges and potentially different bucket sizes.
     *
     * @param otherHistogram
     * @param rangeStart
     * @param rangeEnd
     */
    void simpleInterpolateMergeHandleOutliers(
        FixedBucketsHistogram otherHistogram,
        double rangeStart,
        double rangeEnd
    );

    /**
     * Merge outlier information from another histogram.
     *
     * Called when merging two histograms with identical buckets (same range and number of buckets)
     *
     * @param otherHistogram other histogram being merged
     */
    void handleOutliersForCombineSameBuckets(FixedBucketsHistogram otherHistogram);

    /**
     * Merge outlier information from another histogram.
     *
     * Called when merging two histograms, where the histogram to be merged has lowerLimit greater than my upperLimit
     * @param otherHistogram other histogram being merged
     */
    void handleOutliersCombineDifferentBucketsAllUpper(FixedBucketsHistogram otherHistogram);

    /**
     * Merge outlier information from another histogram.
     *
     * Called when merging two histograms, where the histogram to be merged has upperLimit less than my lowerLimit
     * @param otherHistogram other histogram being merged
     */

    void handleOutliersCombineDifferentBucketsAllLower(FixedBucketsHistogram otherHistogram);
  }

  private OutlierHandler makeOutlierHandler()
  {
    switch (outlierHandlingMode) {
      case IGNORE:
        return new IgnoreOutlierHandler();
      case OVERFLOW:
        return new OverflowOutlierHandler();
      case CLIP:
        return new ClipOutlierHandler();
      default:
        throw new ISE("Unknown outlier handling mode: %s", outlierHandlingMode);
    }
  }

  private static class IgnoreOutlierHandler implements OutlierHandler
  {
    @Override
    public void handleOutlierAdd(boolean exceededMax)
    {
    }

    @Override
    public void simpleInterpolateMergeHandleOutliers(
        FixedBucketsHistogram otherHistogram,
        double rangeStart,
        double rangeEnd
    )
    {
    }

    @Override
    public void handleOutliersForCombineSameBuckets(FixedBucketsHistogram otherHistogram)
    {
    }

    @Override
    public void handleOutliersCombineDifferentBucketsAllUpper(FixedBucketsHistogram otherHistogram)
    {
    }

    @Override
    public void handleOutliersCombineDifferentBucketsAllLower(FixedBucketsHistogram otherHistogram)
    {
    }
  }

  private class OverflowOutlierHandler implements OutlierHandler
  {
    @Override
    public void handleOutlierAdd(boolean exceededMax)
    {
      if (exceededMax) {
        upperOutlierCount += 1;
      } else {
        lowerOutlierCount += 1;
      }
    }

    @Override
    public void simpleInterpolateMergeHandleOutliers(
        FixedBucketsHistogram otherHistogram,
        double rangeStart,
        double rangeEnd
    )
    {
      // They contain me
      if (lowerLimit == rangeStart && upperLimit == rangeEnd) {
        long lowerCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeStart, true));
        long upperCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));
        upperOutlierCount += otherHistogram.getUpperOutlierCount() + upperCountFromOther;
        lowerOutlierCount += otherHistogram.getLowerOutlierCount() + lowerCountFromOther;
      } else if (rangeStart == lowerLimit) {
        // assume that none of the other histogram's outliers fall within our range
        // how many values were there in the portion of the other histogram that falls under my lower limit?
        long lowerCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeStart, true));
        lowerOutlierCount += lowerCountFromOther;
        lowerOutlierCount += otherHistogram.getLowerOutlierCount();
        upperOutlierCount += otherHistogram.getUpperOutlierCount();
      } else if (rangeEnd == upperLimit) {
        // how many values were there in the portion of the other histogram that is greater than my upper limit?
        long upperCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));
        upperOutlierCount += upperCountFromOther;
        upperOutlierCount += otherHistogram.getUpperOutlierCount();
        lowerOutlierCount += otherHistogram.getLowerOutlierCount();
      } else if (rangeStart > lowerLimit && rangeEnd < upperLimit) {
        upperOutlierCount += otherHistogram.getUpperOutlierCount();
        lowerOutlierCount += otherHistogram.getLowerOutlierCount();
      }
    }

    @Override
    public void handleOutliersForCombineSameBuckets(FixedBucketsHistogram otherHistogram)
    {
      lowerOutlierCount += otherHistogram.getLowerOutlierCount();
      upperOutlierCount += otherHistogram.getUpperOutlierCount();
    }

    @Override
    public void handleOutliersCombineDifferentBucketsAllUpper(FixedBucketsHistogram otherHistogram)
    {
      // ignore any lower outliers in the other histogram, we're not sure where those outliers would fall
      // within our range.
      upperOutlierCount += otherHistogram.getCount() + otherHistogram.getUpperOutlierCount();
    }

    @Override
    public void handleOutliersCombineDifferentBucketsAllLower(FixedBucketsHistogram otherHistogram)
    {
      // ignore any upper outliers in the other histogram, we're not sure where those outliers would fall
      // within our range.
      lowerOutlierCount += otherHistogram.getCount() + otherHistogram.getLowerOutlierCount();
    }
  }

  private class ClipOutlierHandler implements OutlierHandler
  {
    @Override
    public void handleOutlierAdd(boolean exceededMax)
    {
      double clippedValue;
      count += 1;
      if (exceededMax) {
        clippedValue = upperLimit;
        histogram[histogram.length - 1] += 1;
      } else {
        clippedValue = lowerLimit;
        histogram[0] += 1;
      }
      if (clippedValue > max) {
        max = clippedValue;
      }
      if (clippedValue < min) {
        min = clippedValue;
      }
    }

    @Override
    public void simpleInterpolateMergeHandleOutliers(
        FixedBucketsHistogram otherHistogram,
        double rangeStart,
        double rangeEnd
    )
    {
      // They contain me
      if (lowerLimit == rangeStart && upperLimit == rangeEnd) {
        long lowerCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeStart, true));
        long upperCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));
        histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount() + upperCountFromOther;
        histogram[0] += otherHistogram.getLowerOutlierCount() + lowerCountFromOther;
        count += otherHistogram.getUpperOutlierCount() + otherHistogram.getLowerOutlierCount();
        count += upperCountFromOther + lowerCountFromOther;
        if (otherHistogram.getUpperOutlierCount() + upperCountFromOther > 0) {
          max = Math.max(max, upperLimit);
        }
        if (otherHistogram.getLowerOutlierCount() + lowerCountFromOther > 0) {
          min = Math.min(min, lowerLimit);
        }
      } else if (rangeStart == lowerLimit) {
        // assume that none of the other histogram's outliers fall within our range
        // how many values were there in the portion of the other histogram that falls under my lower limit?
        long lowerCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeStart, true));

        histogram[0] += lowerCountFromOther;
        histogram[0] += otherHistogram.getLowerOutlierCount();
        histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount();
        count += lowerCountFromOther + otherHistogram.getLowerOutlierCount() + otherHistogram.getUpperOutlierCount();
        if (lowerCountFromOther + otherHistogram.getLowerOutlierCount() > 0) {
          min = lowerLimit;
        }
        if (otherHistogram.getUpperOutlierCount() > 0) {
          max = upperLimit;
        }
      } else if (rangeEnd == upperLimit) {
        // how many values were there in the portion of the other histogram that is greater than my upper limit?
        long upperCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));

        histogram[histogram.length - 1] += upperCountFromOther;
        histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount();
        histogram[0] += otherHistogram.getLowerOutlierCount();
        count += upperCountFromOther + otherHistogram.getLowerOutlierCount() + otherHistogram.getUpperOutlierCount();
        if (upperCountFromOther + otherHistogram.getUpperOutlierCount() > 0) {
          max = upperLimit;
        }
        if (otherHistogram.getLowerOutlierCount() > 0) {
          min = lowerLimit;
        }
      } else if (rangeStart > lowerLimit && rangeEnd < upperLimit) {
        // I contain them

        histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount();
        histogram[0] += otherHistogram.getLowerOutlierCount();
        count += otherHistogram.getUpperOutlierCount() + otherHistogram.getLowerOutlierCount();
        if (otherHistogram.getUpperOutlierCount() > 0) {
          max = Math.max(max, upperLimit);
        } else if (otherHistogram.getCount() > 0) {
          max = Math.max(max, otherHistogram.getMax());
        }
        if (otherHistogram.getLowerOutlierCount() > 0) {
          min = Math.min(min, lowerLimit);
        } else if (otherHistogram.getCount() > 0) {
          min = Math.min(min, otherHistogram.getMin());
        }
      }
    }

    @Override
    public void handleOutliersForCombineSameBuckets(FixedBucketsHistogram otherHistogram)
    {
      if (otherHistogram.getLowerOutlierCount() > 0) {
        histogram[0] += otherHistogram.getLowerOutlierCount();
        count += otherHistogram.getLowerOutlierCount();
        min = Math.min(lowerLimit, min);
      }
      if (otherHistogram.getUpperOutlierCount() > 0) {
        histogram[histogram.length - 1] += otherHistogram.getUpperOutlierCount();
        count += otherHistogram.getUpperOutlierCount();
        max = Math.max(upperLimit, max);
      }
    }

    @Override
    public void handleOutliersCombineDifferentBucketsAllUpper(FixedBucketsHistogram otherHistogram)
    {
      long otherCount = otherHistogram.getCount() + otherHistogram.getUpperOutlierCount();
      histogram[histogram.length - 1] += otherCount;
      count += otherCount;
      if (otherCount > 0) {
        max = Math.max(max, upperLimit);
      }
    }

    @Override
    public void handleOutliersCombineDifferentBucketsAllLower(FixedBucketsHistogram otherHistogram)
    {
      long otherCount = otherHistogram.getCount() + otherHistogram.getLowerOutlierCount();
      histogram[0] += otherCount;
      count += otherCount;
      if (otherCount > 0) {
        min = Math.min(min, lowerLimit);
      }
    }
  }
}
