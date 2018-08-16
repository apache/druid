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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;
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
  private static final byte SERIALIZATION_VERSION = 0x01;

  private static final byte FULL_ENCODING_MODE = 0x01;
  private static final byte SPARSE_ENCODING_MODE = 0x02;

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

  private final ReadWriteLock readWriteLock;

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

  public void add(
      double value
  )
  {
    readWriteLock.writeLock().lock();

    try {
      if (value < lowerLimit) {
        handleOutlier(false);
        return;
      }

      if (value >= upperLimit) {
        handleOutlier(true);
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

  private void handleOutlier(boolean exceededMax)
  {
    switch (outlierHandlingMode) {
      case CLIP:
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
        break;
      case OVERFLOW:
        if (exceededMax) {
          upperOutlierCount += 1;
        } else {
          lowerOutlierCount += 1;
        }
        break;
      case IGNORE:
        break;
      default:
        throw new ISE("Unknown outlier handling mode: " + outlierHandlingMode);
    }
  }

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

  private void combineHistogramSameBuckets(FixedBucketsHistogram otherHistogram)
  {
    long[] otherHistogramArray = otherHistogram.getHistogram();
    for (int i = 0; i < numBuckets; i++) {
      histogram[i] += otherHistogramArray[i];
    }

    count += otherHistogram.getCount();
    max = Math.max(max, otherHistogram.getMax());
    min = Math.min(min, otherHistogram.getMin());

    switch (outlierHandlingMode) {
      case IGNORE:
        break;
      case OVERFLOW:
        lowerOutlierCount += otherHistogram.getLowerOutlierCount();
        upperOutlierCount += otherHistogram.getUpperOutlierCount();
        break;
      case CLIP:
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
        break;
      default:
        throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
    }
  }

  private void combineHistogramDifferentBuckets(FixedBucketsHistogram otherHistogram)
  {
    long otherCount;
    if (otherHistogram.getLowerLimit() >= upperLimit) {
      otherCount = otherHistogram.getCount() + otherHistogram.getUpperOutlierCount();
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          // ignore any lower outliers in the other histogram, we're not sure where those outliers would fall
          // within our range.
          upperOutlierCount += otherCount;
          break;
        case CLIP:
          histogram[histogram.length - 1] += otherCount;
          count += otherCount;
          if (otherCount > 0) {
            max = Math.max(max, upperLimit);
          }
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    } else if (otherHistogram.getUpperLimit() <= lowerLimit) {
      otherCount = otherHistogram.getCount() + otherHistogram.getLowerOutlierCount();
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          // ignore any upper outliers in the other histogram, we're not sure where those outliers would fall
          // within our range.
          lowerOutlierCount += otherCount;
          break;
        case CLIP:
          histogram[0] += otherCount;
          count += otherCount;
          if (otherCount > 0) {
            min = Math.min(min, lowerLimit);
          }
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    } else {
      simpleInterpolateMerge(otherHistogram);
    }
  }

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

  private void simpleInterpolateMergeHandleOutliers(
      FixedBucketsHistogram otherHistogram,
      double rangeStart,
      double rangeEnd
  )
  {
    // They contain me
    if (lowerLimit == rangeStart && upperLimit == rangeEnd) {
      long lowerCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeStart, true));
      long upperCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));

      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          upperOutlierCount += otherHistogram.getUpperOutlierCount() + upperCountFromOther;
          lowerOutlierCount += otherHistogram.getLowerOutlierCount() + lowerCountFromOther;
          break;
        case CLIP:
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
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    } else if (rangeStart == lowerLimit) {
      // assume that none of the other histogram's outliers fall within our range
      // how many values were there in the portion of the other histogram that falls under my lower limit?
      long lowerCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeStart, true));
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          lowerOutlierCount += lowerCountFromOther;
          lowerOutlierCount += otherHistogram.getLowerOutlierCount();
          upperOutlierCount += otherHistogram.getUpperOutlierCount();
          break;
        case CLIP:
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
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    } else if (rangeEnd == upperLimit) {
      // how many values were there in the portion of the other histogram that is greater than my upper limit?
      long upperCountFromOther = Math.round(otherHistogram.getCumulativeCount(rangeEnd, false));
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          upperOutlierCount += upperCountFromOther;
          upperOutlierCount += otherHistogram.getUpperOutlierCount();
          lowerOutlierCount += otherHistogram.getLowerOutlierCount();
          break;
        case CLIP:
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
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    } else if (rangeStart > lowerLimit && rangeEnd < upperLimit) {
      // I contain them
      switch (outlierHandlingMode) {
        case IGNORE:
          break;
        case OVERFLOW:
          upperOutlierCount += otherHistogram.getUpperOutlierCount();
          lowerOutlierCount += otherHistogram.getLowerOutlierCount();
          break;
        case CLIP:
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
          break;
        default:
          throw new ISE("Invalid outlier handling mode: " + outlierHandlingMode);
      }
    }
  }

  // assumes that values are uniformly distributed within each bucket in the other histogram
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

    simpleInterpolateMergeHandleOutliers(otherHistogram, rangeStart, rangeEnd);

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
          }

          double maxStride = Math.floor(
              (theirCursor + toConsume - (theirCurBucket * otherHistogram.getBucketSize()
                                          + otherHistogram.getLowerLimit())) / intraBucketStride
          );
          maxStride *= intraBucketStride;
          maxStride += theirCurrentLowerBucketBoundary;
          if (maxStride < theirCursor + toConsume) {
            max = Math.max(maxStride, max);
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

  // Based off PercentileBuckets code from Netflix Spectator: https://github.com/Netflix/spectator
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

  @JsonValue
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

  public String toBase64()
  {
    byte[] asBytes = toBytes();
    return StringUtils.fromUtf8(Base64.encodeBase64(asBytes));
  }

  public static FixedBucketsHistogram fromBase64(String encodedHistogram)
  {
    byte[] asBytes = Base64.decodeBase64(encodedHistogram.getBytes(StandardCharsets.UTF_8));
    return fromBytes(asBytes);
  }

  public static FixedBucketsHistogram fromBytes(byte[] bytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return fromBytes(buf);
  }

  public static FixedBucketsHistogram fromBytes(ByteBuffer buf)
  {
    byte serializationVersion = buf.get();
    Preconditions.checkArgument(
        serializationVersion == SERIALIZATION_VERSION,
        StringUtils.format("Only serialization version %s is supported.", SERIALIZATION_VERSION)
    );
    byte mode = buf.get();
    if (mode == FULL_ENCODING_MODE) {
      return fromBytesFull(buf);
    } else if (mode == SPARSE_ENCODING_MODE) {
      return fromBytesSparse(buf);
    } else {
      throw new ISE("Invalid histogram serde mode: %s", mode);
    }
  }

  public byte[] toBytesFull(boolean withHeader)
  {
    int size = getFullStorageSize(numBuckets);
    if (withHeader) {
      size += Byte.BYTES + Byte.BYTES;
    }
    ByteBuffer buf = ByteBuffer.allocate(size);
    toBytesFullHelper(buf, withHeader);
    return buf.array();
  }

  private void toBytesFullHelper(ByteBuffer buf, boolean withHeader)
  {
    if (withHeader) {
      buf.put(SERIALIZATION_VERSION);
      buf.put(FULL_ENCODING_MODE);
    }
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

    buf.asLongBuffer().put(histogram);
    buf.position(buf.position() + Long.BYTES * histogram.length);
  }

  public static FixedBucketsHistogram fromBytesFull(byte[] bytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return fromBytesFull(buf);
  }

  private static FixedBucketsHistogram fromBytesFull(ByteBuffer buf)
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

  public static int getFullStorageSize(int numBuckets)
  {
    return Double.BYTES +
           Double.BYTES +
           Integer.BYTES +
           Byte.BYTES +
           Long.BYTES * 4 +
           Double.BYTES * 2 +
           Long.BYTES * numBuckets;
  }

  public static int getHeaderSize()
  {
    return Byte.BYTES * 2;
  }

  public byte[] toBytesSparse(int nonEmptyBuckets)
  {
    int size = getSparseStorageSize(nonEmptyBuckets) + Byte.BYTES + Byte.BYTES;
    ByteBuffer buf = ByteBuffer.allocate(size);
    toSparseBytesHelper(buf, nonEmptyBuckets);
    return buf.array();
  }

  public void toSparseBytesHelper(ByteBuffer buf, int nonEmptyBuckets)
  {
    buf.put(SERIALIZATION_VERSION);
    buf.put(SPARSE_ENCODING_MODE);

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

  public static int getSparseStorageSize(int nonEmptyBuckets)
  {
    return Double.BYTES +
           Double.BYTES +
           Integer.BYTES +
           Byte.BYTES +
           Long.BYTES * 4 +
           Double.BYTES * 2 +
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
}
