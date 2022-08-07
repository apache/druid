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

package org.apache.druid.segment.data;

import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@link Indexed} specialized for storing utf8 string values, which must be sorted and unique, using 'front coding'.
 *
 * Front coding is a type of delta encoding for strings, where values are grouped into buckets. The first value of
 * the bucket is written entirely, and remaining values are stored as pairs of an integer which indicates how much
 * of the first string of the bucket to use as a prefix, followed by the remaining string value after the prefix.
 */
public final class FrontCodedIndexed implements Indexed<String>
{
  public static FrontCodedIndexed read(ByteBuffer buffer, ByteOrder ordering)
  {
    final ByteBuffer copy = buffer.asReadOnlyBuffer().order(ordering);
    final byte version = copy.get();
    Preconditions.checkArgument(version == 0, "only V0 exists, encountered " + version);
    final int bucketSize = copy.get();
    final boolean hasNull = NullHandling.IS_NULL_BYTE == copy.get();
    final int numValues = VByte.readInt(copy);
    // size of offsets + values
    final int size = VByte.readInt(copy);
    // move position to end of buffer
    buffer.position(copy.position() + size);

    final int numBuckets = (int) Math.ceil((double) numValues / (double) bucketSize);
    final int adjustIndex = hasNull ? 1 : 0;
    final int div = Integer.numberOfTrailingZeros(bucketSize);
    final int rem = bucketSize - 1;
    return new FrontCodedIndexed(
        copy,
        bucketSize,
        numBuckets,
        (numValues & rem) == 0 ? bucketSize : numValues & rem,
        hasNull,
        numValues + adjustIndex,
        adjustIndex,
        div,
        rem,
        copy.position(),
        copy.position() + ((numBuckets - 1) * Integer.BYTES)
    );
  }

  private final ByteBuffer buffer;
  private final int adjustedNumValues;
  private final int adjustIndex;
  private final int bucketSize;
  private final int numBuckets;
  private final int div;
  private final int rem;
  private final int offsetsPosition;
  private final int bucketsPosition;
  private final boolean hasNull;
  private final int lastBucketNumValues;
  private final Comparator<String> comparator;

  private FrontCodedIndexed(
      ByteBuffer buffer,
      int bucketSize,
      int numBuckets,
      int lastBucketNumValues,
      boolean hasNull,
      int adjustedNumValues,
      int adjustIndex,
      int div,
      int rem,
      int offsetsPosition,
      int bucketsPosition
  )
  {
    if (Integer.bitCount(bucketSize) != 1) {
      throw new ISE("bucketSize must be a power of two but was[%,d]", bucketSize);
    }
    this.buffer = buffer;
    this.bucketSize = bucketSize;
    this.hasNull = hasNull;
    this.adjustedNumValues = adjustedNumValues;
    this.adjustIndex = adjustIndex;
    this.div = div;
    this.rem = rem;
    this.numBuckets = numBuckets;
    this.offsetsPosition = offsetsPosition;
    this.bucketsPosition = bucketsPosition;
    this.lastBucketNumValues = lastBucketNumValues;
    this.comparator = ColumnType.STRING.getNullableStrategy();
  }

  @Override
  public int size()
  {
    return adjustedNumValues;
  }

  @Nullable
  @Override
  public String get(int index)
  {
    final int adjustedIndex;
    if (hasNull) {
      if (index == 0) {
        return null;
      } else {
        adjustedIndex = index - 1;
      }
    } else {
      adjustedIndex = index;
    }
    final int offsetNum = adjustedIndex >> div;
    final int bucketIndex = adjustedIndex & rem;
    final int offset = offsetNum > 0 ? buffer.getInt(offsetsPosition + ((offsetNum - 1) * Integer.BYTES)) : 0;
    ByteBuffer copy = buffer.asReadOnlyBuffer().order(buffer.order());
    copy.position(bucketsPosition + offset);
    return getFromBucket(copy, bucketIndex);
  }

  @Override
  public int indexOf(@Nullable String val)
  {
    ByteBuffer copy = buffer.asReadOnlyBuffer().order(buffer.order());
    final String value = NullHandling.emptyToNullIfNeeded(val);
    if (value == null) {
      return hasNull ? 0 : -1;
    }
    int minBucketIndex = 0;
    int maxBucketIndex = numBuckets - 1;
    while (minBucketIndex < maxBucketIndex) {
      int currBucketIndex = (minBucketIndex + maxBucketIndex) >>> 1;
      int currBucketFirstValueIndex = currBucketIndex * bucketSize;

      final String currBucketFirstValue = get(currBucketFirstValueIndex + adjustIndex);
      final String nextBucketFirstValue = get(currBucketFirstValueIndex + bucketSize + adjustIndex);
      int comparison = comparator.compare(currBucketFirstValue, value);
      if (comparison == 0) {
        return currBucketFirstValueIndex + adjustIndex;
      }

      int comparisonNext = comparator.compare(nextBucketFirstValue, value);
      if (comparison < 0 && comparisonNext > 0) {
        final int offset = currBucketIndex > 0 ? copy.getInt(offsetsPosition + ((currBucketIndex - 1) * Integer.BYTES)) : 0;
        copy.position(bucketsPosition + offset);
        final int pos = findInBucket(copy, bucketSize, value, comparator);
        if (pos < 0) {
          return -currBucketFirstValueIndex + pos - adjustIndex;
        }
        return currBucketFirstValueIndex + pos + adjustIndex;
      } else if (comparison < 0) {
        minBucketIndex = currBucketIndex + 1;
      } else {
        maxBucketIndex = currBucketIndex - 1;
      }
    }

    final int bucketIndexBase = minBucketIndex * bucketSize;
    final int numValuesInBucket;
    if (minBucketIndex == numBuckets - 1) {
      numValuesInBucket = lastBucketNumValues;
    } else {
      numValuesInBucket = bucketSize;
    }
    final int offset;
    if (minBucketIndex > 0) {
      offset = copy.getInt(offsetsPosition + ((minBucketIndex - 1) * Integer.BYTES));
    } else {
      offset = 0;
    }
    copy.position(bucketsPosition + offset);
    final int pos = findInBucket(copy, numValuesInBucket, value, comparator);
    if (pos < 0) {
      return -bucketIndexBase + pos - adjustIndex;
    }
    return bucketIndexBase + pos + adjustIndex;
  }

  @Override
  public Iterator<String> iterator()
  {
    ByteBuffer copy = buffer.asReadOnlyBuffer().order(buffer.order());
    copy.position(bucketsPosition);
    final String[] firstBucket = readBucket(copy, numBuckets > 1 ? bucketSize : lastBucketNumValues);
    return new Iterator<String>()
    {
      private int currIndex = 0;
      private int currentBucketIndex = 0;
      private String[] currentBucket = firstBucket;

      @Override
      public boolean hasNext()
      {
        return currIndex < adjustedNumValues;
      }

      @Override
      public String next()
      {
        if (hasNull && currIndex == 0) {
          currIndex++;
          return null;
        }
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final int adjustedCurrIndex = hasNull ? currIndex - 1 : currIndex;
        final int bucketNum = adjustedCurrIndex >> div;
        if (bucketNum != currentBucketIndex) {
          final int offset = copy.getInt(offsetsPosition + ((bucketNum - 1) * Integer.BYTES));
          copy.position(bucketsPosition + offset);
          currentBucket = readBucket(
              copy,
              bucketNum < (numBuckets - 1) ? bucketSize : lastBucketNumValues
          );
          currentBucketIndex = bucketNum;
        }
        int offset = adjustedCurrIndex & rem;
        currIndex++;
        return currentBucket[offset];
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("buffer", buffer);
    inspector.visit("hasNulls", hasNull);
    inspector.visit("bucketSize", bucketSize);
  }

  public static String getFromBucket(ByteBuffer bucket, int offset)
  {
    final String first = readString(bucket);
    if (offset == 0) {
      return first;
    }
    int pos = 0;
    int prefixLength;
    String fragment;
    // scan through bucket values until we reach offset
    do {
      prefixLength = VByte.readInt(bucket);
      fragment = readString(bucket);
    } while (++pos < offset);
    return first.substring(0, prefixLength) + fragment;
  }

  public static int findInBucket(ByteBuffer bucket, int numValues, String value, Comparator<String> comparator)
  {
    final String first = readString(bucket);
    final int firstCompare = comparator.compare(first, value);
    if (firstCompare == 0) {
      return 0;
    }
    if (firstCompare > 0) {
      return -1;
    }
    int offset = 0;
    int prefixLength;
    String fragment;
    // scan through bucket values until we find match or compare numValues
    int insertionPoint = 1;
    while (++offset < numValues) {
      prefixLength = VByte.readInt(bucket);
      fragment = readString(bucket);
      final String next = first.substring(0, prefixLength) + fragment;
      final int cmp = comparator.compare(next, value);
      if (cmp == 0) {
        return offset;
      } else if (cmp < 0) {
        insertionPoint++;
      }
    }
    return -(insertionPoint + 1);
  }

  public static String[] readBucket(ByteBuffer bucket, int numValues)
  {
    final String[] bucketValues = new String[numValues];
    bucketValues[0] = readString(bucket);
    int pos = 1;
    while (pos < numValues) {
      final int prefixLength = VByte.readInt(bucket);
      final String fragment = readString(bucket);
      bucketValues[pos++] = bucketValues[0].substring(0, prefixLength) + fragment;
    }
    return bucketValues;
  }

  public static int writeBucket(ByteBuffer buffer, String[] values, int numValues)
  {
    int written = 0;
    String first = null;
    while (written < numValues) {
      String next = values[written];
      if (written == 0) {
        first = next;
        int rem = writeString(buffer, first);
        if (rem < 0) {
          return rem;
        }
      } else {
        int i = 0;
        for (; i < first.length(); i++) {
          if (first.charAt(i) != next.charAt(i)) {
            break;
          }
        }
        int rem = buffer.remaining() - VByte.estimateIntSize(i);
        if (rem < 0) {
          return rem;
        }
        VByte.writeInt(buffer, i);
        rem = writeString(buffer, next.substring(i));
        if (rem < 0) {
          return rem;
        }
      }
      written++;
    }
    return written;
  }

  public static String readString(ByteBuffer buffer)
  {
    int length = VByte.readInt(buffer);
    final byte[] blob = new byte[length];
    buffer.get(blob, 0, length);
    return StringUtils.fromUtf8(blob);
  }

  public static int writeString(ByteBuffer buffer, String value)
  {
    final byte[] bytes = StringUtils.toUtf8(value);
    final int remaining = buffer.remaining() - VByte.estimateIntSize(bytes.length) - bytes.length;
    if (remaining < 0) {
      return remaining;
    }
    final int pos = buffer.position();
    VByte.writeInt(buffer, bytes.length);
    buffer.put(bytes, 0, bytes.length);
    return buffer.position() - pos;
  }
}
