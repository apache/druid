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
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@link Indexed} specialized for storing variable-width binary values (such as utf8 encoded strings), which must be
 * sorted and unique, using 'front coding'. Front coding is a type of delta encoding for byte arrays, where sorted
 * values are grouped into buckets. The first value of the bucket is written entirely, and remaining values are stored
 * as a pair of an integer which indicates how much of the first byte array of the bucket to use as a prefix, followed
 * by the remaining bytes after the prefix to complete the value.
 *
 * Getting a value first picks the appropriate bucket, finds its offset in the underlying buffer, then scans the bucket
 * values to seek to the correct position of the value within the bucket in order to reconstruct it using the prefix
 * length.
 *
 * Finding the index of a value involves binary searching the first values of each bucket to find the correct bucket,
 * then a linear scan within the bucket to find the matching value (or negative insertion point -1 for values that
 * are not present).
 *
 * The value iterator reads an entire bucket at a time, reconstructing the values into an array to iterate within the
 * bucket before moving onto the next bucket as the iterator is consumed.
 */
public final class FrontCodedIndexed implements Indexed<ByteBuffer>
{
  public static FrontCodedIndexed read(ByteBuffer buffer, Comparator<ByteBuffer> comparator, ByteOrder ordering)
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
        comparator,
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
  private final Comparator<ByteBuffer> comparator;

  private FrontCodedIndexed(
      ByteBuffer buffer,
      Comparator<ByteBuffer> comparator,
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
    this.comparator = comparator;
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
  }

  @Override
  public int size()
  {
    return adjustedNumValues;
  }

  @Nullable
  @Override
  public ByteBuffer get(int index)
  {
    final int adjustedIndex;
    // due to vbyte encoding, the null value is not actually stored in the bucket (no negative values), so we adjust
    // the index
    if (hasNull) {
      if (index == 0) {
        return null;
      } else {
        adjustedIndex = index - 1;
      }
    } else {
      adjustedIndex = index;
    }
    // find the bucket which contains the value with maths
    final int offsetNum = adjustedIndex >> div;
    final int bucketIndex = adjustedIndex & rem;
    // get offset of that bucket in the value buffer
    final int offset = offsetNum > 0 ? buffer.getInt(offsetsPosition + ((offsetNum - 1) * Integer.BYTES)) : 0;
    ByteBuffer copy = buffer.asReadOnlyBuffer().order(buffer.order());
    copy.position(bucketsPosition + offset);
    return getFromBucket(copy, bucketIndex);
  }

  @Override
  public int indexOf(@Nullable ByteBuffer value)
  {
    // performs binary search using the first values of each bucket to locate the appropriate bucket, and then does
    // a linear scan to find the value within the bucket
    ByteBuffer copy = buffer.asReadOnlyBuffer().order(buffer.order());
    if (value == null) {
      return hasNull ? 0 : -1;
    }
    int minBucketIndex = 0;
    int maxBucketIndex = numBuckets - 1;
    while (minBucketIndex < maxBucketIndex) {
      int currBucketIndex = (minBucketIndex + maxBucketIndex) >>> 1;
      int currBucketFirstValueIndex = currBucketIndex * bucketSize;

      final ByteBuffer currBucketFirstValue = get(currBucketFirstValueIndex + adjustIndex);
      // we compare against the adjacent bucket to determine if the value is actually in this bucket or if we need
      // to keep searching buckets
      final ByteBuffer nextBucketFirstValue = get(currBucketFirstValueIndex + bucketSize + adjustIndex);
      int comparison = comparator.compare(currBucketFirstValue, value);
      if (comparison == 0) {
        // it turns out that the first value in current bucket is what we are looking for, short circuit
        return currBucketFirstValueIndex + adjustIndex;
      }

      int comparisonNext = comparator.compare(nextBucketFirstValue, value);
      if (comparison < 0 && comparisonNext > 0) {
        // this is exactly the right bucket
        final int offset = currBucketIndex > 0 ? copy.getInt(offsetsPosition + ((currBucketIndex - 1) * Integer.BYTES)) : 0;
        copy.position(bucketsPosition + offset);
        // find the value in the bucket (or where it would be if it were present)
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

    // this is where we ended up, try to find the value in the bucket
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
  public Iterator<ByteBuffer> iterator()
  {
    ByteBuffer copy = buffer.asReadOnlyBuffer().order(buffer.order());
    copy.position(bucketsPosition);
    final ByteBuffer[] firstBucket = readBucket(copy, numBuckets > 1 ? bucketSize : lastBucketNumValues);
    // iterator decodes and buffers a bucket at a time, paging through buckets as the iterator is consumed
    return new Iterator<ByteBuffer>()
    {
      private int currIndex = 0;
      private int currentBucketIndex = 0;
      private ByteBuffer[] currentBucket = firstBucket;

      @Override
      public boolean hasNext()
      {
        return currIndex < adjustedNumValues;
      }

      @Override
      public ByteBuffer next()
      {
        // null is handled special
        if (hasNull && currIndex == 0) {
          currIndex++;
          return null;
        }
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final int adjustedCurrIndex = hasNull ? currIndex - 1 : currIndex;
        final int bucketNum = adjustedCurrIndex >> div;
        // load next bucket if needed
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

  /**
   * Get a value from a bucket at a relative position
   */
  public static ByteBuffer getFromBucket(ByteBuffer bucket, int offset)
  {
    final byte[] first = readBytes(bucket);
    if (offset == 0) {
      return ByteBuffer.wrap(first);
    }
    int pos = 0;
    int prefixLength;
    byte[] fragment;
    // scan through bucket values until we reach offset
    do {
      prefixLength = VByte.readInt(bucket);
      fragment = readBytes(bucket);
    } while (++pos < offset);
    return combinePrefixAndFragment(first, prefixLength, fragment);
  }

  /**
   * Find the relative position of a value in a bucket with a linear scan
   */
  public static int findInBucket(ByteBuffer bucket, int numValues, ByteBuffer value, Comparator<ByteBuffer> comparator)
  {
    final byte[] first = readBytes(bucket);
    final int firstCompare = comparator.compare(ByteBuffer.wrap(first), value);
    if (firstCompare == 0) {
      return 0;
    }
    if (firstCompare > 0) {
      return -1;
    }
    int offset = 0;
    int prefixLength;
    byte[] fragment;
    // scan through bucket values until we find match or compare numValues
    int insertionPoint = 1;
    while (++offset < numValues) {
      prefixLength = VByte.readInt(bucket);
      fragment = readBytes(bucket);
      ByteBuffer next = combinePrefixAndFragment(first, prefixLength, fragment);
      final int cmp = comparator.compare(next, value);
      if (cmp == 0) {
        return offset;
      } else if (cmp < 0) {
        insertionPoint++;
      } else {
        break;
      }
    }
    return -(insertionPoint + 1);
  }

  /**
   * reconstruct a value given the bytes of the first bucket value, the length of the first value to use as a prefix,
   * and the fragment of the value to use after the prefix.
   */
  private static ByteBuffer combinePrefixAndFragment(byte[] first, int prefixLength, byte[] fragment)
  {
    ByteBuffer next = ByteBuffer.allocate(prefixLength + fragment.length);
    next.put(first, 0, prefixLength);
    next.put(fragment);
    next.flip();
    return next;
  }

  /**
   * Read an entire bucket from a {@link ByteBuffer}, returning an array of reconstructed value bytes.
   */
  public static ByteBuffer[] readBucket(ByteBuffer bucket, int numValues)
  {
    final byte[] prefixBytes = readBytes(bucket);
    final ByteBuffer[] bucketBuffers = new ByteBuffer[numValues];
    bucketBuffers[0] = ByteBuffer.wrap(prefixBytes);
    int pos = 1;
    while (pos < numValues) {
      final int prefixLength = VByte.readInt(bucket);
      final byte[] fragment = readBytes(bucket);
      ByteBuffer utf8 = combinePrefixAndFragment(prefixBytes, prefixLength, fragment);
      bucketBuffers[pos++] = utf8;
    }
    return bucketBuffers;
  }

  /**
   * Read the bytes of a single {@link VByte} encoded variable length value
   */
  public static byte[] readBytes(ByteBuffer buffer)
  {
    int length = VByte.readInt(buffer);
    final byte[] blob = new byte[length];
    buffer.get(blob, 0, length);
    return blob;
  }
}
