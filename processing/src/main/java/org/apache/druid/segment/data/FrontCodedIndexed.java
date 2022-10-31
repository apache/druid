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
import com.google.common.base.Supplier;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@link Indexed} specialized for storing variable-width binary values (such as utf8 encoded strings), which must be
 * sorted and unique, using 'front coding'. Front coding is a type of delta encoding for byte arrays, where sorted
 * values are grouped into buckets. The first value of the bucket is written entirely, and remaining values are stored
 * as a pair of an integer which indicates how much of the first byte array of the bucket to use as a prefix, followed
 * by the remaining bytes after the prefix to complete the value.
 *
 * front coded indexed layout:
 * | version | bucket size | has null? | number of values | size of "offsets" + "buckets" | "offsets" | "buckets" |
 * | ------- | ----------- | --------- | ---------------- | ----------------------------- | --------- | --------- |
 * |    byte |        byte |      byte |        vbyte int |                     vbyte int |     int[] |  bucket[] |
 *
 * "offsets" are the ending offsets of each bucket stored in order, stored as plain integers for easy random access.
 *
 * bucket layout:
 * | first value | prefix length | fragment | ... | prefix length | fragment |
 * | ----------- | ------------- | -------- | --- | ------------- | -------- |
 * |        blob |     vbyte int |     blob | ... |     vbyte int |     blob |
 *
 * blob layout:
 * | blob length | blob bytes |
 * | ----------- | ---------- |
 * |   vbyte int |     byte[] |
 *
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
 *
 * This class is not thread-safe since during operation modifies positions of a shared buffer.
 */
public final class FrontCodedIndexed implements Indexed<ByteBuffer>
{
  public static Supplier<FrontCodedIndexed> read(ByteBuffer buffer, ByteOrder ordering)
  {
    final ByteBuffer orderedBuffer = buffer.asReadOnlyBuffer().order(ordering);
    final byte version = orderedBuffer.get();
    Preconditions.checkArgument(version == 0, "only V0 exists, encountered " + version);
    final int bucketSize = orderedBuffer.get();
    final boolean hasNull = NullHandling.IS_NULL_BYTE == orderedBuffer.get();
    final int numValues = VByte.readInt(orderedBuffer);
    // size of offsets + values
    final int size = VByte.readInt(orderedBuffer);
    final int offsetsPosition = orderedBuffer.position();
    // move position to end of buffer
    buffer.position(offsetsPosition + size);

    return () -> new FrontCodedIndexed(
        buffer,
        ordering,
        bucketSize,
        numValues,
        hasNull,
        offsetsPosition
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

  private FrontCodedIndexed(
      ByteBuffer buffer,
      ByteOrder order,
      int bucketSize,
      int numValues,
      boolean hasNull,
      int offsetsPosition
  )
  {
    if (Integer.bitCount(bucketSize) != 1) {
      throw new ISE("bucketSize must be a power of two but was[%,d]", bucketSize);
    }
    this.buffer = buffer.asReadOnlyBuffer().order(order);
    this.bucketSize = bucketSize;
    this.hasNull = hasNull;

    this.numBuckets = (int) Math.ceil((double) numValues / (double) bucketSize);
    this.adjustIndex = hasNull ? 1 : 0;
    this.adjustedNumValues = numValues + adjustIndex;
    this.div = Integer.numberOfTrailingZeros(bucketSize);
    this.rem = bucketSize - 1;
    this.lastBucketNumValues = (numValues & rem) == 0 ? bucketSize : numValues & rem;
    this.offsetsPosition = offsetsPosition;
    this.bucketsPosition = offsetsPosition + ((numBuckets - 1) * Integer.BYTES);
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
    if (hasNull && index == 0) {
      return null;
    }

    // due to vbyte encoding, the null value is not actually stored in the bucket (no negative values), so we adjust
    // the index
    final int adjustedIndex = index - adjustIndex;
    // find the bucket which contains the value with maths
    final int bucket = adjustedIndex >> div;
    final int bucketIndex = adjustedIndex & rem;
    final int offset = getBucketOffset(bucket);
    buffer.position(offset);
    return getFromBucket(buffer, bucketIndex);
  }

  @Override
  public int indexOf(@Nullable ByteBuffer value)
  {
    // performs binary search using the first values of each bucket to locate the appropriate bucket, and then does
    // a linear scan to find the value within the bucket
    if (value == null) {
      return hasNull ? 0 : -1;
    }

    int minBucketIndex = 0;
    int maxBucketIndex = numBuckets - 1;
    while (minBucketIndex < maxBucketIndex) {
      int currentBucket = (minBucketIndex + maxBucketIndex) >>> 1;
      int currBucketFirstValueIndex = currentBucket * bucketSize;

      // compare against first value in "current" bucket
      final int offset = getBucketOffset(currentBucket);
      buffer.position(offset);
      final int firstLength = VByte.readInt(buffer);
      final int firstOffset = buffer.position();
      int comparison = compareBucketFirstValue(buffer, firstLength, value);
      // save the length of the shared prefix with the first value of the bucket and the value to match so we
      // can use it later to skip over all values in the bucket that share a longer prefix with the first value
      // (the bucket is sorted, so the prefix length gets smaller as values increase)
      final int sharedPrefix = buffer.position() - firstOffset;
      if (comparison == 0) {
        if (firstLength == value.remaining()) {
          // it turns out that the first value in current bucket is what we are looking for, short circuit
          return currBucketFirstValueIndex + adjustIndex;
        } else {
          comparison = Integer.compare(firstLength, value.remaining());
        }
      }

      // we also compare against the adjacent bucket to determine if the value is actually in this bucket or
      // if we need to keep searching buckets
      final int nextOffset = getBucketOffset(currentBucket + 1);
      buffer.position(nextOffset);
      final int nextLength = VByte.readInt(buffer);
      int comparisonNext = compareBucketFirstValue(buffer, nextLength, value);
      if (comparisonNext == 0) {
        if (nextLength == value.remaining()) {
          // it turns out that the first value in next bucket is what we are looking for, go ahead and short circuit
          // for that as well, even though we weren't going to scan that bucket on this iteration...
          return (currBucketFirstValueIndex + adjustIndex) + bucketSize;
        } else {
          comparisonNext = Integer.compare(nextLength, value.remaining());
        }
      }

      if (comparison < 0 && comparisonNext > 0) {
        // this is exactly the right bucket
        // find the value in the bucket (or where it would be if it were present)
        buffer.position(firstOffset + firstLength);

        return findValueInBucket(value, currBucketFirstValueIndex, bucketSize, sharedPrefix);
      } else if (comparison < 0) {
        minBucketIndex = currentBucket + 1;
      } else {
        maxBucketIndex = currentBucket - 1;
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
    final int offset = getBucketOffset(minBucketIndex);

    // like we did in the loop, except comparison being smaller the first value here is a short circuit
    buffer.position(offset);
    final int firstLength = VByte.readInt(buffer);
    final int firstOffset = buffer.position();
    int comparison = compareBucketFirstValue(buffer, firstLength, value);
    final int sharedPrefix = buffer.position() - firstOffset;
    if (comparison == 0) {
      if (firstLength == value.remaining()) {
        // it turns out that the first value in current bucket is what we are looking for, short circuit
        return bucketIndexBase + adjustIndex;
      } else {
        comparison = Integer.compare(firstLength, value.remaining());
      }
    }

    if (comparison > 0) {
      // value preceedes bucket, so bail out
      return -(bucketIndexBase + adjustIndex) - 1;
    }

    buffer.position(firstOffset + firstLength);

    return findValueInBucket(value, bucketIndexBase, numValuesInBucket, sharedPrefix);
  }

  @Override
  public boolean isSorted()
  {
    // FrontCodedIndexed only supports sorted values
    return true;
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

  private int getBucketOffset(int bucket)
  {
    // get offset of that bucket in the value buffer, subtract 1 to get the starting position because we only store the
    // ending offset, so look at the ending offset of the previous bucket, or 0 if this is the first bucket
    return bucketsPosition + (bucket > 0 ? buffer.getInt(offsetsPosition + ((bucket - 1) * Integer.BYTES)) : 0);
  }


  /**
   * Performs an unsigned byte comparison of the first value in a bucket with the specified value. Note that this method
   * MUST be prepared before calling, as it expects the length of the first value to have already been read externally,
   * and the buffer position to be at the start of the first bucket value. The final buffer position will be the
   * 'shared prefix length' of the first value in the bucket and the value to compare
   */
  private static int compareBucketFirstValue(ByteBuffer bucketBuffer, int length, ByteBuffer value)
  {
    final int startOffset = bucketBuffer.position();
    final int commonLength = Math.min(length, value.remaining());
    // save the length of the shared prefix with the first value of the bucket and the value to match so we
    // can use it later to skip over all values in the bucket that share a longer prefix with the first value
    // (the bucket is sorted, so the prefix length gets smaller as values increase)
    int sharedPrefix;
    int comparison = 0;
    for (sharedPrefix = 0; sharedPrefix < commonLength; sharedPrefix++) {
      comparison = unsignedByteCompare(bucketBuffer.get(), value.get(sharedPrefix));
      if (comparison != 0) {
        bucketBuffer.position(startOffset + sharedPrefix);
        break;
      }
    }
    return comparison;
  }

  /**
   * Finds a value in a bucket among the fragments. The first value is assumed to have been already compared against
   * and be smaller than the value we are looking for. This comparison is the source of the 'shared prefix', which is
   * the length which the value has in common with the first value of the bucket.
   *
   * This method uses this shared prefix length to skip more expensive byte by byte full value comparisons when
   * possible by comparing the shared prefix length with the prefix length of the fragment. Since the bucket is always
   * sorted, prefix lengths shrink as you progress to higher indexes, and we can use this to reason that a fragment
   * with a longer prefix length than the shared prefix will always sort before the value we are looking for, and values
   * which have a shorter prefix will always be greater than the value we are looking for, so we only need to do a
   * full comparison if the prefix length is the same
   *
   * this method modifies the position of {@link #buffer}
   */
  private int findValueInBucket(
      ByteBuffer value,
      int currBucketFirstValueIndex,
      int bucketSize,
      int sharedPrefix
  )
  {
    int relativePosition = 0;
    int prefixLength;
    // scan through bucket values until we find match or compare numValues
    int insertionPoint = 1;
    while (++relativePosition < bucketSize) {
      prefixLength = VByte.readInt(buffer);
      if (prefixLength > sharedPrefix) {
        final int skip = VByte.readInt(buffer);
        buffer.position(buffer.position() + skip);
        insertionPoint++;
      } else if (prefixLength < sharedPrefix) {
        // prefix is smaller, that means this value sorts ahead of it
        break;
      } else {
        final int fragmentLength = VByte.readInt(buffer);
        final int common = Math.min(fragmentLength, value.remaining() - prefixLength);
        int fragmentComparison = 0;
        for (int i = 0; i < common; i++) {
          fragmentComparison = unsignedByteCompare(buffer.get(buffer.position() + i), value.get(prefixLength + i));
          if (fragmentComparison != 0) {
            break;
          }
        }
        if (fragmentComparison == 0) {
          fragmentComparison = Integer.compare(prefixLength + fragmentLength, value.remaining());
        }

        if (fragmentComparison == 0) {
          return (currBucketFirstValueIndex + adjustIndex) + relativePosition;
        } else if (fragmentComparison < 0) {
          buffer.position(buffer.position() + fragmentLength);
          insertionPoint++;
        } else {
          break;
        }
      }
    }
    // (-(insertion point) - 1)
    return -(currBucketFirstValueIndex + adjustIndex) + (-(insertionPoint) - 1);
  }

  /**
   * Get a value from a bucket at a relative position.
   *
   * This method modifies the position of the buffer.
   */
  static ByteBuffer getFromBucket(ByteBuffer buffer, int offset)
  {
    int prefixPosition;
    if (offset == 0) {
      final int length = VByte.readInt(buffer);
      final ByteBuffer firstValue = buffer.asReadOnlyBuffer().order(buffer.order());
      firstValue.limit(firstValue.position() + length);
      return firstValue;
    } else {
      final int firstLength = VByte.readInt(buffer);
      prefixPosition = buffer.position();
      buffer.position(buffer.position() + firstLength);
    }
    int pos = 0;
    int prefixLength;
    int fragmentLength;
    int fragmentPosition;
    // scan through bucket values until we reach offset
    do {
      prefixLength = VByte.readInt(buffer);
      if (++pos < offset) {
        // not there yet, no need to read anything other than the length to skip ahead
        final int skipLength = VByte.readInt(buffer);
        buffer.position(buffer.position() + skipLength);
      } else {
        // we've reached our destination
        fragmentLength = VByte.readInt(buffer);
        fragmentPosition = buffer.position();
        break;
      }
    } while (true);
    final int valueLength = prefixLength + fragmentLength;
    ByteBuffer value = ByteBuffer.allocate(valueLength);
    for (int i = 0; i < valueLength; i++) {
      if (i < prefixLength) {
        value.put(buffer.get(prefixPosition + i));
      } else {
        value.put(buffer.get(fragmentPosition + i - prefixLength));
      }
    }
    value.flip();
    return value;
  }


  /**
   * Read an entire bucket from a {@link ByteBuffer}, returning an array of reconstructed value bytes.
   *
   * This method modifies the position of the buffer.
   */
  private static ByteBuffer[] readBucket(ByteBuffer bucket, int numValues)
  {
    final int length = VByte.readInt(bucket);
    final byte[] prefixBytes = new byte[length];
    bucket.get(prefixBytes, 0, length);
    final ByteBuffer[] bucketBuffers = new ByteBuffer[numValues];
    bucketBuffers[0] = ByteBuffer.wrap(prefixBytes);
    int pos = 1;
    while (pos < numValues) {
      final int prefixLength = VByte.readInt(bucket);
      final int fragmentLength = VByte.readInt(bucket);
      final byte[] fragment = new byte[fragmentLength];
      bucket.get(fragment, 0, fragmentLength);
      final ByteBuffer value = ByteBuffer.allocate(prefixLength + fragmentLength);
      value.put(prefixBytes, 0, prefixLength);
      value.put(fragment);
      value.flip();
      bucketBuffers[pos++] = value;
    }
    return bucketBuffers;
  }

  public static int unsignedByteCompare(byte b1, byte b2)
  {
    return (b1 & 0xFF) - (b2 & 0xFF);
  }
}
