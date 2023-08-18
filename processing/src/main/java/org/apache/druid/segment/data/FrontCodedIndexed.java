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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@link Indexed} specialized for storing variable-width binary values (such as utf8 encoded strings), which must be
 * sorted and unique, using 'front coding'. Front coding is a type of delta encoding for byte arrays, where sorted
 * values are grouped into buckets. The first value of the bucket is written entirely, and remaining values are stored
 * as a pair of an integer which indicates how much of the first byte array of the bucket to use as a prefix, followed
 * by the remaining bytes after the prefix to complete the value. If using 'incremental' buckets, instead of using the
 * prefix of the first bucket value, instead the prefix is computed against the immediately preceding value in the
 * bucket.
 * <p>
 * front coded indexed layout:
 * | version | bucket size | has null? | number of values | size of "offsets" + "buckets" | "offsets" | "buckets" |
 * | ------- | ----------- | --------- | ---------------- | ----------------------------- | --------- | --------- |
 * |    byte |        byte |      byte |        vbyte int |                     vbyte int |     int[] |  bucket[] |
 * <p>
 * "offsets" are the ending offsets of each bucket stored in order, stored as plain integers for easy random access.
 * <p>
 * bucket layout:
 * | first value | prefix length | fragment | ... | prefix length | fragment |
 * | ----------- | ------------- | -------- | --- | ------------- | -------- |
 * |        blob |     vbyte int |     blob | ... |     vbyte int |     blob |
 * <p>
 * blob layout:
 * | blob length | blob bytes |
 * | ----------- | ---------- |
 * |   vbyte int |     byte[] |
 * <p>
 * <p>
 * Getting a value first picks the appropriate bucket, finds its offset in the underlying buffer, then scans the bucket
 * values to seek to the correct position of the value within the bucket in order to reconstruct it using the prefix
 * length.
 * <p>
 * Finding the index of a value involves binary searching the first values of each bucket to find the correct bucket,
 * then a linear scan within the bucket to find the matching value (or negative insertion point -1 for values that
 * are not present).
 * <p>
 * The value iterator reads an entire bucket at a time, reconstructing the values into an array to iterate within the
 * bucket before moving onto the next bucket as the iterator is consumed.
 * <p>
 * This class is not thread-safe since during operation modifies positions of a shared buffer.
 */
public abstract class FrontCodedIndexed implements Indexed<ByteBuffer>
{
  public static final byte V0 = 0;
  public static final byte V1 = 1;
  public static final byte DEFAULT_VERSION = V0;
  public static final int DEFAULT_BUCKET_SIZE = 4;

  public static byte validateVersion(byte version)
  {
    if (version != FrontCodedIndexed.V0 && version != FrontCodedIndexed.V1) {
      throw new IAE(
          "Unknown format version for FrontCodedIndexed [%s], must be [%s] or [%s]",
          version,
          FrontCodedIndexed.V0,
          FrontCodedIndexed.V1
      );
    }
    return version;
  }

  public static Supplier<FrontCodedIndexed> read(ByteBuffer buffer, ByteOrder ordering)
  {
    final ByteBuffer orderedBuffer = buffer.asReadOnlyBuffer().order(ordering);
    final byte version = orderedBuffer.get();
    Preconditions.checkArgument(version == V0 || version == V1, "only V0 and V1 exist, encountered " + version);
    final int bucketSize = Byte.toUnsignedInt(orderedBuffer.get());
    final boolean hasNull = NullHandling.IS_NULL_BYTE == orderedBuffer.get();
    final int numValues = VByte.readInt(orderedBuffer);
    // size of offsets + values
    final int size = VByte.readInt(orderedBuffer);
    final int offsetsPosition = orderedBuffer.position();
    // move position to end of buffer
    buffer.position(offsetsPosition + size);

    if (version == V0) {
      return () -> new FrontCodedV0(
          buffer,
          ordering,
          bucketSize,
          numValues,
          hasNull,
          offsetsPosition
      );
    } else {
      return () -> new FrontCodedV1(
          buffer,
          ordering,
          bucketSize,
          numValues,
          hasNull,
          offsetsPosition
      );
    }
  }

  protected final ByteBuffer buffer;
  protected final int adjustedNumValues;
  protected final int adjustIndex;
  protected final int bucketSize;
  protected final int numBuckets;
  protected final int div;
  protected final int rem;
  protected final int offsetsPosition;
  protected final int bucketsPosition;
  protected final boolean hasNull;
  protected final int lastBucketNumValues;

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

  /**
   * Get a value from a bucket at a relative position.
   * <p>
   * This method modifies the position of the buffer.
   */
  abstract ByteBuffer getFromBucket(ByteBuffer buffer, int offset);

  /**
   * Read an entire bucket from a {@link ByteBuffer}, returning an array of reconstructed value bytes.
   * <p>
   * This method modifies the position of the buffer.
   */
  abstract ByteBuffer[] readBucket(ByteBuffer buffer, int numValues);

  /**
   * Finds a value in a bucket among the fragments. The first value is assumed to have been already compared against
   * and be smaller than the value we are looking for. This comparison is the source of the 'shared prefix', which is
   * the length which the value has in common with the previous value of the bucket.
   * <p>
   * This method uses this shared prefix length to skip more expensive byte by byte full value comparisons when
   * possible by comparing the shared prefix length with the prefix length of the fragment. Since the bucket is always
   * sorted, prefix lengths shrink as you progress to higher indexes, and we can use this to reason that a fragment
   * with a longer prefix length than the shared prefix will always sort before the value we are looking for, and values
   * which have a shorter prefix will always be greater than the value we are looking for, so we only need to do a
   * full comparison if the prefix length is the same
   * <p>
   * this method modifies the position of {@link #buffer}
   */
  abstract int findInBucket(ByteBuffer value, int currBucketFirstValueIndex, int bucketSize, int sharedPrefixLength);
  
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
    Indexed.checkIndex(index, adjustedNumValues);

    // due to vbyte encoding, the null value is not actually stored in the bucket. we would typically represent it as a
    // length of -1, since 0 is the empty string, but VByte encoding cannot have negative values, so if the null value
    // is present, we adjust the index by 1 since it is always stored as position 0 due to sorting first
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

    if (numBuckets == 0) {
      return hasNull ? -2 : -1;
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

        return findInBucket(value, currBucketFirstValueIndex, bucketSize, sharedPrefix);
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
      return ~(bucketIndexBase + adjustIndex);
    }

    buffer.position(firstOffset + firstLength);

    return findInBucket(value, bucketIndexBase, numValuesInBucket, sharedPrefix);
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
    if (adjustedNumValues == 0) {
      return Collections.emptyIterator();
    }
    if (hasNull && adjustedNumValues == 1) {
      return Collections.<ByteBuffer>singletonList(null).iterator();
    }
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
   * Performs byte-by-byte comparison of the first value in a bucket with the specified value. Note that this method
   * MUST be prepared before calling, as it expects the length of the first value to have already been read externally,
   * and the buffer position to be at the start of the first bucket value. The final buffer position will be the
   * 'shared prefix length' of the first value in the bucket and the value to compare.
   * <p>
   * Bytes are compared using {@link StringUtils#compareUtf8UsingJavaStringOrdering(byte, byte)}. Therefore, when the
   * values are UTF-8 encoded strings, the ordering is compatible with {@link String#compareTo(String)}.
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
      comparison = StringUtils.compareUtf8UsingJavaStringOrdering(bucketBuffer.get(), value.get(sharedPrefix));
      if (comparison != 0) {
        bucketBuffer.position(startOffset + sharedPrefix);
        break;
      }
    }
    return comparison;
  }

  public static final class FrontCodedV0 extends FrontCodedIndexed
  {
    private FrontCodedV0(
        ByteBuffer buffer,
        ByteOrder order,
        int bucketSize,
        int numValues,
        boolean hasNull,
        int offsetsPosition
    )
    {
      super(buffer, order, bucketSize, numValues, hasNull, offsetsPosition);
    }

    @Override
    ByteBuffer getFromBucket(ByteBuffer buffer, int offset)
    {
      return getValueFromBucket(buffer, offset);
    }

    public static ByteBuffer getValueFromBucket(ByteBuffer buffer, int offset)
    {
      int prefixPosition;
      if (offset == 0) {
        final int length = VByte.readInt(buffer);
        final ByteBuffer firstValue = buffer.asReadOnlyBuffer();
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

    @Override
    ByteBuffer[] readBucket(ByteBuffer buffer, int numValues)
    {
      final int length = VByte.readInt(buffer);
      final byte[] prefixBytes = new byte[length];
      buffer.get(prefixBytes, 0, length);
      final ByteBuffer[] bucketBuffers = new ByteBuffer[numValues];
      bucketBuffers[0] = ByteBuffer.wrap(prefixBytes);
      int pos = 1;
      while (pos < numValues) {
        final int prefixLength = VByte.readInt(buffer);
        final int fragmentLength = VByte.readInt(buffer);
        final byte[] fragment = new byte[fragmentLength];
        buffer.get(fragment, 0, fragmentLength);
        final ByteBuffer value = ByteBuffer.allocate(prefixLength + fragmentLength);
        value.put(prefixBytes, 0, prefixLength);
        value.put(fragment);
        value.flip();
        bucketBuffers[pos++] = value;
      }
      return bucketBuffers;
    }

    @Override
    int findInBucket(ByteBuffer value, int currBucketFirstValueIndex, int bucketSize, int sharedPrefixLength)
    {
      int relativePosition = 0;
      int prefixLength;
      // scan through bucket values until we find match or compare numValues
      int insertionPoint = 1;
      while (++relativePosition < bucketSize) {
        prefixLength = VByte.readInt(buffer);
        if (prefixLength > sharedPrefixLength) {
          // this value shares more in common with the first value, so the value we are looking for comes after
          final int skip = VByte.readInt(buffer);
          buffer.position(buffer.position() + skip);
          insertionPoint++;
        } else if (prefixLength < sharedPrefixLength) {
          // prefix is smaller, that means this value sorts ahead of it
          break;
        } else {
          final int fragmentLength = VByte.readInt(buffer);
          final int common = Math.min(fragmentLength, value.remaining() - prefixLength);
          int fragmentComparison = 0;
          for (int i = 0; i < common; i++) {
            fragmentComparison = StringUtils.compareUtf8UsingJavaStringOrdering(
                buffer.get(buffer.position() + i),
                value.get(prefixLength + i)
            );
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
      return -(currBucketFirstValueIndex + adjustIndex) + (~insertionPoint);
    }
  }

  public static final class FrontCodedV1 extends FrontCodedIndexed
  {
    private final int[] unwindPrefixLength;
    private final int[] unwindBufferPosition;

    private FrontCodedV1(
        ByteBuffer buffer,
        ByteOrder order,
        int bucketSize,
        int numValues,
        boolean hasNull,
        int offsetsPosition
    )
    {
      super(buffer, order, bucketSize, numValues, hasNull, offsetsPosition);
      this.unwindPrefixLength = new int[bucketSize];
      this.unwindBufferPosition = new int[bucketSize];
    }

    @Override
    ByteBuffer getFromBucket(ByteBuffer buffer, int offset)
    {
      // first value is written whole
      final int length = VByte.readInt(buffer);
      if (offset == 0) {
        // return first value directly from underlying buffer since it is stored whole
        final ByteBuffer value = buffer.asReadOnlyBuffer();
        value.limit(value.position() + length);
        return value;
      }
      int pos = 0;
      int prefixLength;
      int fragmentLength;
      unwindPrefixLength[pos] = 0;
      unwindBufferPosition[pos] = buffer.position();

      buffer.position(buffer.position() + length);
      do {
        prefixLength = VByte.readInt(buffer);
        if (++pos < offset) {
          // not there yet, no need to read anything other than the length to skip ahead
          final int skipLength = VByte.readInt(buffer);
          unwindPrefixLength[pos] = prefixLength;
          unwindBufferPosition[pos] = buffer.position();
          buffer.position(buffer.position() + skipLength);
        } else {
          // we've reached our destination
          fragmentLength = VByte.readInt(buffer);
          if (prefixLength == 0) {
            // no prefix, return it directly from the underlying buffer
            final ByteBuffer value = buffer.asReadOnlyBuffer();
            value.limit(value.position() + fragmentLength);
            return value;
          }
          break;
        }
      } while (true);
      final int valueLength = prefixLength + fragmentLength;
      final byte[] valueBytes = new byte[valueLength];
      buffer.get(valueBytes, prefixLength, fragmentLength);
      for (int i = prefixLength; i > 0;) {
        // previous value had a larger prefix than or the same as the value we are looking for
        // skip it since the fragment doesn't have anything we need
        if (unwindPrefixLength[--pos] >= i) {
          continue;
        }
        buffer.position(unwindBufferPosition[pos]);
        buffer.get(valueBytes, unwindPrefixLength[pos], i - unwindPrefixLength[pos]);
        i = unwindPrefixLength[pos];
      }
      return ByteBuffer.wrap(valueBytes);
    }

    @Override
    ByteBuffer[] readBucket(ByteBuffer buffer, int numValues)
    {
      final ByteBuffer[] bucketBuffers = new ByteBuffer[numValues];

      // first value is written whole
      final int length = VByte.readInt(buffer);
      byte[] prefixBytes = new byte[length];
      buffer.get(prefixBytes, 0, length);
      bucketBuffers[0] = ByteBuffer.wrap(prefixBytes);
      int pos = 1;
      while (pos < numValues) {
        final int prefixLength = VByte.readInt(buffer);
        final int fragmentLength = VByte.readInt(buffer);
        byte[] nextValueBytes = new byte[prefixLength + fragmentLength];
        System.arraycopy(prefixBytes, 0, nextValueBytes, 0, prefixLength);
        buffer.get(nextValueBytes, prefixLength, fragmentLength);
        final ByteBuffer value = ByteBuffer.wrap(nextValueBytes);
        prefixBytes = nextValueBytes;
        bucketBuffers[pos++] = value;
      }
      return bucketBuffers;
    }

    @Override
    int findInBucket(ByteBuffer value, int currBucketFirstValueIndex, int bucketSize, int sharedPrefixLength)
    {
      int relativePosition = 0;
      int prefixLength;
      // scan through bucket values until we find match or compare numValues
      int insertionPoint = 1;
      while (++relativePosition < bucketSize) {
        prefixLength = VByte.readInt(buffer);
        if (prefixLength > sharedPrefixLength) {
          // bucket value shares more in common with the preceding value, so the value we are looking for comes after
          final int skip = VByte.readInt(buffer);
          buffer.position(buffer.position() + skip);
          insertionPoint++;
        } else if (prefixLength < sharedPrefixLength) {
          // bucket value prefix is smaller, that means the value we are looking for sorts ahead of it
          break;
        } else {
          // value has the same shared prefix, so compare additional values to find
          final int fragmentLength = VByte.readInt(buffer);
          final int common = Math.min(fragmentLength, value.remaining() - prefixLength);
          int fragmentComparison = 0;
          boolean shortCircuit = false;
          for (int i = 0; i < common; i++) {
            fragmentComparison = StringUtils.compareUtf8UsingJavaStringOrdering(
                buffer.get(buffer.position() + i),
                value.get(prefixLength + i)
            );
            if (fragmentComparison != 0) {
              sharedPrefixLength = prefixLength + i;
              shortCircuit = true;
              break;
            }
          }
          if (fragmentComparison == 0) {
            fragmentComparison = Integer.compare(prefixLength + fragmentLength, value.remaining());
          }

          if (fragmentComparison == 0) {
            return (currBucketFirstValueIndex + adjustIndex) + relativePosition;
          } else if (fragmentComparison < 0) {
            // value we are looking for is longer than the current bucket value, continue on
            if (!shortCircuit) {
              sharedPrefixLength = prefixLength + common;
            }
            buffer.position(buffer.position() + fragmentLength);
            insertionPoint++;
          } else {
            break;
          }
        }
      }
      // (-(insertion point) - 1)
      return -(currBucketFirstValueIndex + adjustIndex) + (~insertionPoint);
    }
  }
}
