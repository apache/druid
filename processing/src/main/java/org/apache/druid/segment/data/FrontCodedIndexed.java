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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class FrontCodedIndexed implements Indexed<String>
{
  private final ByteBuffer buffer;
  private final int numValues;
  private final int adjustedNumValues;
  private final int adjustIndex;
  private final int bucketSize;
  private final int numBuckets;
  private final int div;
  private final int rem;
  private final int offsetsPosition;
  private final int bucketsPosition;
  private final boolean hasNulls;
  private final int lastBucketNumValues;
  private final Comparator<String> comparator;

  public FrontCodedIndexed(ByteBuffer baseBuffer, ByteOrder ordering)
  {
    buffer = baseBuffer.asReadOnlyBuffer().order(ordering);
    bucketSize = buffer.get();
    hasNulls = NullHandling.IS_NULL_BYTE == buffer.get();
    div = Integer.numberOfTrailingZeros(bucketSize);
    rem = bucketSize - 1;
    numValues = readVbyteInt(buffer);
    adjustIndex = hasNulls ? 1 : 0;
    comparator = hasNulls ? ColumnType.STRING.getNullableStrategy() : ColumnType.STRING.getStrategy();
    adjustedNumValues = numValues + adjustIndex;
    numBuckets = (int) Math.ceil((double) numValues / (double) bucketSize);
    offsetsPosition = buffer.position();
    bucketsPosition = buffer.position() + ((numBuckets - 1) * Integer.BYTES);
    lastBucketNumValues = (numValues & rem) == 0 ? bucketSize : numValues & rem;
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
    if (hasNulls) {
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
    buffer.position(bucketsPosition + offset);
    return getFromBucket(buffer, bucketIndex);
  }

  @Override
  public int indexOf(@Nullable String value)
  {
    if (hasNulls && value == null) {
      return 0;
    }
    int minBucketIndex = 0;
    int maxBucketIndex = numBuckets - 1;
    while (minBucketIndex < maxBucketIndex) {
      int currBucketIndex = (minBucketIndex + maxBucketIndex) >>> 1;
      int currBucketFirstValueIndex = currBucketIndex * bucketSize;

      final String currBucketFirstValue = get(currBucketFirstValueIndex);
      final String nextBucketFirstValue = get(currBucketFirstValueIndex + bucketSize);
      int comparison = comparator.compare(currBucketFirstValue, value);
      if (comparison == 0) {
        return currBucketFirstValueIndex + adjustIndex;
      }

      int comparisonNext = comparator.compare(nextBucketFirstValue, value);
      if (comparison < 0 && comparisonNext > 0) {
        final int offset = currBucketIndex > 0 ? buffer.getInt(offsetsPosition + ((currBucketIndex - 1) * Integer.BYTES)) : 0;
        buffer.position(bucketsPosition + offset);
        final int pos = findInBucket(buffer, bucketSize, value, comparator);
        if (pos < 0) {
          return pos;
        }
        return currBucketFirstValueIndex + pos + adjustIndex;
      } else if (comparison < 0) {
        minBucketIndex = currBucketIndex + 1;
      } else {
        maxBucketIndex = currBucketIndex - 1;
      }
    }
    // check last bucket
    final int offset = buffer.getInt(offsetsPosition + ((numBuckets - 2) * Integer.BYTES));
    int lastBucketBaseIndex = (numBuckets - 1) * bucketSize;
    buffer.position(bucketsPosition + offset);
    final int pos = findInBucket(buffer, lastBucketNumValues, value, comparator);
    if (pos < 0) {
      return pos;
    }
    return lastBucketBaseIndex + pos + adjustIndex;
  }

  @Override
  public Iterator<String> iterator()
  {
    buffer.position(bucketsPosition);
    final String[] firstBucket = readBucket(buffer, numBuckets > 1 ? bucketSize : lastBucketNumValues);
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
        if (hasNulls && currIndex == 0) {
          currIndex++;
          return null;
        }
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final int adjustedCurrIndex = hasNulls ? currIndex - 1 : currIndex;
        final int bucketNum = adjustedCurrIndex >> div;
        if (bucketNum != currentBucketIndex) {
          final int offset = buffer.getInt(offsetsPosition + ((bucketNum - 1) * Integer.BYTES));
          buffer.position(bucketsPosition + offset);
          currentBucket = readBucket(
              buffer,
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
    // todo (clint): remember what goes in here
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
      prefixLength = readVbyteInt(bucket);
      fragment = readString(bucket);
    } while (++pos < offset);
    return first.substring(0, prefixLength) + fragment;
  }

  public static int findInBucket(ByteBuffer bucket, int numValues, String value, Comparator<String> comparator)
  {
    final String first = readString(bucket);
    if (comparator.compare(first, value) == 0) {
      return 0;
    }
    int offset = 0;
    int prefixLength;
    String fragment;
    // scan through bucket values until we reach offset
    while (++offset < numValues) {
      prefixLength = readVbyteInt(bucket);
      fragment = readString(bucket);
      final String next = first.substring(0, prefixLength) + fragment;
      if (comparator.compare(next, value) == 0) {
        return offset;
      }
    }
    return -1;
  }

  public static String[] readBucket(ByteBuffer bucket, int numValues)
  {
    final String[] bucketValues = new String[numValues];
    bucketValues[0] = readString(bucket);
    int pos = 1;
    while (pos < numValues) {
      final int prefixLength = readVbyteInt(bucket);
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
        writeVbyteInt(buffer, i);
        writeString(buffer, next.substring(i));
      }
      written++;
    }
    return written;
  }

  public static int readVbyteInt(ByteBuffer buffer)
  {
    // based on
    // https://github.com/lemire/JavaFastPFOR/blob/master/src/main/java/me/lemire/integercompression/VariableByte.java
    byte b;
    int v = (b = buffer.get()) & 0x7F;
    if (b < 0) {
      return v;
    }
    v = (((b = buffer.get()) & 0x7F) << 7) | v;
    if (b < 0) {
      return v;
    }
    v = (((b = buffer.get()) & 0x7F) << 14) | v;
    if (b < 0) {
      return v;
    }
    v = (((b = buffer.get()) & 0x7F) << 21) | v;
    if (b < 0) {
      return v;
    }
    v = ((buffer.get() & 0x7F) << 28) | v;
    return v;
  }

  public static int writeVbyteInt(ByteBuffer buffer, int val)
  {
    final int pos = buffer.position();
    // based on:
    // https://github.com/lemire/JavaFastPFOR/blob/master/src/main/java/me/lemire/integercompression/VariableByte.java
    if (val < (1 << 7)) {
      buffer.put((byte) (val | (1 << 7)));
    } else if (val < (1 << 14)) {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) (extract7bitsmaskless(1, (val)) | (1 << 7)));
    } else if (val < (1 << 21)) {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) extract7bits(1, val));
      buffer.put((byte) (extract7bitsmaskless(2, (val)) | (1 << 7)));
    } else if (val < (1 << 28)) {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) extract7bits(1, val));
      buffer.put((byte) extract7bits(2, val));
      buffer.put((byte) (extract7bitsmaskless(3, (val)) | (1 << 7)));
    } else {
      buffer.put((byte) extract7bits(0, val));
      buffer.put((byte) extract7bits(1, val));
      buffer.put((byte) extract7bits(2, val));
      buffer.put((byte) extract7bits(3, val));
      buffer.put((byte) (extract7bitsmaskless(4, (val)) | (1 << 7)));
    }
    return buffer.position() - pos;
  }

  private static byte extract7bits(int i, int val)
  {
    return (byte) ((val >> (7 * i)) & ((1 << 7) - 1));
  }

  private static byte extract7bitsmaskless(int i, int val)
  {
    return (byte) ((val >> (7 * i)));
  }

  // todo (clint): use for ColumnType.STRING type strategy?
  public static String readString(ByteBuffer buffer)
  {
    int length = readVbyteInt(buffer);
    final byte[] blob = new byte[length];
    buffer.get(blob, 0, length);
    return StringUtils.fromUtf8(blob);
  }

  public static int writeString(ByteBuffer buffer, String value)
  {
    final byte[] bytes = StringUtils.toUtf8(value);
    final int remaining = buffer.remaining() - estimateSizeVByteInt(bytes.length);
    if (remaining >= 0) {
      final int pos = buffer.position();
      writeVbyteInt(buffer, bytes.length);
      buffer.put(bytes, 0, bytes.length);
      return buffer.position() - pos;
    } else {
      return remaining;
    }
  }

  public static int estimateSizeVByteInt(int val)
  {
    if (val < (1 << 7)) {
      return 1;
    } else if (val < (1 << 14)) {
      return 2;
    } else if (val < (1 << 21)) {
      return 3;
    } else if (val < (1 << 28)) {
      return 4;
    } else {
      return 5;
    }
  }
}
