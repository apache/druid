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
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongComparators;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

/**
 * Specialized implementation for {@link FixedIndexed<Long>} which does not contain any null values, allowing it to
 * deal in java long value types instead of {@link Long} objects, and utilize specialized {@link ByteBuffer} methods
 * to more efficiently read data.
 */
public final class FixedIndexedLongs implements Indexed<Long>, HotLoopCallee
{
  public static FixedIndexedLongs read(ByteBuffer bb, ByteOrder byteOrder)
  {
    final ByteBuffer buffer = bb.asReadOnlyBuffer().order(byteOrder);
    final byte version = buffer.get();
    Preconditions.checkState(version == 0, "Unknown version [%s]", version);
    final byte flags = buffer.get();
    final boolean hasNull = (flags & NullHandling.IS_NULL_BYTE) == NullHandling.IS_NULL_BYTE ? true : false;
    final boolean isSorted = (flags & FixedIndexed.IS_SORTED_MASK) == FixedIndexed.IS_SORTED_MASK ? true : false;
    Preconditions.checkState(!hasNull, "Cannot use FixedIndexedInts for FixedIndex with null values");
    Preconditions.checkState(!(hasNull && !isSorted), "cannot have null values if not sorted");
    final int size = buffer.getInt() + (hasNull ? 1 : 0);
    final int valuesOffset = buffer.position();
    final FixedIndexedLongs fixedIndexed = new FixedIndexedLongs(
        buffer,
        isSorted,
        size,
        valuesOffset
    );
    bb.position(buffer.position() + (Double.BYTES * size));
    return fixedIndexed;
  }

  private final ByteBuffer buffer;
  private final int size;
  private final int valuesOffset;
  private final boolean isSorted;
  private final LongComparator comparator;

  private FixedIndexedLongs(
      ByteBuffer buffer,
      boolean isSorted,
      int size,
      int valuesOffset
  )
  {
    this.buffer = buffer;
    this.size = size;
    this.valuesOffset = valuesOffset;
    this.isSorted = isSorted;
    this.comparator = LongComparators.NATURAL_COMPARATOR;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Nullable
  @Override
  public Long get(int index)
  {
    return getLong(index);
  }

  @Override
  public int indexOf(@Nullable Long value)
  {
    if (value == null) {
      return -1;
    }
    return indexOf(value.longValue());
  }

  public long getLong(int index)
  {
    return buffer.getLong(valuesOffset + (index * Long.BYTES));
  }

  public int indexOf(long value)
  {
    if (!isSorted) {
      throw new UnsupportedOperationException("Reverse lookup not allowed.");
    }
    int minIndex = 0;
    int maxIndex = size - 1;
    while (minIndex <= maxIndex) {
      int currIndex = (minIndex + maxIndex) >>> 1;

      long currValue = getLong(currIndex);
      int comparison = comparator.compare(currValue, value);
      if (comparison == 0) {
        return currIndex;
      }

      if (comparison < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex - 1;
      }
    }

    return -(minIndex + 1);
  }


  public LongIterator longIterator()
  {
    final ByteBuffer copy = buffer.asReadOnlyBuffer().order(buffer.order());
    copy.position(valuesOffset);
    copy.limit(valuesOffset + (size * Long.BYTES));
    return new LongIterator()
    {
      @Override
      public long nextLong()
      {
        return copy.getLong();
      }

      @Override
      public boolean hasNext()
      {
        return copy.hasRemaining();
      }
    };
  }

  @Override
  public Iterator<Long> iterator()
  {
    final LongIterator iterator = longIterator();
    return new Iterator<Long>()
    {
      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public Long next()
      {
        return iterator.nextLong();
      }
    };
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("buffer", buffer);
    inspector.visit("comparator", comparator);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder("FixedIndexedLongs[");
    if (size() > 0) {
      for (int i = 0; i < size(); i++) {
        long value = getLong(i);
        sb.append(value).append(',').append(' ');
      }
      sb.setLength(sb.length() - 2);
    }
    sb.append(']');
    return sb.toString();
  }
}
