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
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.TypeStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Fixed width value implementation of {@link Indexed}, stored simply as a header that contains the number of values,
 * and then the values stored sequentially.
 *
 *    | version (byte) | flags (byte) | size (number of elements) (int) | [value1 (width)] [value2 (width)] ... |
 *
 * The current version is always 0. The flags contain information about whether the values are sorted (and so supports
 * {@link #indexOf(Object)}) and if so, if there is a null value present. The nulls bit is 0x01, and the sorted bit
 * is 0x02, which set {@link #hasNull} and {@link #isSorted} respectively.
 *
 * If {@link #hasNull} is set, id 0 is ALWAYS null, so the comparator should be 'nulls first' or else behavior will
 * be unexpected. {@link #hasNull} can only be set if also {@link #isSorted} is set, since the null value is not
 * actually stored in the values section.
 */
public class FixedIndexed<T> implements Indexed<T>
{
  public static final byte IS_SORTED_MASK = 0x02;

  public static <T> FixedIndexed<T> read(ByteBuffer bb, TypeStrategy<T> strategy, ByteOrder byteOrder, int width)
  {
    final ByteBuffer buffer = bb.asReadOnlyBuffer().order(byteOrder);
    final byte version = buffer.get();
    Preconditions.checkState(version == 0, "Unknown version [%s]", version);
    final byte flags = buffer.get();
    final boolean hasNull = (flags & NullHandling.IS_NULL_BYTE) == NullHandling.IS_NULL_BYTE ? true : false;
    final boolean isSorted = (flags & IS_SORTED_MASK) == IS_SORTED_MASK ? true : false;
    Preconditions.checkState(!(hasNull && !isSorted), "cannot have null values if not sorted");
    final int size = buffer.getInt() + (hasNull ? 1 : 0);
    final int valuesOffset = buffer.position();
    final FixedIndexed<T> fixedIndexed = new FixedIndexed<>(
        buffer,
        strategy,
        hasNull,
        isSorted,
        width,
        size,
        valuesOffset
    );
    bb.position(buffer.position() + (width * size));
    return fixedIndexed;
  }

  private final ByteBuffer buffer;
  private final TypeStrategy<T> typeStrategy;
  private final int width;
  private final int size;
  private final int valuesOffset;
  private final boolean hasNull;
  private final boolean isSorted;
  private final Comparator<T> comparator;

  private FixedIndexed(
      ByteBuffer buffer,
      TypeStrategy<T> typeStrategy,
      boolean hasNull,
      boolean isSorted,
      int width,
      int size,
      int valuesOffset
  )
  {
    this.buffer = buffer;
    this.typeStrategy = typeStrategy;
    Preconditions.checkArgument(width > 0, "FixedIndexed requires a fixed width value type");
    this.width = width;
    this.size = size;
    this.valuesOffset = valuesOffset;
    this.hasNull = hasNull;
    this.isSorted = isSorted;
    this.comparator = Comparator.nullsFirst(typeStrategy);
  }

  @Override
  public int size()
  {
    return size;
  }

  @Nullable
  @Override
  public T get(int index)
  {
    if (hasNull) {
      if (index == 0) {
        return null;
      }
      return typeStrategy.read(buffer, valuesOffset + ((index - 1) * width));
    } else {
      return typeStrategy.read(buffer, valuesOffset + (index * width));
    }
  }

  @Override
  public int indexOf(@Nullable T value)
  {
    if (!isSorted) {
      throw new UnsupportedOperationException("Reverse lookup not allowed.");
    }
    int minIndex = 0;
    int maxIndex = size - 1;
    while (minIndex <= maxIndex) {
      int currIndex = (minIndex + maxIndex) >>> 1;

      T currValue = get(currIndex);
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

  @Override
  public Iterator<T> iterator()
  {
    return IndexedIterable.create(this).iterator();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("buffer", buffer);
    inspector.visit("typeStrategy", typeStrategy);
    inspector.visit("comparator", comparator);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder("FixedIndexed[");
    if (size() > 0) {
      for (int i = 0; i < size(); i++) {
        T value = get(i);
        sb.append(value).append(',').append(' ');
      }
      sb.setLength(sb.length() - 2);
    }
    sb.append(']');
    return sb.toString();
  }
}
