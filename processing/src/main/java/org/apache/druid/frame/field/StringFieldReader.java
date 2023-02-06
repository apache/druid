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

package org.apache.druid.frame.field;

import com.google.common.base.Predicate;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads fields written by {@link StringFieldWriter} or {@link StringArrayFieldWriter}.
 *
 * Strings are written in UTF8 and terminated by {@link StringFieldWriter#VALUE_TERMINATOR}. Note that this byte
 * appears in valid UTF8 encodings if and only if the string contains a NUL (char 0). Therefore, this field writer
 * cannot write out strings containing NUL characters.
 *
 * Rows are terminated by {@link StringFieldWriter#ROW_TERMINATOR}.
 *
 * Nulls are stored as {@link StringFieldWriter#NULL_BYTE}. All other strings are prepended by
 * {@link StringFieldWriter#NOT_NULL_BYTE} byte to differentiate them from nulls.
 *
 * This encoding allows the encoded data to be compared as bytes in a way that matches the behavior of
 * {@link org.apache.druid.segment.StringDimensionHandler#DIMENSION_SELECTOR_COMPARATOR}, except null and
 * empty list are not considered equal.
 */
public class StringFieldReader implements FieldReader
{
  private final boolean asArray;

  StringFieldReader(final boolean asArray)
  {
    this.asArray = asArray;
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer)
  {
    return new Selector(memory, fieldPointer, null, asArray);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    if (asArray) {
      throw new ISE("Cannot call makeDimensionSelector on field of type [%s]", ColumnType.STRING_ARRAY);
    }

    return new Selector(memory, fieldPointer, extractionFn, false);
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  /**
   * Selector that reads a value from a location pointed to by {@link ReadableFieldPointer}.
   */
  private static class Selector implements DimensionSelector
  {
    private final Memory memory;
    private final ReadableFieldPointer fieldPointer;
    @Nullable
    private final ExtractionFn extractionFn;
    private final boolean asArray;

    private long currentFieldPosition = -1;
    private final RangeIndexedInts indexedInts = new RangeIndexedInts();
    private final List<ByteBuffer> currentUtf8Strings = new ArrayList<>();

    private Selector(
        final Memory memory,
        final ReadableFieldPointer fieldPointer,
        @Nullable final ExtractionFn extractionFn,
        final boolean asArray
    )
    {
      this.memory = memory;
      this.fieldPointer = fieldPointer;
      this.extractionFn = extractionFn;
      this.asArray = asArray;
    }

    @Nullable
    @Override
    public Object getObject()
    {
      final List<ByteBuffer> currentStrings = computeCurrentUtf8Strings();
      final int size = currentStrings.size();

      if (size == 0) {
        return asArray ? Collections.emptyList() : null;
      } else if (size == 1) {
        return asArray ? Collections.singletonList(lookupName(0)) : lookupName(0);
      } else {
        final List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          strings.add(lookupName(i));
        }
        return strings;
      }
    }

    @Override
    public IndexedInts getRow()
    {
      indexedInts.setSize(computeCurrentUtf8Strings().size());
      return indexedInts;
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      final ByteBuffer byteBuffer = computeCurrentUtf8Strings().get(id);
      final String s = byteBuffer != null ? StringUtils.fromUtf8(byteBuffer.duplicate()) : null;
      return extractionFn == null ? s : extractionFn.apply(s);
    }

    @Override
    public boolean supportsLookupNameUtf8()
    {
      return extractionFn == null;
    }

    @Nullable
    @Override
    public ByteBuffer lookupNameUtf8(int id)
    {
      if (extractionFn != null) {
        throw new ISE("Cannot use lookupNameUtf8 on this selector");
      }

      return computeCurrentUtf8Strings().get(id);
    }

    @Override
    public int getValueCardinality()
    {
      return CARDINALITY_UNKNOWN;
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return false;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return null;
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
    }

    @Override
    public ValueMatcher makeValueMatcher(Predicate<String> predicate)
    {
      return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
    }

    @Override
    public Class<?> classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }

    /**
     * Update {@link #currentUtf8Strings} if needed, then return it.
     */
    private List<ByteBuffer> computeCurrentUtf8Strings()
    {
      final long fieldPosition = fieldPointer.position();

      if (fieldPosition != currentFieldPosition) {
        updateCurrentUtf8Strings(fieldPosition);
      }

      this.currentFieldPosition = fieldPosition;
      return currentUtf8Strings;
    }

    private void updateCurrentUtf8Strings(final long fieldPosition)
    {
      currentUtf8Strings.clear();

      long position = fieldPosition;
      long limit = memory.getCapacity();

      boolean rowTerminatorSeen = false;

      while (position < limit && !rowTerminatorSeen) {
        final byte kind = memory.getByte(position);
        rowTerminatorSeen = false;
        position++;

        switch (kind) {
          case StringFieldWriter.VALUE_TERMINATOR:
            // Skip; next byte will be a null/not-null byte or a row terminator.
            break;

          case StringFieldWriter.ROW_TERMINATOR:
            // Skip; this is the end of the row, so we'll fall through to the return statement.
            rowTerminatorSeen = true;
            break;

          case StringFieldWriter.NULL_BYTE:
            currentUtf8Strings.add(null);
            break;

          case StringFieldWriter.NOT_NULL_BYTE:
            for (long i = position; ; i++) {
              if (i >= limit) {
                throw new ISE("Value overrun");
              }

              final byte b = memory.getByte(i);

              if (b == StringFieldWriter.VALUE_TERMINATOR) {
                final int len = Ints.checkedCast(i - position);
                final ByteBuffer buf = FrameReaderUtils.readByteBuffer(memory, position, len);
                currentUtf8Strings.add(buf);
                position += len;
                break;
              }
            }

            break;

          default:
            throw new ISE("Invalid value start byte [%s]", kind);
        }
      }

      if (!rowTerminatorSeen) {
        throw new ISE("Unexpected end of field");
      }
    }
  }
}
