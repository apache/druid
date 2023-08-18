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
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reads fields written by {@link StringFieldWriter} or {@link StringArrayFieldWriter}.
 *
 * Strings are written in UTF8 and terminated by {@link StringFieldWriter#VALUE_TERMINATOR}. Note that this byte
 * appears in valid UTF8 encodings if and only if the string contains a NUL (char 0). Therefore, this field writer
 * cannot write out strings containing NUL characters.
 *
 * All rows are terminated by {@link StringFieldWriter#ROW_TERMINATOR}.
 *
 * Empty rows are represented in one byte: solely that {@link StringFieldWriter#ROW_TERMINATOR}. Rows that are null
 * themselves (i.e., a null array) are represented as a {@link StringFieldWriter#NULL_ROW} followed by a
 * {@link StringFieldWriter#ROW_TERMINATOR}. This encoding for null arrays is decoded by older readers as an
 * empty array; null arrays are a feature that did not exist in earlier versions of the code.
 *
 * Null strings are stored as {@link StringFieldWriter#NULL_BYTE}. All other strings are prepended by
 * {@link StringFieldWriter#NOT_NULL_BYTE} byte to differentiate them from nulls.
 *
 * This encoding allows the encoded data to be compared as bytes in a way that matches the behavior of
 * {@link org.apache.druid.segment.StringDimensionHandler#DIMENSION_SELECTOR_COMPARATOR}, except null and
 * empty list are not considered equal.
 */
public class StringFieldReader implements FieldReader
{
  private final boolean asArray;

  /**
   * Create a string reader.
   *
   * @param asArray if false, selectors from {@link #makeColumnValueSelector} behave like {@link ValueType#STRING}
   *                selectors (potentially multi-value ones). If true, selectors from {@link #makeColumnValueSelector}
   *                behave like string array selectors.
   */
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
  public boolean isNull(Memory memory, long position)
  {
    final byte firstByte = memory.getByte(position);

    if (firstByte == StringFieldWriter.NULL_ROW) {
      return true;
    } else if (!asArray) {
      return (NullHandling.replaceWithDefault() || firstByte == StringFieldWriter.NULL_BYTE)
             && memory.getByte(position + 1) == StringFieldWriter.VALUE_TERMINATOR
             && memory.getByte(position + 2) == StringFieldWriter.ROW_TERMINATOR;
    } else {
      return false;
    }
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

    /**
     * Current UTF-8 buffers, updated by {@link #computeCurrentUtf8Strings()}. Readers must only use this if
     * {@link #currentUtf8StringsIsNull} is false.
     */
    private final List<ByteBuffer> currentUtf8Strings = new ArrayList<>();

    /**
     * If true, {@link #currentUtf8Strings} must be ignored by readers, and null must be used instead. This is done
     * instead of nulling out {@link #currentUtf8Strings} to save on garbage.
     */
    private boolean currentUtf8StringsIsNull;

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

      if (currentStrings == null) {
        return null;
      }

      final int size = currentStrings.size();

      if (size == 0) {
        return asArray ? ObjectArrays.EMPTY_ARRAY : null;
      } else if (size == 1) {
        return asArray ? new Object[]{lookupName(0)} : lookupName(0);
      } else {
        final Object[] strings = new Object[size];
        for (int i = 0; i < size; i++) {
          strings[i] = lookupName(i);
        }
        return asArray ? strings : Arrays.asList(strings);
      }
    }

    @Override
    public IndexedInts getRow()
    {
      final List<ByteBuffer> strings = computeCurrentUtf8Strings();
      final int size = strings == null ? 0 : strings.size();
      indexedInts.setSize(size);
      return indexedInts;
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      final List<ByteBuffer> strings = computeCurrentUtf8Strings();

      if (strings == null) {
        return null;
      } else {
        final ByteBuffer byteBuffer = strings.get(id);
        final String s = byteBuffer != null ? StringUtils.fromUtf8(byteBuffer.duplicate()) : null;
        return extractionFn == null ? s : extractionFn.apply(s);
      }
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

      final List<ByteBuffer> strings = computeCurrentUtf8Strings();
      return strings == null ? null : strings.get(id);
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
    @Nullable
    private List<ByteBuffer> computeCurrentUtf8Strings()
    {
      final long fieldPosition = fieldPointer.position();

      if (fieldPosition != currentFieldPosition) {
        updateCurrentUtf8Strings(fieldPosition);
      }

      this.currentFieldPosition = fieldPosition;

      if (currentUtf8StringsIsNull) {
        return null;
      } else {
        return currentUtf8Strings;
      }
    }

    private void updateCurrentUtf8Strings(final long fieldPosition)
    {
      currentUtf8StringsIsNull = false;
      currentUtf8Strings.clear();

      long position = fieldPosition;
      long limit = memory.getCapacity();

      boolean rowTerminatorSeen = false;

      while (position < limit && !rowTerminatorSeen) {
        final byte kind = memory.getByte(position);
        position++;

        switch (kind) {
          case StringFieldWriter.VALUE_TERMINATOR: // Or NULL_ROW (same byte value)
            if (position == fieldPosition + 1) {
              // It was NULL_ROW.
              currentUtf8StringsIsNull = true;
            }

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

                if (len == 0 && NullHandling.replaceWithDefault()) {
                  // Empty strings and nulls are the same in this mode.
                  currentUtf8Strings.add(null);
                } else {
                  final ByteBuffer buf = FrameReaderUtils.readByteBuffer(memory, position, len);
                  currentUtf8Strings.add(buf);
                }

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
