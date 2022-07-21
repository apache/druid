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

package org.apache.druid.frame.read.columnar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.columnar.FrameColumnWriters;
import org.apache.druid.frame.write.columnar.StringFrameColumnWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;
  private final boolean asArray;

  StringFrameColumnReader(int columnNumber, boolean asArray)
  {
    this.columnNumber = columnNumber;
    this.asArray = asArray;
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);

    final boolean multiValue = isMultiValue(memory);
    final long startOfStringLengthSection = getStartOfStringLengthSection(frame.numRows(), multiValue);
    final long startOfStringDataSection = getStartOfStringDataSection(memory, frame.numRows(), multiValue);

    final BaseColumn baseColumn;

    if (asArray) {
      baseColumn = new StringArrayFrameColumn(
          frame,
          multiValue,
          memory,
          startOfStringLengthSection,
          startOfStringDataSection
      );
    } else {
      baseColumn = new StringFrameColumn(
          frame,
          multiValue,
          memory,
          startOfStringLengthSection,
          startOfStringDataSection
      );
    }

    return new ColumnPlus(
        baseColumn,
        new ColumnCapabilitiesImpl().setType(asArray ? ColumnType.STRING_ARRAY : ColumnType.STRING)
                                    .setHasMultipleValues(!asArray && multiValue)
                                    .setDictionaryEncoded(false)
                                    .setHasBitmapIndexes(false)
                                    .setHasSpatialIndexes(false)
                                    .setHasNulls(ColumnCapabilities.Capable.UNKNOWN),
        frame.numRows()
    );
  }

  private void validate(final Memory region)
  {
    // Check if column is big enough for a header
    if (region.getCapacity() < StringFrameColumnWriter.DATA_OFFSET) {
      throw new ISE("Column is not big enough for a header");
    }

    final byte typeCode = region.getByte(0);
    if (typeCode != FrameColumnWriters.TYPE_STRING) {
      throw new ISE("Column does not have the correct type code");
    }
  }

  private static boolean isMultiValue(final Memory memory)
  {
    return memory.getByte(1) == 1;
  }

  private static int getCumulativeRowLength(
      final Memory memory,
      final int physicalRow
  )
  {
    // Note: only valid to call this if multiValue = true.
    return memory.getInt(StringFrameColumnWriter.DATA_OFFSET + (long) Integer.BYTES * physicalRow);
  }

  private static long getStartOfStringLengthSection(
      final int numRows,
      final boolean multiValue
  )
  {
    if (multiValue) {
      return StringFrameColumnWriter.DATA_OFFSET + (long) Integer.BYTES * numRows;
    } else {
      return StringFrameColumnWriter.DATA_OFFSET;
    }
  }

  private static long getStartOfStringDataSection(
      final Memory memory,
      final int numRows,
      final boolean multiValue
  )
  {
    final int totalNumValues;

    if (multiValue) {
      totalNumValues = getCumulativeRowLength(memory, numRows - 1);
    } else {
      totalNumValues = numRows;
    }

    return getStartOfStringLengthSection(numRows, multiValue) + (long) Integer.BYTES * totalNumValues;
  }

  @VisibleForTesting
  static class StringFrameColumn implements DictionaryEncodedColumn<String>
  {
    private final Frame frame;
    private final boolean multiValue;
    private final Memory memory;
    private final long startOfStringLengthSection;
    private final long startOfStringDataSection;

    private StringFrameColumn(
        Frame frame,
        boolean multiValue,
        Memory memory,
        long startOfStringLengthSection,
        long startOfStringDataSection
    )
    {
      this.frame = frame;
      this.multiValue = multiValue;
      this.memory = memory;
      this.startOfStringLengthSection = startOfStringLengthSection;
      this.startOfStringDataSection = startOfStringDataSection;
    }

    @Override
    public boolean hasMultipleValues()
    {
      // Only used in segment tests that don't run on frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public int getSingleValueRow(int rowNum)
    {
      // Only used in segment tests that don't run on frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexedInts getMultiValueRow(int rowNum)
    {
      // Only used in segment tests that don't run on frames.
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      // Only used on columns from segments, not frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public int lookupId(String name)
    {
      // Only used on columns from segments, not frames.
      throw new UnsupportedOperationException();
    }

    @Override
    public int getCardinality()
    {
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }

    @Override
    public DimensionSelector makeDimensionSelector(ReadableOffset offset, @Nullable ExtractionFn extractionFn)
    {
      if (multiValue) {
        class MultiValueSelector implements DimensionSelector
        {
          private int currentRow = -1;
          private List<ByteBuffer> currentValues = null;
          private final RangeIndexedInts indexedInts = new RangeIndexedInts();

          @Override
          public int getValueCardinality()
          {
            return CARDINALITY_UNKNOWN;
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            populate();
            final ByteBuffer buf = currentValues.get(id);
            final String s = buf == null ? null : StringUtils.fromUtf8(buf.duplicate());
            return extractionFn == null ? s : extractionFn.apply(s);
          }

          @Nullable
          @Override
          public ByteBuffer lookupNameUtf8(int id)
          {
            assert supportsLookupNameUtf8();
            populate();
            return currentValues.get(id);
          }

          @Override
          public boolean supportsLookupNameUtf8()
          {
            return extractionFn == null;
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
          public IndexedInts getRow()
          {
            populate();
            return indexedInts;
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

          @Nullable
          @Override
          public Object getObject()
          {
            return defaultGetObject();
          }

          @Override
          public Class<?> classOfObject()
          {
            return String.class;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // Do nothing.
          }

          private void populate()
          {
            final int row = offset.getOffset();

            if (row != currentRow) {
              currentValues = getRowAsListUtf8(frame.physicalRow(row));
              indexedInts.setSize(currentValues.size());
              currentRow = row;
            }
          }
        }

        return new MultiValueSelector();
      } else {
        class SingleValueSelector extends BaseSingleValueDimensionSelector
        {
          @Nullable
          @Override
          protected String getValue()
          {
            final String s = getString(frame.physicalRow(offset.getOffset()));
            return extractionFn == null ? s : extractionFn.apply(s);
          }

          @Nullable
          @Override
          public ByteBuffer lookupNameUtf8(int id)
          {
            assert supportsLookupNameUtf8();
            return getStringUtf8(frame.physicalRow(offset.getOffset()));
          }

          @Override
          public boolean supportsLookupNameUtf8()
          {
            return extractionFn == null;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // Do nothing.
          }
        }

        return new SingleValueSelector();
      }
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset offset)
    {
      // Callers should use object selectors, because we have no dictionary.
      throw new UnsupportedOperationException();
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
    {
      // Callers should use object selectors, because we have no dictionary.
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorObjectSelector makeVectorObjectSelector(final ReadableVectorOffset offset)
    {
      class StringFrameVectorObjectSelector implements VectorObjectSelector
      {
        private final Object[] vector = new Object[offset.getMaxVectorSize()];
        private int id = ReadableVectorInspector.NULL_ID;

        @Override
        public Object[] getObjectVector()
        {
          computeVectorIfNeeded();
          return vector;
        }

        @Override
        public int getMaxVectorSize()
        {
          return offset.getMaxVectorSize();
        }

        @Override
        public int getCurrentVectorSize()
        {
          return offset.getCurrentVectorSize();
        }

        private void computeVectorIfNeeded()
        {
          if (id == offset.getId()) {
            return;
          }

          if (offset.isContiguous()) {
            final int start = offset.getStartOffset();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(i + start);
              vector[i] = getRowAsObject(physicalRow, true);
            }
          } else {
            final int[] offsets = offset.getOffsets();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(offsets[i]);
              vector[i] = getRowAsObject(physicalRow, true);
            }
          }

          id = offset.getId();
        }
      }

      return new StringFrameVectorObjectSelector();
    }

    @Override
    public int length()
    {
      return frame.numRows();
    }

    @Override
    public void close()
    {
      // Do nothing.
    }

    /**
     * Returns a ByteBuffer containing UTF-8 encoded string number {@code index}. The ByteBuffer is always newly
     * created, so it is OK to change its position, limit, etc. However, it may point to shared memory, so it is
     * not OK to write to its contents.
     */
    @Nullable
    private ByteBuffer getStringUtf8(final int index)
    {
      final long dataStart;
      final long dataEnd =
          startOfStringDataSection +
          memory.getInt(startOfStringLengthSection + (long) Integer.BYTES * index);

      if (index == 0) {
        dataStart = startOfStringDataSection;
      } else {
        dataStart =
            startOfStringDataSection +
            memory.getInt(startOfStringLengthSection + (long) Integer.BYTES * (index - 1));
      }

      final int dataLength = Ints.checkedCast(dataEnd - dataStart);

      if ((dataLength == 0 && NullHandling.replaceWithDefault()) ||
          (dataLength == 1 && memory.getByte(dataStart) == FrameWriterUtils.NULL_STRING_MARKER)) {
        return null;
      }

      return FrameReaderUtils.readByteBuffer(memory, dataStart, dataLength);
    }

    @Nullable
    private String getString(final int index)
    {
      final ByteBuffer stringUtf8 = getStringUtf8(index);

      if (stringUtf8 == null) {
        return null;
      } else {
        return StringUtils.fromUtf8(stringUtf8);
      }
    }

    @Nullable
    private Object getRowAsObject(final int physicalRow, final boolean decode)
    {
      if (multiValue) {
        final int cumulativeRowLength = getCumulativeRowLength(memory, physicalRow);
        final int rowLength = physicalRow == 0
                              ? cumulativeRowLength
                              : cumulativeRowLength - getCumulativeRowLength(memory, physicalRow - 1);

        if (rowLength == 0) {
          return Collections.emptyList();
        } else if (rowLength == 1) {
          final int index = cumulativeRowLength - 1;
          return decode ? getString(index) : getStringUtf8(index);
        } else {
          final List<Object> row = new ArrayList<>(rowLength);

          for (int i = 0; i < rowLength; i++) {
            final int index = cumulativeRowLength - rowLength + i;
            row.add(decode ? getString(index) : getStringUtf8(index));
          }

          return row;
        }
      } else {
        return decode ? getString(physicalRow) : getStringUtf8(physicalRow);
      }
    }

    private List<ByteBuffer> getRowAsListUtf8(final int physicalRow)
    {
      final Object object = getRowAsObject(physicalRow, false);

      if (object instanceof List) {
        //noinspection unchecked
        return (List<ByteBuffer>) object;
      } else {
        return Collections.singletonList((ByteBuffer) object);
      }
    }
  }

  static class StringArrayFrameColumn implements BaseColumn
  {
    private final StringFrameColumn delegate;

    private StringArrayFrameColumn(
        Frame frame,
        boolean multiValue,
        Memory memory,
        long startOfStringLengthSection,
        long startOfStringDataSection
    )
    {
      this.delegate = new StringFrameColumn(
          frame,
          multiValue,
          memory,
          startOfStringLengthSection,
          startOfStringDataSection
      );
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ColumnValueSelector<List> makeColumnValueSelector(ReadableOffset offset)
    {
      final DimensionSelector delegateSelector = delegate.makeDimensionSelector(offset, null);

      return new ObjectColumnSelector<List>()
      {
        @Override
        public List getObject()
        {
          final IndexedInts row = delegateSelector.getRow();
          final int sz = row.size();

          if (sz == 0) {
            return Collections.emptyList();
          } else if (sz == 1) {
            return Collections.singletonList(delegateSelector.lookupName(0));
          } else {
            final List<String> retVal = new ArrayList<>(sz);

            for (int i = 0; i < sz; i++) {
              retVal.add(delegateSelector.lookupName(i));
            }

            return retVal;
          }
        }

        @Override
        public Class<List> classOfObject()
        {
          return List.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          delegateSelector.inspectRuntimeShape(inspector);
        }
      };
    }

    @Override
    public void close()
    {
      // do nothing
    }
  }
}
