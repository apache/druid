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


package org.apache.druid.segment.nested;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorSelectorUtils;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.utils.CloseableUtils;
import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.BitSet;

public class NestedFieldLiteralDictionaryEncodedColumn implements DictionaryEncodedColumn<String>
{
  private final NestedLiteralTypeInfo.TypeSet types;
  @Nullable
  private final ColumnType singleType;
  private final ColumnarLongs longsColumn;
  private final ColumnarDoubles doublesColumn;
  private final ColumnarInts column;
  private final GenericIndexed<String> globalDictionary;
  private final FixedIndexed<Long> globalLongDictionary;
  private final FixedIndexed<Double> globalDoubleDictionary;
  private final FixedIndexed<Integer> dictionary;
  private final ImmutableBitmap nullBitmap;

  private final int adjustLongId;
  private final int adjustDoubleId;

  public NestedFieldLiteralDictionaryEncodedColumn(
      NestedLiteralTypeInfo.TypeSet types,
      ColumnarLongs longsColumn,
      ColumnarDoubles doublesColumn,
      ColumnarInts column,
      GenericIndexed<String> globalDictionary,
      FixedIndexed<Long> globalLongDictionary,
      FixedIndexed<Double> globalDoubleDictionary,
      FixedIndexed<Integer> dictionary,
      ImmutableBitmap nullBitmap
  )
  {
    this.types = types;
    this.singleType = types.getSingleType();
    this.longsColumn = longsColumn;
    this.doublesColumn = doublesColumn;
    this.column = column;
    this.globalDictionary = globalDictionary;
    this.globalLongDictionary = globalLongDictionary;
    this.globalDoubleDictionary = globalDoubleDictionary;
    this.dictionary = dictionary;
    this.nullBitmap = nullBitmap;
    this.adjustLongId = globalDictionary.size();
    this.adjustDoubleId = adjustLongId + globalLongDictionary.size();
  }

  @Override
  public int length()
  {
    return column.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return false;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    throw new IllegalStateException("Multi-value row not supported");
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    final int globalId = dictionary.get(id);
    if (globalId < globalDictionary.size()) {
      return globalDictionary.get(globalId);
    } else if (globalId < adjustLongId + globalLongDictionary.size()) {
      return String.valueOf(globalLongDictionary.get(globalId - adjustLongId));
    } else {
      return String.valueOf(globalDoubleDictionary.get(globalId - adjustDoubleId));
    }
  }

  @Override
  public int lookupId(String name)
  {
    return dictionary.indexOf(getIdFromGlobalDictionary(name));
  }

  @Override
  public int getCardinality()
  {
    return dictionary.size();
  }

  private int getIdFromGlobalDictionary(@Nullable String val)
  {
    if (val == null) {
      return 0;
    }

    if (singleType != null) {
      switch (singleType.getType()) {
        case LONG:
          return globalLongDictionary.indexOf(GuavaUtils.tryParseLong(val));
        case DOUBLE:
          return globalDoubleDictionary.indexOf(Doubles.tryParse(val));
        default:
          return globalDictionary.indexOf(val);
      }
    } else {
      int candidate = globalDictionary.indexOf(val);
      if (candidate < 0) {
        candidate = globalLongDictionary.indexOf(GuavaUtils.tryParseLong(val));
      }
      if (candidate < 0) {
        candidate = globalDoubleDictionary.indexOf(Doubles.tryParse(val));
      }
      return candidate;
    }
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      ReadableOffset offset,
      @Nullable ExtractionFn extractionFn
  )
  {
    // copy everywhere all the time
    class StringDimensionSelector extends AbstractDimensionSelector
        implements SingleValueHistoricalDimensionSelector, IdLookup
    {
      private final SingleIndexedInt row = new SingleIndexedInt();

      @Override
      public IndexedInts getRow()
      {
        row.setValue(getRowValue());
        return row;
      }

      public int getRowValue()
      {
        return column.get(offset.getOffset());
      }

      @Override
      public float getFloat()
      {
        final int localId = getRowValue();
        final int globalId = dictionary.get(localId);
        if (globalId == 0) {
          // zero
          assert NullHandling.replaceWithDefault();
          return 0f;
        } else if (globalId < adjustLongId) {
          // try to convert string to float
          Float f = Floats.tryParse(globalDictionary.get(globalId));
          return f == null ? 0f : f;
        } else if (globalId < adjustDoubleId) {
          return globalLongDictionary.get(globalId - adjustLongId).floatValue();
        } else {
          return globalDoubleDictionary.get(globalId - adjustDoubleId).floatValue();
        }
      }

      @Override
      public double getDouble()
      {
        final int localId = getRowValue();
        final int globalId = dictionary.get(localId);
        if (globalId == 0) {
          // zero
          assert NullHandling.replaceWithDefault();
          return 0.0;
        } else if (globalId < adjustLongId) {
          // try to convert string to double
          Double d = Doubles.tryParse(globalDictionary.get(globalId));
          return d == null ? 0.0 : d;
        } else if (globalId < adjustDoubleId) {
          return globalLongDictionary.get(globalId - adjustLongId).doubleValue();
        } else {
          return globalDoubleDictionary.get(globalId - adjustDoubleId);
        }
      }

      @Override
      public long getLong()
      {
        final int localId = getRowValue();
        final int globalId = dictionary.get(localId);
        if (globalId == 0) {
          // zero
          assert NullHandling.replaceWithDefault();
          return 0L;
        } else if (globalId < adjustLongId) {
          // try to convert string to long
          Long l = GuavaUtils.tryParseLong(globalDictionary.get(globalId));
          return l == null ? 0L : l;
        } else if (globalId < adjustDoubleId) {
          return globalLongDictionary.get(globalId - adjustLongId);
        } else {
          return globalDoubleDictionary.get(globalId - adjustDoubleId).longValue();
        }
      }

      @Override
      public boolean isNull()
      {
        return dictionary.get(getRowValue()) == 0;
      }

      @Override
      public IndexedInts getRow(int offset)
      {
        row.setValue(getRowValue(offset));
        return row;
      }

      @Override
      public int getRowValue(int offset)
      {
        return column.get(offset);
      }

      @Override
      public ValueMatcher makeValueMatcher(final @Nullable String value)
      {
        if (extractionFn == null) {
          final int valueId = lookupId(value);
          if (valueId >= 0) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                return getRowValue() == valueId;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("column", NestedFieldLiteralDictionaryEncodedColumn.this);
              }
            };
          } else {
            return BooleanValueMatcher.of(false);
          }
        } else {
          // Employ caching BitSet optimization
          return makeValueMatcher(Predicates.equalTo(value));
        }
      }

      @Override
      public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
      {
        final BitSet checkedIds = new BitSet(getCardinality());
        final BitSet matchingIds = new BitSet(getCardinality());

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            final int id = getRowValue();

            if (checkedIds.get(id)) {
              return matchingIds.get(id);
            } else {
              final boolean matches = predicate.apply(lookupName(id));
              checkedIds.set(id);
              if (matches) {
                matchingIds.set(id);
              }
              return matches;
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("column", NestedFieldLiteralDictionaryEncodedColumn.this);
          }
        };
      }

      @Override
      public Object getObject()
      {
        return NestedFieldLiteralDictionaryEncodedColumn.this.lookupName(getRowValue());
      }

      @Override
      public Class classOfObject()
      {
        return String.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", column);
        inspector.visit("offset", offset);
        inspector.visit("extractionFn", extractionFn);
      }

      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Override
      public String lookupName(int id)
      {
        final String value = NestedFieldLiteralDictionaryEncodedColumn.this.lookupName(id);
        return extractionFn == null ? value : extractionFn.apply(value);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return extractionFn == null ? this : null;
      }

      @Override
      public int lookupId(String name)
      {
        if (extractionFn == null) {
          return NestedFieldLiteralDictionaryEncodedColumn.this.lookupId(name);
        }
        throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
      }
    }

    return new StringDimensionSelector();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    if (singleType != null) {

      // this null handling stuff is copied from elsewhere, and also copied in basically all of the
      // numerical column selectors, there is probably some base structure that can be extracted...
      if (Types.is(singleType, ValueType.LONG)) {
        return new LongColumnSelector()
        {
          private PeekableIntIterator nullIterator = nullBitmap.peekableIterator();
          private int nullMark = -1;
          private int offsetMark = -1;

          @Override
          public long getLong()
          {
            return longsColumn.get(offset.getOffset());
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("longColumn", longsColumn);
            inspector.visit("nullBitmap", nullBitmap);
          }

          @Override
          public boolean isNull()
          {
            final int i = offset.getOffset();
            if (i < offsetMark) {
              // offset was reset, reset iterator state
              nullMark = -1;
              nullIterator = nullBitmap.peekableIterator();
            }
            offsetMark = i;
            if (nullMark < i) {
              nullIterator.advanceIfNeeded(offsetMark);
              if (nullIterator.hasNext()) {
                nullMark = nullIterator.next();
              }
            }
            return nullMark == offsetMark;
          }
        };
      } else if (Types.is(singleType, ValueType.DOUBLE)) {
        return new DoubleColumnSelector()
        {
          private PeekableIntIterator nullIterator = nullBitmap.peekableIterator();
          private int nullMark = -1;
          private int offsetMark = -1;

          @Override
          public double getDouble()
          {
            return doublesColumn.get(offset.getOffset());
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("doubleColumn", doublesColumn);
            inspector.visit("nullBitmap", nullBitmap);
          }

          @Override
          public boolean isNull()
          {
            final int i = offset.getOffset();
            if (i < offsetMark) {
              // offset was reset, reset iterator state
              nullMark = -1;
              nullIterator = nullBitmap.peekableIterator();
            }
            offsetMark = i;
            if (nullMark < i) {
              nullIterator.advanceIfNeeded(offsetMark);
              if (nullIterator.hasNext()) {
                nullMark = nullIterator.next();
              }
            }
            return nullMark == offsetMark;
          }
        };
      }
    }
    return makeDimensionSelector(offset, null);
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset offset)
  {
    // also copied from StringDictionaryEncodedColumn
    class QueryableSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector, IdLookup
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public int[] getRowVector()
      {
        if (id == offset.getId()) {
          return vector;
        }

        if (offset.isContiguous()) {
          column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }

        id = offset.getId();
        return vector;
      }

      @Override
      public int getValueCardinality()
      {
        return getCardinality();
      }

      @Nullable
      @Override
      public String lookupName(final int id)
      {
        return NestedFieldLiteralDictionaryEncodedColumn.this.lookupName(id);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return this;
      }

      @Override
      public int lookupId(@Nullable final String name)
      {
        return NestedFieldLiteralDictionaryEncodedColumn.this.lookupId(name);
      }

      @Override
      public int getCurrentVectorSize()
      {
        return offset.getCurrentVectorSize();
      }

      @Override
      public int getMaxVectorSize()
      {
        return offset.getMaxVectorSize();
      }
    }

    return new QueryableSingleValueDimensionVectorSelector();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    // also copied from StringDictionaryEncodedColumn
    class DictionaryEncodedStringSingleValueVectorObjectSelector implements VectorObjectSelector
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private final String[] strings = new String[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override

      public Object[] getObjectVector()
      {
        if (id == offset.getId()) {
          return strings;
        }

        if (offset.isContiguous()) {
          column.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          column.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }
        for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
          strings[i] = lookupName(vector[i]);
        }
        id = offset.getId();

        return strings;
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
    }

    return new DictionaryEncodedStringSingleValueVectorObjectSelector();
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
  {
    if (singleType != null) {
      if (Types.is(singleType, ValueType.LONG)) {
        return new BaseLongVectorValueSelector(offset)
        {
          private final long[] valueVector = new long[offset.getMaxVectorSize()];
          @Nullable
          private boolean[] nullVector = null;
          private int id = ReadableVectorInspector.NULL_ID;

          @Nullable
          private PeekableIntIterator nullIterator = nullBitmap.peekableIterator();
          private int offsetMark = -1;

          @Override
          public long[] getLongVector()
          {
            computeVectorsIfNeeded();
            return valueVector;
          }

          @Nullable
          @Override
          public boolean[] getNullVector()
          {
            computeVectorsIfNeeded();
            return nullVector;
          }

          private void computeVectorsIfNeeded()
          {
            if (id == offset.getId()) {
              return;
            }

            if (offset.isContiguous()) {
              if (offset.getStartOffset() < offsetMark) {
                nullIterator = nullBitmap.peekableIterator();
              }
              offsetMark = offset.getStartOffset() + offset.getCurrentVectorSize();
              longsColumn.get(valueVector, offset.getStartOffset(), offset.getCurrentVectorSize());
            } else {
              final int[] offsets = offset.getOffsets();
              if (offsets[offsets.length - 1] < offsetMark) {
                nullIterator = nullBitmap.peekableIterator();
              }
              offsetMark = offsets[offsets.length - 1];
              longsColumn.get(valueVector, offsets, offset.getCurrentVectorSize());
            }

            nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);

            id = offset.getId();
          }
        };
      } else if (Types.is(singleType, ValueType.DOUBLE)) {
        return new BaseDoubleVectorValueSelector(offset)
        {
          private final double[] valueVector = new double[offset.getMaxVectorSize()];
          @Nullable
          private boolean[] nullVector = null;
          private int id = ReadableVectorInspector.NULL_ID;

          @Nullable
          private PeekableIntIterator nullIterator = nullBitmap != null ? nullBitmap.peekableIterator() : null;
          private int offsetMark = -1;

          @Override
          public double[] getDoubleVector()
          {
            computeVectorsIfNeeded();
            return valueVector;
          }

          @Nullable
          @Override
          public boolean[] getNullVector()
          {
            computeVectorsIfNeeded();
            return nullVector;
          }

          private void computeVectorsIfNeeded()
          {
            if (id == offset.getId()) {
              return;
            }

            if (offset.isContiguous()) {
              if (offset.getStartOffset() < offsetMark) {
                nullIterator = nullBitmap.peekableIterator();
              }
              offsetMark = offset.getStartOffset() + offset.getCurrentVectorSize();
              doublesColumn.get(valueVector, offset.getStartOffset(), offset.getCurrentVectorSize());
            } else {
              final int[] offsets = offset.getOffsets();
              if (offsets[offsets.length - 1] < offsetMark) {
                nullIterator = nullBitmap.peekableIterator();
              }
              offsetMark = offsets[offsets.length - 1];
              doublesColumn.get(valueVector, offsets, offset.getCurrentVectorSize());
            }

            nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);

            id = offset.getId();
          }
        };
      }
      throw new UOE("Cannot make vector value selector for [%s] typed nested field", types);
    }
    throw new UOE("Cannot make vector value selector for variant typed [%s] nested field", types);
  }

  @Override
  public void close() throws IOException
  {
    CloseableUtils.closeAll(column, longsColumn, doublesColumn);
  }
}
