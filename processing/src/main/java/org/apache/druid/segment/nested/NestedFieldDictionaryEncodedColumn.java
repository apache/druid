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

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
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
import java.nio.ByteBuffer;
import java.util.BitSet;

public class NestedFieldDictionaryEncodedColumn<TStringDictionary extends Indexed<ByteBuffer>>
    implements DictionaryEncodedColumn<String>
{
  private final FieldTypeInfo.TypeSet types;
  @Nullable
  private final ColumnType singleType;
  private final ColumnType logicalType;
  private final ExpressionType logicalExpressionType;
  private final ColumnarLongs longsColumn;
  private final ColumnarDoubles doublesColumn;
  private final ColumnarInts column;
  private final TStringDictionary globalDictionary;
  private final FixedIndexed<Long> globalLongDictionary;
  private final FixedIndexed<Double> globalDoubleDictionary;

  private final FrontCodedIntArrayIndexed globalArrayDictionary;

  private final FixedIndexed<Integer> dictionary;
  private final ImmutableBitmap nullBitmap;

  private final int adjustLongId;
  private final int adjustDoubleId;
  private final int adjustArrayId;


  public NestedFieldDictionaryEncodedColumn(
      FieldTypeInfo.TypeSet types,
      ColumnarLongs longsColumn,
      ColumnarDoubles doublesColumn,
      ColumnarInts column,
      TStringDictionary globalDictionary,
      FixedIndexed<Long> globalLongDictionary,
      FixedIndexed<Double> globalDoubleDictionary,
      @Nullable FrontCodedIntArrayIndexed globalArrayDictionary,
      FixedIndexed<Integer> dictionary,
      ImmutableBitmap nullBitmap
  )
  {
    this.types = types;
    ColumnType leastRestrictive = null;
    for (ColumnType type : FieldTypeInfo.convertToSet(types.getByteValue())) {
      leastRestrictive = ColumnType.leastRestrictiveType(leastRestrictive, type);
    }
    this.logicalType = leastRestrictive;
    this.logicalExpressionType = ExpressionType.fromColumnTypeStrict(logicalType);
    this.singleType = types.getSingleType();
    this.longsColumn = longsColumn;
    this.doublesColumn = doublesColumn;
    this.column = column;
    this.globalDictionary = globalDictionary;
    this.globalLongDictionary = globalLongDictionary;
    this.globalDoubleDictionary = globalDoubleDictionary;
    this.globalArrayDictionary = globalArrayDictionary;
    this.dictionary = dictionary;
    this.nullBitmap = nullBitmap;
    this.adjustLongId = globalDictionary.size();
    this.adjustDoubleId = adjustLongId + globalLongDictionary.size();
    this.adjustArrayId = adjustDoubleId + globalDoubleDictionary.size();
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
      return StringUtils.fromUtf8Nullable(globalDictionary.get(globalId));
    } else if (globalId < globalDictionary.size() + globalLongDictionary.size()) {
      return String.valueOf(globalLongDictionary.get(globalId - adjustLongId));
    } else if (globalId < globalDictionary.size() + globalLongDictionary.size() + globalDoubleDictionary.size()) {
      return String.valueOf(globalDoubleDictionary.get(globalId - adjustDoubleId));
    }
    return null;
  }

  @Override
  public int lookupId(String name)
  {
    final int globalId = getIdFromGlobalDictionary(name);
    if (globalId < 0) {
      return -1;
    }
    return dictionary.indexOf(globalId);
  }

  @Override
  public int getCardinality()
  {
    return dictionary.size();
  }

  public FixedIndexed<Integer> getDictionary()
  {
    return dictionary;
  }

  private int getIdFromGlobalDictionary(@Nullable String val)
  {
    if (val == null) {
      return 0;
    }

    if (singleType != null) {
      switch (singleType.getType()) {
        case LONG:
          final Long l = GuavaUtils.tryParseLong(val);
          if (l == null) {
            return -1;
          }
          final int globalLong = globalLongDictionary.indexOf(l);
          if (globalLong < 0) {
            return -1;
          }
          return globalLong + adjustLongId;
        case DOUBLE:
          final Double d = Doubles.tryParse(val);
          if (d == null) {
            return -1;
          }
          final int globalDouble = globalDoubleDictionary.indexOf(d);
          if (globalDouble < 0) {
            return -1;
          }
          return globalDouble + adjustDoubleId;
        default:
          return globalDictionary.indexOf(StringUtils.toUtf8ByteBuffer(val));
      }
    } else {
      int candidate = globalDictionary.indexOf(StringUtils.toUtf8ByteBuffer(val));
      if (candidate < 0) {
        final Long l = GuavaUtils.tryParseLong(val);
        if (l != null) {
          candidate = globalLongDictionary.indexOf(l);
          if (candidate >= 0) {
            candidate += adjustLongId;
          }
        }
      }
      if (candidate < 0) {
        final Double d = Doubles.tryParse(val);
        if (d != null) {
          candidate = globalDoubleDictionary.indexOf(d);
          if (candidate >= 0) {
            candidate += adjustDoubleId;
          }
        }
      }
      return candidate;
    }
  }

  private Object lookupGlobalScalarObject(int globalId)
  {
    if (globalId < globalDictionary.size()) {
      return StringUtils.fromUtf8Nullable(globalDictionary.get(globalId));
    } else if (globalId < globalDictionary.size() + globalLongDictionary.size()) {
      return globalLongDictionary.get(globalId - adjustLongId);
    } else if (globalId < globalDictionary.size() + globalLongDictionary.size() + globalDoubleDictionary.size()) {
      return globalDoubleDictionary.get(globalId - adjustDoubleId);
    }
    throw new IllegalArgumentException("not a scalar in the dictionary");
  }


  /**
   * Lookup value from appropriate scalar value dictionary, coercing the value to {@link #logicalType}, particularly
   * useful for the vector query engine which prefers all the types are consistent
   * <p>
   * This method should NEVER be used when values must round trip to be able to be looked up from the array value
   * dictionary since it might coerce element values to a different type
   */
  @Nullable
  private Object lookupGlobalScalarValueAndCast(int globalId)
  {

    if (globalId == 0) {
      return null;
    }
    if (singleType != null) {
      return lookupGlobalScalarObject(globalId);
    } else {
      final ExprEval<?> eval = ExprEval.ofType(logicalExpressionType, lookupGlobalScalarObject(globalId));
      return eval.value();
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
          return 0f;
        } else if (globalId < adjustLongId) {
          // try to convert string to float
          Float f = Floats.tryParse(StringUtils.fromUtf8(globalDictionary.get(globalId)));
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
          return 0.0;
        } else if (globalId < adjustLongId) {
          // try to convert string to double
          Double d = Doubles.tryParse(StringUtils.fromUtf8(globalDictionary.get(globalId)));
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
          return 0L;
        } else if (globalId < adjustLongId) {
          // try to convert string to long
          Long l = GuavaUtils.tryParseLong(StringUtils.fromUtf8(globalDictionary.get(globalId)));
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
        if (dictionary.get(getRowValue()) == 0) {
          return true;
        }
        return DimensionHandlerUtils.isNumericNull(getObject());
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
              public boolean matches(boolean includeUnknown)
              {
                final int rowId = getRowValue();
                return (includeUnknown && rowId == 0) || rowId == valueId;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("column", NestedFieldDictionaryEncodedColumn.this);
              }
            };
          } else {
            return new ValueMatcher()
            {
              @Override
              public boolean matches(boolean includeUnknown)
              {
                return includeUnknown && getRowValue() == 0;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("column", NestedFieldDictionaryEncodedColumn.this);
              }
            };
          }
        } else {
          // Employ caching BitSet optimization
          return makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo(value));
        }
      }

      @Override
      public ValueMatcher makeValueMatcher(final DruidPredicateFactory predicateFactory)
      {
        final BitSet checkedIds = new BitSet(getCardinality());
        final BitSet matchingIds = new BitSet(getCardinality());
        final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches(boolean includeUnknown)
          {
            final int id = getRowValue();

            if (checkedIds.get(id)) {
              return matchingIds.get(id);
            } else {
              final String rowVal = lookupName(id);
              final boolean matches = predicate.apply(rowVal).matches(includeUnknown);
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
            inspector.visit("column", NestedFieldDictionaryEncodedColumn.this);
          }
        };
      }

      @Override
      public Object getObject()
      {
        return NestedFieldDictionaryEncodedColumn.this.lookupName(getRowValue());
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
        final String value = NestedFieldDictionaryEncodedColumn.this.lookupName(id);
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
          return NestedFieldDictionaryEncodedColumn.this.lookupId(name);
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
    if (singleType == null || singleType.isArray()) {
      return new ColumnValueSelector<Object>()
      {

        private PeekableIntIterator nullIterator = nullBitmap.peekableIterator();
        private int nullMark = -1;
        private int offsetMark = -1;

        @Nullable
        @Override
        public Object getObject()
        {
          final int localId = column.get(offset.getOffset());
          final int globalId = dictionary.get(localId);
          if (globalId < adjustArrayId) {
            return lookupGlobalScalarObject(globalId);
          } else {
            int[] arr = globalArrayDictionary.get(globalId - adjustArrayId);
            if (arr == null) {
              return null;
            }
            final Object[] array = new Object[arr.length];
            for (int i = 0; i < arr.length; i++) {
              array[i] = lookupGlobalScalarObject(arr[i]);
            }
            return array;
          }
        }

        @Override
        public float getFloat()
        {
          final int localId = column.get(offset.getOffset());
          final int globalId = dictionary.get(localId);
          if (globalId == 0) {
            // zero
            return 0f;
          } else if (globalId < adjustLongId) {
            // try to convert string to float
            Float f = Floats.tryParse(StringUtils.fromUtf8(globalDictionary.get(globalId)));
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
          final int localId = column.get(offset.getOffset());
          final int globalId = dictionary.get(localId);
          if (globalId == 0) {
            // zero
            return 0.0;
          } else if (globalId < adjustLongId) {
            // try to convert string to double
            Double d = Doubles.tryParse(StringUtils.fromUtf8(globalDictionary.get(globalId)));
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
          final int localId = column.get(offset.getOffset());
          final int globalId = dictionary.get(localId);
          if (globalId == 0) {
            // zero
            return 0L;
          } else if (globalId < adjustLongId) {
            // try to convert string to long
            Long l = GuavaUtils.tryParseLong(StringUtils.fromUtf8(globalDictionary.get(globalId)));
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
          if (nullMark == offsetMark) {
            return true;
          }
          final int localId = column.get(offset.getOffset());
          final int globalId = dictionary.get(localId);
          // zero is always null
          if (globalId == 0) {
            return true;
          } else if (globalId < adjustLongId) {
            final String value = StringUtils.fromUtf8Nullable(globalDictionary.get(globalId));
            return GuavaUtils.tryParseLong(value) == null && Doubles.tryParse(value) == null;
          }
          // if id is less than array ids, it is definitely a number and not null (since null is 0)
          return globalId >= adjustArrayId;
        }

        @Override
        public Class<?> classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("longColumn", longsColumn);
          inspector.visit("nullBitmap", nullBitmap);
        }
      };
    }

    // single type strings, use a dimension selector
    return makeDimensionSelector(offset, null);
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset offset)
  {
    final class StringVectorSelector extends StringUtf8DictionaryEncodedColumn.StringSingleValueDimensionVectorSelector
    {
      public StringVectorSelector()
      {
        super(column, offset);
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
        return NestedFieldDictionaryEncodedColumn.this.lookupName(id);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        // only supported for single type string columns
        return globalDictionary.get(dictionary.indexOf(id));
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return singleType != null && singleType.is(ValueType.STRING);
      }

      @Override
      public int lookupId(@Nullable String name)
      {
        return NestedFieldDictionaryEncodedColumn.this.lookupId(name);
      }
    }

    return new StringVectorSelector();
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    // if the logical type is string, use simplified string vector object selector
    if (ColumnType.STRING.equals(logicalType)) {
      final class StringVectorSelector extends StringUtf8DictionaryEncodedColumn.StringVectorObjectSelector
      {
        public StringVectorSelector()
        {
          super(column, offset);
        }

        @Nullable
        @Override
        public String lookupName(int id)
        {
          return NestedFieldDictionaryEncodedColumn.this.lookupName(id);
        }
      }

      return new StringVectorSelector();
    }
    // mixed type, this coerces values to the logical type so that the vector engine can deal with consistently typed
    // values
    return new VariantColumn.VariantVectorObjectSelector(
        offset,
        column,
        globalArrayDictionary,
        logicalExpressionType,
        adjustArrayId
    )
    {
      @Override
      public int adjustDictionaryId(int id)
      {
        return dictionary.get(id);
      }

      @Override
      public @Nullable Object lookupScalarValue(int dictionaryId)
      {
        return NestedFieldDictionaryEncodedColumn.this.lookupGlobalScalarObject(dictionaryId);
      }

      @Override
      public @Nullable Object lookupScalarValueAndCast(int dictionaryId)
      {
        return NestedFieldDictionaryEncodedColumn.this.lookupGlobalScalarValueAndCast(dictionaryId);
      }
    };
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

            if (nullIterator != null) {
              nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);
            }

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

            if (nullIterator != null) {
              nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);
            }

            id = offset.getId();
          }
        };
      }
      throw DruidException.defensive("Cannot make vector value selector for [%s] typed nested field", types);
    }
    if (FieldTypeInfo.convertToSet(types.getByteValue()).stream().allMatch(TypeSignature::isNumeric)) {
      return new BaseDoubleVectorValueSelector(offset)
      {
        private final double[] valueVector = new double[offset.getMaxVectorSize()];
        private final int[] idVector = new int[offset.getMaxVectorSize()];
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
            column.get(idVector, offset.getStartOffset(), offset.getCurrentVectorSize());
          } else {
            final int[] offsets = offset.getOffsets();
            if (offsets[offsets.length - 1] < offsetMark) {
              nullIterator = nullBitmap.peekableIterator();
            }
            offsetMark = offsets[offsets.length - 1];
            column.get(idVector, offsets, offset.getCurrentVectorSize());
          }
          for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
            final int globalId = dictionary.get(idVector[i]);
            if (globalId == 0) {
              continue;
            }
            if (globalId < adjustDoubleId) {
              valueVector[i] = globalLongDictionary.get(globalId - adjustLongId).doubleValue();
            } else {
              valueVector[i] = globalDoubleDictionary.get(globalId - adjustDoubleId).doubleValue();
            }
          }

          if (nullIterator != null) {
            nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);
          }

          id = offset.getId();
        }
      };
    }
    throw DruidException.defensive("Cannot make vector value selector for variant typed [%s] nested field", types);
  }

  @Override
  public void close() throws IOException
  {
    CloseableUtils.closeAll(column, longsColumn, doublesColumn);
  }
}
