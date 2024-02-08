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
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
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
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ColumnTypeFactory;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;
import org.apache.druid.segment.vector.BaseDoubleVectorValueSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorSelectorUtils;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * {@link NestedCommonFormatColumn} for single type array columns, and mixed type columns. If {@link #variantTypes}
 * is non-null, the column is composed of all of the types defined there, otherwise all rows are consistently
 * {@link #logicalType}. If mixed type, logical type is set by {@link ColumnType#leastRestrictiveType}.
 */
public class VariantColumn<TStringDictionary extends Indexed<ByteBuffer>>
    implements DictionaryEncodedColumn<String>, NestedCommonFormatColumn
{
  private final TStringDictionary stringDictionary;
  private final FixedIndexed<Long> longDictionary;
  private final FixedIndexed<Double> doubleDictionary;
  private final FrontCodedIntArrayIndexed arrayDictionary;
  private final ColumnarInts encodedValueColumn;
  private final ImmutableBitmap nullValueBitmap;
  private final ColumnType logicalType;
  private final ExpressionType logicalExpressionType;
  @Nullable
  private final FieldTypeInfo.TypeSet variantTypes;
  private final int adjustLongId;
  private final int adjustDoubleId;
  private final int adjustArrayId;

  public VariantColumn(
      TStringDictionary stringDictionary,
      FixedIndexed<Long> longDictionary,
      FixedIndexed<Double> doubleDictionary,
      FrontCodedIntArrayIndexed arrayDictionary,
      ColumnarInts encodedValueColumn,
      ImmutableBitmap nullValueBitmap,
      ColumnType logicalType,
      @Nullable Byte variantTypeSetByte
  )
  {
    this.stringDictionary = stringDictionary;
    this.longDictionary = longDictionary;
    this.doubleDictionary = doubleDictionary;
    this.arrayDictionary = arrayDictionary;
    this.encodedValueColumn = encodedValueColumn;
    this.nullValueBitmap = nullValueBitmap;
    this.logicalExpressionType = ExpressionType.fromColumnTypeStrict(logicalType);
    this.variantTypes = variantTypeSetByte == null ? null : new FieldTypeInfo.TypeSet(variantTypeSetByte);
    // use the variant type bytes if set, in current code the logical type should have been computed via this same means
    // however older versions of the code had a bug which could incorrectly classify mixed types as nested data
    if (variantTypeSetByte != null) {
      ColumnType theType = null;
      for (ColumnType type : FieldTypeInfo.convertToSet(variantTypeSetByte)) {
        theType = ColumnType.leastRestrictiveType(theType, type);
      }
      if (theType != null) {
        // sign bit is used to indicate empty arrays, this
        if (variantTypeSetByte < 0 && !theType.isArray()) {
          theType = ColumnTypeFactory.getInstance().ofArray(theType);
        }
        this.logicalType = theType;
      } else {
        this.logicalType = logicalType;
      }
    } else {
      this.logicalType = logicalType;
    }
    this.adjustLongId = stringDictionary.size();
    this.adjustDoubleId = adjustLongId + longDictionary.size();
    this.adjustArrayId = adjustDoubleId + doubleDictionary.size();
  }

  @Override
  public ColumnType getLogicalType()
  {
    return logicalType;
  }

  @Override
  public Indexed<String> getStringDictionary()
  {
    return new StringEncodingStrategies.Utf8ToStringIndexed(stringDictionary);
  }

  @Override
  public Indexed<Long> getLongDictionary()
  {
    return longDictionary;
  }

  @Override
  public Indexed<Double> getDoubleDictionary()
  {
    return doubleDictionary;
  }

  @Override
  public Indexed<Object[]> getArrayDictionary()
  {
    Iterable<Object[]> arrays = () -> new Iterator<Object[]>()
    {
      final Iterator<int[]> delegate = arrayDictionary.iterator();

      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public Object[] next()
      {
        final int[] next = delegate.next();
        final Object[] nextArray = new Object[next.length];
        for (int i = 0; i < nextArray.length; i++) {
          nextArray[i] = lookupId(next[i]);
        }
        return nextArray;
      }

      @Nullable
      private Object lookupId(int id)
      {
        if (id == 0) {
          return null;
        }
        final int adjustLongId = stringDictionary.size();
        final int adjustDoubleId = stringDictionary.size() + longDictionary.size();
        if (id < adjustLongId) {
          return StringUtils.fromUtf8Nullable(stringDictionary.get(id));
        } else if (id < adjustDoubleId) {
          return longDictionary.get(id - adjustLongId);
        } else if (id < adjustDoubleId + doubleDictionary.size()) {
          return doubleDictionary.get(id - adjustDoubleId);
        }
        throw new IAE("Unknown id [%s]", id);
      }
    };
    return new Indexed<Object[]>()
    {
      @Override
      public int size()
      {
        return arrayDictionary.size();
      }

      @Nullable
      @Override
      public Object[] get(int index)
      {
        throw new UnsupportedOperationException("get not supported");
      }

      @Override
      public int indexOf(@Nullable Object[] value)
      {
        throw new UnsupportedOperationException("indexOf not supported");
      }

      @Override
      public Iterator<Object[]> iterator()
      {
        return arrays.iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // meh
      }
    };
  }

  @Override
  public SortedMap<String, FieldTypeInfo.MutableTypeSet> getFieldTypeInfo()
  {
    if (variantTypes != null) {
      FieldTypeInfo.MutableTypeSet rootOnlyType = new FieldTypeInfo.MutableTypeSet(variantTypes.getByteValue());
      SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = new TreeMap<>();
      fields.put(NestedPathFinder.JSON_PATH_ROOT, rootOnlyType);
      return fields;
    }
    FieldTypeInfo.MutableTypeSet rootOnlyType = new FieldTypeInfo.MutableTypeSet().add(getLogicalType());
    SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = new TreeMap<>();
    fields.put(NestedPathFinder.JSON_PATH_ROOT, rootOnlyType);
    return fields;
  }

  @Override
  public int length()
  {
    return encodedValueColumn.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return false;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return encodedValueColumn.get(rowNum);
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
    if (id < stringDictionary.size()) {
      return StringUtils.fromUtf8Nullable(stringDictionary.get(id));
    } else if (id < stringDictionary.size() + longDictionary.size()) {
      return String.valueOf(longDictionary.get(id - adjustLongId));
    } else if (id < stringDictionary.size() + longDictionary.size() + doubleDictionary.size()) {
      return String.valueOf(doubleDictionary.get(id - adjustDoubleId));
    }
    return null;
  }

  @Override
  public int lookupId(String val)
  {
    if (val == null) {
      return 0;
    }
    int candidate = stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(val));
    if (candidate >= 0) {
      return candidate;
    }
    final Long l = GuavaUtils.tryParseLong(val);
    if (l != null) {
      candidate = longDictionary.indexOf(l);
      if (candidate >= 0) {
        candidate += adjustLongId;
        return candidate;
      }
    }
    final Double d = Doubles.tryParse(val);
    if (d != null) {
      candidate = doubleDictionary.indexOf(d);
      if (candidate >= 0) {
        candidate += adjustDoubleId;
        return candidate;
      }
    }

    // not in here, we can't really do anything cool here
    return -1;
  }


  public IntSet lookupIds(String val)
  {
    IntSet intList = new IntArraySet(3);
    if (val == null) {
      intList.add(0);
      return intList;
    }
    int candidate = stringDictionary.indexOf(StringUtils.toUtf8ByteBuffer(val));
    if (candidate >= 0) {
      intList.add(candidate);
    }
    Long l = GuavaUtils.tryParseLong(val);
    if (l != null) {
      candidate = longDictionary.indexOf(l);
      if (candidate >= 0) {
        candidate += adjustLongId;
        intList.add(candidate);
      }
    }
    Double d = Doubles.tryParse(val);
    if (d != null) {
      candidate = doubleDictionary.indexOf(d);
      if (candidate >= 0) {
        candidate += adjustDoubleId;
        intList.add(candidate);
      }
    }

    return intList;
  }

  @Override
  public int getCardinality()
  {
    if (logicalType.isArray() && variantTypes == null) {
      return arrayDictionary.size();
    }
    // this probably isn't correct if we expose this as a multi-value dimension instead of an array, which would leave
    // the array dictionary out of this computation
    return stringDictionary.size() + longDictionary.size() + doubleDictionary.size() + arrayDictionary.size();
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      ReadableOffset offset,
      @Nullable ExtractionFn extractionFn
  )
  {
    if (variantTypes == null && logicalType.isArray()) {
      throw new IAE("Dimension selector is currently unsupported for [%s]", logicalType);
    }
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
        return encodedValueColumn.get(offset.getOffset());
      }

      @Override
      public float getFloat()
      {
        final int id = getRowValue();
        if (id == 0) {
          // zero
          return 0f;
        } else if (id < adjustLongId) {
          // try to convert string to float
          Float f = Floats.tryParse(StringUtils.fromUtf8(stringDictionary.get(id)));
          return f == null ? 0f : f;
        } else if (id < adjustDoubleId) {
          return longDictionary.get(id - adjustLongId).floatValue();
        } else {
          return doubleDictionary.get(id - adjustDoubleId).floatValue();
        }
      }

      @Override
      public double getDouble()
      {
        final int id = getRowValue();
        if (id == 0) {
          // zero
          return 0.0;
        } else if (id < adjustLongId) {
          // try to convert string to double
          Double d = Doubles.tryParse(StringUtils.fromUtf8(stringDictionary.get(id)));
          return d == null ? 0.0 : d;
        } else if (id < adjustDoubleId) {
          return longDictionary.get(id - adjustLongId).doubleValue();
        } else {
          return doubleDictionary.get(id - adjustDoubleId);
        }
      }

      @Override
      public long getLong()
      {
        final int id = getRowValue();
        if (id == 0) {
          // zero
          return 0L;
        } else if (id < adjustLongId) {
          // try to convert string to long
          Long l = GuavaUtils.tryParseLong(StringUtils.fromUtf8(stringDictionary.get(id)));
          return l == null ? 0L : l;
        } else if (id < adjustDoubleId) {
          return longDictionary.get(id - adjustLongId);
        } else {
          return doubleDictionary.get(id - adjustDoubleId).longValue();
        }
      }

      @Override
      public boolean isNull()
      {
        if (getRowValue() == 0) {
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
        return encodedValueColumn.get(offset);
      }

      @Override
      public ValueMatcher makeValueMatcher(final @Nullable String value)
      {
        if (extractionFn == null) {
          final IntSet valueIds = VariantColumn.this.lookupIds(value);
          if (valueIds.size() > 0) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches(boolean includeUnknown)
              {
                final int rowId = getRowValue();
                // null is always 0
                return (includeUnknown && rowId == 0) || valueIds.contains(getRowValue());
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("column", VariantColumn.this);
              }
            };
          } else {
            return new ValueMatcher()
            {
              @Override
              public boolean matches(boolean includeUnknown)
              {
                // null is always 0
                return includeUnknown && getRowValue() == 0;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("column", VariantColumn.this);
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
              final boolean matches = predicate.apply(lookupName(id)).matches(includeUnknown);
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
            inspector.visit("column", VariantColumn.this);
          }
        };
      }

      @Override
      public Object getObject()
      {
        return VariantColumn.this.lookupName(getRowValue());
      }

      @Override
      public Class classOfObject()
      {
        return String.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", encodedValueColumn);
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
        final String value = VariantColumn.this.lookupName(id);
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
          return VariantColumn.this.lookupId(name);
        }
        throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
      }
    }

    return new StringDimensionSelector();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return new ColumnValueSelector<Object>()
    {
      private PeekableIntIterator nullIterator = nullValueBitmap.peekableIterator();
      private int nullMark = -1;
      private int offsetMark = -1;

      @Nullable
      @Override
      public Object getObject()
      {
        final int id = encodedValueColumn.get(offset.getOffset());
        if (id < adjustArrayId) {
          return lookupScalarValue(id);
        } else {
          int[] arr = arrayDictionary.get(id - adjustArrayId);
          if (arr == null) {
            return null;
          }
          final Object[] array = new Object[arr.length];
          for (int i = 0; i < arr.length; i++) {
            array[i] = lookupScalarValue(arr[i]);
          }
          return array;
        }
      }

      @Override
      public float getFloat()
      {
        final int id = encodedValueColumn.get(offset.getOffset());
        if (id == 0) {
          // zero
          return 0f;
        } else if (id < adjustLongId) {
          // try to convert string to float
          Float f = Floats.tryParse(StringUtils.fromUtf8(stringDictionary.get(id)));
          return f == null ? 0f : f;
        } else if (id < adjustDoubleId) {
          return longDictionary.get(id - adjustLongId).floatValue();
        } else if (id < adjustArrayId) {
          return doubleDictionary.get(id - adjustDoubleId).floatValue();
        }
        return 0L;
      }

      @Override
      public double getDouble()
      {
        final int id = encodedValueColumn.get(offset.getOffset());
        if (id == 0) {
          // zero
          return 0.0;
        } else if (id < adjustLongId) {
          // try to convert string to double
          Double d = Doubles.tryParse(StringUtils.fromUtf8(stringDictionary.get(id)));
          return d == null ? 0.0 : d;
        } else if (id < adjustDoubleId) {
          return longDictionary.get(id - adjustLongId).doubleValue();
        } else if (id < adjustArrayId) {
          return doubleDictionary.get(id - adjustDoubleId);
        }
        return 0.0;
      }

      @Override
      public long getLong()
      {
        final int id = encodedValueColumn.get(offset.getOffset());
        if (id == 0) {
          // zero
          return 0L;
        } else if (id < adjustLongId) {
          // try to convert string to long
          Long l = GuavaUtils.tryParseLong(StringUtils.fromUtf8(stringDictionary.get(id)));
          return l == null ? 0L : l;
        } else if (id < adjustDoubleId) {
          return longDictionary.get(id - adjustLongId);
        } else if (id < adjustArrayId) {
          return doubleDictionary.get(id - adjustDoubleId).longValue();
        } else {
          return 0L;
        }
      }

      @Override
      public boolean isNull()
      {
        final int i = offset.getOffset();
        if (i < offsetMark) {
          // offset was reset, reset iterator state
          nullMark = -1;
          nullIterator = nullValueBitmap.peekableIterator();
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
        final int id = encodedValueColumn.get(offset.getOffset());
        // zero is always null
        if (id == 0) {
          return true;
        } else if (id < adjustLongId) {
          final String value = StringUtils.fromUtf8Nullable(stringDictionary.get(id));
          return GuavaUtils.tryParseLong(value) == null && Doubles.tryParse(value) == null;
        }
        // if id is less than array ids, its definitely a number and not null (since null is 0)
        return id >= adjustArrayId;
      }

      @Override
      public Class<?> classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("encodedValueColumn", encodedValueColumn);
      }
    };
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset offset)
  {
    final class StringVectorSelector extends StringUtf8DictionaryEncodedColumn.StringSingleValueDimensionVectorSelector
    {
      public StringVectorSelector()
      {
        super(encodedValueColumn, offset);
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
        return VariantColumn.this.lookupName(id);
      }

      @Override
      public int lookupId(@Nullable String name)
      {
        return VariantColumn.this.lookupId(name);
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
  public VectorValueSelector makeVectorValueSelector(ReadableVectorOffset offset)
  {
    if (FieldTypeInfo.convertToSet(variantTypes.getByteValue()).stream().allMatch(x -> x.isNumeric())) {
      return new BaseDoubleVectorValueSelector(offset)
      {
        private final double[] valueVector = new double[offset.getMaxVectorSize()];
        private final int[] idVector = new int[offset.getMaxVectorSize()];
        @Nullable
        private boolean[] nullVector = null;
        private int id = ReadableVectorInspector.NULL_ID;

        @Nullable
        private PeekableIntIterator nullIterator = nullValueBitmap != null ? nullValueBitmap.peekableIterator() : null;
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
              nullIterator = nullValueBitmap.peekableIterator();
            }
            offsetMark = offset.getStartOffset() + offset.getCurrentVectorSize();
            encodedValueColumn.get(idVector, offset.getStartOffset(), offset.getCurrentVectorSize());
          } else {
            final int[] offsets = offset.getOffsets();
            if (offsets[offsets.length - 1] < offsetMark) {
              nullIterator = nullValueBitmap.peekableIterator();
            }
            offsetMark = offsets[offsets.length - 1];
            encodedValueColumn.get(idVector, offsets, offset.getCurrentVectorSize());
          }
          for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
            int dictId = idVector[i];
            if (dictId == 0) {
              valueVector[i] = 0.0;
            } else if (dictId < adjustDoubleId) {
              valueVector[i] = longDictionary.get(dictId - adjustLongId).doubleValue();
            } else {
              valueVector[i] = doubleDictionary.get(dictId - adjustDoubleId).doubleValue();
            }
          }

          if (nullIterator != null) {
            nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullIterator);
          }

          id = offset.getId();
        }
      };
    }
    throw DruidException.defensive("Cannot make vector value selector for variant typed [%s] column", variantTypes);
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    return new VariantVectorObjectSelector(
        offset,
        encodedValueColumn,
        arrayDictionary,
        logicalExpressionType,
        adjustArrayId
    )
    {
      @Override
      public int adjustDictionaryId(int id)
      {
        return id;
      }

      @Nullable
      @Override
      public Object lookupScalarValue(int dictionaryId)
      {
        return VariantColumn.this.lookupScalarValue(dictionaryId);
      }

      @Nullable
      @Override
      public Object lookupScalarValueAndCast(int dictionaryId)
      {
        return VariantColumn.this.lookupScalarValueAndCast(dictionaryId);
      }
    };
  }

  @Override
  public void close() throws IOException
  {
    encodedValueColumn.close();
  }

  /**
   * Lookup value from appropriate scalar value dictionary, coercing the value to {@link #logicalType}, particularly
   * useful for the vector query engine which prefers all the types are consistent
   * <p>
   * This method should NEVER be used when values must round trip to be able to be looked up from the array value
   * dictionary since it might coerce element values to a different type
   */
  @Nullable
  private Object lookupScalarValueAndCast(int id)
  {
    if (id == 0) {
      return null;
    }
    if (variantTypes == null) {
      return lookupScalarValue(id);
    } else {
      ExprEval eval = ExprEval.ofType(logicalExpressionType, lookupScalarValue(id));
      return eval.value();
    }
  }

  @Nullable
  private Object lookupScalarValue(int id)
  {
    if (id < adjustLongId) {
      return StringUtils.fromUtf8Nullable(stringDictionary.get(id));
    } else if (id < adjustDoubleId) {
      return longDictionary.get(id - adjustLongId);
    } else if (id < adjustArrayId) {
      return doubleDictionary.get(id - adjustDoubleId);
    }
    throw new IllegalArgumentException("not a scalar in the dictionary");
  }

  /**
   * Make a {@link VectorObjectSelector} for a dictionary encoded column that coerces mixed types to a common type
   */
  public abstract static class VariantVectorObjectSelector implements VectorObjectSelector
  {
    private final int[] vector;
    private final Object[] objects;
    private int offsetId = ReadableVectorInspector.NULL_ID;
    private final ReadableVectorOffset offset;
    private final ColumnarInts encodedValueColumn;
    private final FrontCodedIntArrayIndexed arrayDictionary;
    private final ExpressionType logicalExpressionType;
    private final int adjustArrayId;

    protected VariantVectorObjectSelector(
        ReadableVectorOffset offset,
        ColumnarInts encodedValueColumn,
        FrontCodedIntArrayIndexed arrayDictionary,
        ExpressionType logicalExpressionType,
        int adjustArrayId
    )
    {
      this.offset = offset;
      this.encodedValueColumn = encodedValueColumn;
      this.arrayDictionary = arrayDictionary;
      this.logicalExpressionType = logicalExpressionType;
      this.adjustArrayId = adjustArrayId;
      this.objects = new Object[offset.getMaxVectorSize()];
      this.vector = new int[offset.getMaxVectorSize()];
    }

    public abstract int adjustDictionaryId(int id);

    @Nullable
    public abstract Object lookupScalarValue(int dictionaryId);

    @Nullable
    public abstract Object lookupScalarValueAndCast(int dictionaryId);

    @Override
    public Object[] getObjectVector()
    {
      if (offsetId == offset.getId()) {
        return objects;
      }

      if (offset.isContiguous()) {
        encodedValueColumn.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
      } else {
        encodedValueColumn.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
      }
      for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
        final int dictionaryId = adjustDictionaryId(vector[i]);
        if (dictionaryId < adjustArrayId) {
          objects[i] = lookupScalarValueAndCast(dictionaryId);
        } else {
          int[] arr = arrayDictionary.get(dictionaryId - adjustArrayId);
          if (arr == null) {
            objects[i] = null;
          } else {
            final Object[] array = new Object[arr.length];
            for (int j = 0; j < arr.length; j++) {
              array[j] = lookupScalarValue(arr[j]);
            }
            objects[i] = ExprEval.ofType(logicalExpressionType, array).asArray();
          }
        }
      }
      offsetId = offset.getId();

      return objects;
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
}
