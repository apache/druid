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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.roaringbitmap.PeekableIntIterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class VariantArrayColumn<TStringDictionary extends Indexed<ByteBuffer>> implements NestedCommonFormatColumn
{
  private final TStringDictionary stringDictionary;
  private final FixedIndexed<Long> longDictionary;
  private final FixedIndexed<Double> doubleDictionary;
  private final FrontCodedIntArrayIndexed arrayDictionary;
  private final ColumnarInts encodedValueColumn;
  private final ImmutableBitmap nullValueBitmap;
  private final ColumnType logicalType;
  private final int adjustLongId;
  private final int adjustDoubleId;
  private final int adjustArrayId;

  public VariantArrayColumn(
      TStringDictionary stringDictionary,
      FixedIndexed<Long> longDictionary,
      FixedIndexed<Double> doubleDictionary,
      FrontCodedIntArrayIndexed arrayDictionary,
      ColumnarInts encodedValueColumn,
      ImmutableBitmap nullValueBitmap,
      ColumnType logicalType
  )
  {
    this.stringDictionary = stringDictionary;
    this.longDictionary = longDictionary;
    this.doubleDictionary = doubleDictionary;
    this.arrayDictionary = arrayDictionary;
    this.encodedValueColumn = encodedValueColumn;
    this.nullValueBitmap = nullValueBitmap;
    this.logicalType = logicalType;
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
    Iterable<Object[]> arrays = () -> {

      return new Iterator<Object[]>()
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
        private Object lookupId(int globalId)
        {
          if (globalId == 0) {
            return null;
          }
          final int adjustLongId = stringDictionary.size();
          final int adjustDoubleId = stringDictionary.size() + longDictionary.size();
          if (globalId < adjustLongId) {
            return StringUtils.fromUtf8Nullable(stringDictionary.get(globalId));
          } else if (globalId < adjustDoubleId) {
            return longDictionary.get(globalId - adjustLongId);
          } else if (globalId < adjustDoubleId + doubleDictionary.size()) {
            return doubleDictionary.get(globalId - adjustDoubleId);
          }
          throw new IAE("Unknown globalId [%s]", globalId);
        }
      };
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
        } else {
          return doubleDictionary.get(id - adjustDoubleId).floatValue();
        }
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
        } else {
          return doubleDictionary.get(id - adjustDoubleId);
        }
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
        } else {
          return doubleDictionary.get(id - adjustDoubleId).longValue();
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
        return DimensionHandlerUtils.isNumericNull(getObject());
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
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    return new VectorObjectSelector()
    {
      private final int[] vector = new int[offset.getMaxVectorSize()];
      private final Object[] objects = new Object[offset.getMaxVectorSize()];
      private int id = ReadableVectorInspector.NULL_ID;

      @Override

      public Object[] getObjectVector()
      {
        if (id == offset.getId()) {
          return objects;
        }

        if (offset.isContiguous()) {
          encodedValueColumn.get(vector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          encodedValueColumn.get(vector, offset.getOffsets(), offset.getCurrentVectorSize());
        }
        for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
          final int globalId = vector[i];
          if (globalId < adjustArrayId) {
            objects[i] = lookupScalarValue(globalId);
          } else {
            int[] arr = arrayDictionary.get(globalId - adjustArrayId);
            if (arr == null) {
              objects[i] = null;
            } else {
              final Object[] array = new Object[arr.length];
              for (int j = 0; j < arr.length; j++) {
                array[j] = lookupScalarValue(arr[j]);
              }
              objects[i] = array;
            }
          }
        }
        id = offset.getId();

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
    };
  }

  @Override
  public void close() throws IOException
  {
    encodedValueColumn.close();
  }

  private Object lookupScalarValue(int globalId)
  {
    if (globalId < adjustLongId) {
      return StringUtils.fromUtf8Nullable(stringDictionary.get(globalId));
    } else if (globalId < adjustDoubleId) {
      return longDictionary.get(globalId - adjustLongId);
    } else if (globalId < adjustArrayId) {
      return doubleDictionary.get(globalId - adjustDoubleId);
    }
    throw new IllegalArgumentException("not a scalar in the dictionary");
  }
}
