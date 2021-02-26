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

package org.apache.druid.segment.column;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;

/**
 * This interface represents a complex column and can be implemented by druid extension writer of a custom column
 * with arbitrary serialization instead of a custom column that serializes rows of objects serialized using
 * {@link org.apache.druid.segment.data.GenericIndexed} class which is default implementation of "writeToXXX" methods in
 * {@link org.apache.druid.segment.serde.ComplexColumnSerializer}. In that case {@link GenericIndexedBasedComplexColumn}
 * should be used.
 */
@ExtensionPoint
public interface ComplexColumn extends BaseColumn
{
  /**
   * @return Class of objects returned on calls to {@link ComplexColumn#getRowValue(int)} .
   */
  Class<?> getClazz();

  /**
   * @return Typename associated with this column.
   */
  String getTypeName();

  /**
   * Return rows in the column.
   * @param rowNum the row number
   * @return row object of type same as {@link ComplexColumn#getClazz()}  } at row number "rowNum" .
   */
  Object getRowValue(int rowNum);

  /**
   * @return serialized size (in bytes) of this column.
   */
  int getLength();

  /**
   * Close and release any resources associated with this column.
   */
  @Override
  void close();

  /**
   * Optionally overridden when complex column serialization is not based on default serialization based
   * on {@link org.apache.druid.segment.data.GenericIndexed} in {@link org.apache.druid.segment.serde.ComplexColumnSerializer}.
   * @param offset object to retrieve row number
   * @return the {@link ColumnValueSelector} object
   */
  @Override
  default ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        return getRowValue(offset.getOffset());
      }

      @Override
      public Class classOfObject()
      {
        return getClazz();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", ComplexColumn.this);
      }
    };
  }

  @Override
  default VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
  {
    return new VectorObjectSelector()
    {
      final Object[] vector = new Object[offset.getMaxVectorSize()];

      private int id = ReadableVectorInspector.NULL_ID;

      @Override
      public Object[] getObjectVector()
      {
        if (id == offset.getId()) {
          return vector;
        }

        if (offset.isContiguous()) {
          final int startOffset = offset.getStartOffset();
          final int vectorSize = offset.getCurrentVectorSize();

          for (int i = 0; i < vectorSize; i++) {
            vector[i] = getRowValue(startOffset + i);
          }
        } else {
          final int[] offsets = offset.getOffsets();
          final int vectorSize = offset.getCurrentVectorSize();

          for (int i = 0; i < vectorSize; i++) {
            vector[i] = getRowValue(offsets[i]);
          }
        }

        id = offset.getId();
        return vector;
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
    };
  }
}
