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

package org.apache.druid.segment.serde;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumn;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class CompressedComplexColumn implements ComplexColumn
{
  private final String typeName;
  private final CompressedVariableSizedBlobColumn compressedColumn;
  private final ImmutableBitmap nullValues;
  private final ObjectStrategy<?> objectStrategy;

  public CompressedComplexColumn(
      String typeName,
      CompressedVariableSizedBlobColumn compressedColumn,
      ImmutableBitmap nullValues,
      ObjectStrategy<?> objectStrategy
  )
  {
    this.typeName = typeName;
    this.compressedColumn = compressedColumn;
    this.nullValues = nullValues;
    this.objectStrategy = objectStrategy;
  }

  @Override
  public Class<?> getClazz()
  {
    return objectStrategy.getClazz();
  }

  @Override
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public Object getRowValue(int rowNum)
  {
    if (nullValues.get(rowNum)) {
      return null;
    }

    final ByteBuffer valueBuffer = compressedColumn.get(rowNum);
    return objectStrategy.fromByteBuffer(valueBuffer, valueBuffer.remaining());
  }

  @Override
  public int getLength()
  {
    return compressedColumn.size();
  }

  @Override
  public void close()
  {
    CloseableUtils.closeAndWrapExceptions(compressedColumn);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        if (nullValues.get(offset.getOffset())) {
          return null;
        }
        final ByteBuffer valueBuffer = compressedColumn.get(offset.getOffset());
        return objectStrategy.fromByteBuffer(valueBuffer, valueBuffer.remaining());
      }

      @Override
      public Class classOfObject()
      {
        return getClazz();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", compressedColumn);
        inspector.visit("nullValues", nullValues);
      }
    };
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
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
            vector[i] = getForOffset(startOffset + i);
          }
        } else {
          final int[] offsets = offset.getOffsets();
          final int vectorSize = offset.getCurrentVectorSize();

          for (int i = 0; i < vectorSize; i++) {
            vector[i] = getForOffset(offsets[i]);

          }
        }

        id = offset.getId();
        return vector;
      }

      @Nullable
      private Object getForOffset(int offset)
      {
        if (nullValues.get(offset)) {
          // maybe someday can use bitmap batch operations for nulls?
          return null;
        }
        final ByteBuffer valueBuffer = compressedColumn.get(offset);
        return objectStrategy.fromByteBuffer(valueBuffer, valueBuffer.remaining());
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
