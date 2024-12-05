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
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumn;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public final class CompressedComplexColumn<T> implements ComplexColumn
{
  private final String typeName;
  private final CompressedVariableSizedBlobColumn compressedColumn;
  private final ImmutableBitmap nullValues;
  private final ObjectStrategy<T> objectStrategy;

  public CompressedComplexColumn(
      String typeName,
      CompressedVariableSizedBlobColumn compressedColumn,
      ImmutableBitmap nullValues,
      ObjectStrategy<T> objectStrategy
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
  @Nullable
  public T getRowValue(int rowNum)
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
}
