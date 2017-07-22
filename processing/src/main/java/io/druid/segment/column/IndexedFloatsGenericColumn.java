/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.column;

import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedFloats;
import io.druid.segment.data.IndexedLongs;

/**
*/
public class IndexedFloatsGenericColumn implements GenericColumn
{
  private final IndexedFloats column;

  public IndexedFloatsGenericColumn(final IndexedFloats column)
  {
    this.column = column;
  }

  @Override
  public int length()
  {
    return column.size();
  }

  @Override
  public ValueType getType()
  {
    return ValueType.FLOAT;
  }

  @Override
  public boolean hasMultipleValues()
  {
    return false;
  }

  @Override
  public String getStringSingleValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Indexed<String> getStringMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public IndexedFloats getFloatMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongSingleValueRow(int rowNum)
  {
    return (long) column.get(rowNum);
  }

  @Override
  public IndexedLongs getLongMultiValueRow(int rowNum)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleSingleValueRow(int rowNum)
  {
    return (double) column.get(rowNum);
  }

  @Override
  public void close()
  {
    column.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("column", column);
  }
}
