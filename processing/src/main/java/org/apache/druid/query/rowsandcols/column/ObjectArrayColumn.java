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

package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.query.rowsandcols.util.FindResult;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;

public class ObjectArrayColumn implements Column
{
  private final Object[] objects;
  private final ColumnType resultType;
  private final Comparator<Object> comparator;

  public ObjectArrayColumn(Object[] objects, ColumnType resultType)
  {
    this(objects, resultType, Comparator.nullsFirst(resultType.getStrategy()));
  }

  public ObjectArrayColumn(Object[] objects, ColumnType resultType, Comparator<Object> comparator)
  {
    this.objects = objects;
    this.resultType = resultType;
    this.comparator = comparator;
  }

  /**
   * Gets the underlying object array.  This method is exposed on the concrete class explicitly to allow for
   * mutation.  This class does absolutely nothing to ensure that said mutation of the array is valid.  It is up
   * to the caller that is choosing to do this mutation to make sure that it is safe.
   *
   * @return the object array backing this column
   */
  public Object[] getObjects()
  {
    return objects;
  }

  @Nonnull
  @Override
  public ColumnAccessor toAccessor()
  {
    return new MyColumnAccessor();
  }

  @Nullable
  @SuppressWarnings("unchecked")
  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    if (VectorCopier.class.equals(clazz)) {
      return (T) (VectorCopier) (into, intoStart) -> System.arraycopy(objects, 0, into, intoStart, objects.length);
    }
    if (ColumnValueSwapper.class.equals(clazz)) {
      return (T) (ColumnValueSwapper) (lhs, rhs) -> {
        Object tmp = objects[lhs];
        objects[lhs] = objects[rhs];
        objects[rhs] = tmp;
      };
    }
    return null;
  }

  private class MyColumnAccessor extends ObjectColumnAccessorBase implements BinarySearchableAccessor
  {
    @Override
    protected Object getVal(int rowNum)
    {
      return objects[rowNum];
    }

    @Override
    protected Comparator<Object> getComparator()
    {
      return comparator;
    }

    @Override
    public ColumnType getType()
    {
      return resultType;
    }

    @Override
    public int numRows()
    {
      return objects.length;
    }

    @Override
    public FindResult findNull(int startIndex, int endIndex)
    {
      return findComplex(startIndex, endIndex, null);
    }

    @Override
    public FindResult findDouble(int startIndex, int endIndex, double val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findFloat(int startIndex, int endIndex, float val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findLong(int startIndex, int endIndex, long val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findString(int startIndex, int endIndex, String val)
    {
      return findComplex(startIndex, endIndex, val);
    }

    @Override
    public FindResult findComplex(int startIndex, int endIndex, Object val)
    {
      if (comparator.compare(objects[startIndex], val) == 0) {
        int end = startIndex + 1;

        while (end < endIndex && comparator.compare(objects[end], val) == 0) {
          ++end;
        }
        return FindResult.found(startIndex, end);
      }

      int i = Arrays.binarySearch(objects, startIndex, endIndex, val, comparator);
      if (i > 0) {
        int foundStart = i;
        int foundEnd = i + 1;

        while (foundStart - 1 >= startIndex && comparator.compare(objects[foundStart - 1], val) == 0) {
          --foundStart;
        }

        while (foundEnd < endIndex && comparator.compare(objects[foundEnd], val) == 0) {
          ++foundEnd;
        }

        return FindResult.found(foundStart, foundEnd);
      } else {
        return FindResult.notFound(-(i + 1));
      }
    }
  }
}
