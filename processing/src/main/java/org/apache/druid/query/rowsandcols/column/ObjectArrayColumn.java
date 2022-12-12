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

import org.apache.druid.segment.column.ColumnType;

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

  @Override
  public ColumnAccessor toAccessor()
  {
    return new ObjectColumnAccessorBase()
    {
      @Override
      protected Object getVal(int cell)
      {
        return objects[cell];
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
    };
  }

  @Override
  public <T> T as(Class<? extends T> clazz)
  {
    return null;
  }

}
