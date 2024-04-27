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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.junit.Test;

import java.util.AbstractList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class RearrangedRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public RearrangedRowsAndColumnsTest()
  {
    super(RearrangedRowsAndColumns.class);
  }

  public static Function<MapOfColumnsRowsAndColumns, RearrangedRowsAndColumns> MAKER = input -> {
    int[] pointers = new int[input.numRows()];
    for (int i = 0; i < pointers.length; ++i) {
      pointers[i] = i;
    }
    shuffleArray(pointers);

    Map<String, Column> shuffledMap = new LinkedHashMap<>();
    for (String columnName : input.getColumnNames()) {
      final ColumnAccessor accessor = input.findColumn(columnName).toAccessor();
      Object[] theVals = new Object[pointers.length];

      for (int i = 0; i < pointers.length; ++i) {
        theVals[pointers[i]] = accessor.getObject(i);
      }

      shuffledMap.put(columnName, new ObjectArrayColumn(theVals, accessor.getType()));
    }

    return new RearrangedRowsAndColumns(pointers, new MapOfColumnsRowsAndColumns(shuffledMap, pointers.length));
  };

  @SuppressWarnings("UnusedCollectionModifiedInPlace")
  private static void shuffleArray(int[] pointers)
  {
    Collections.shuffle(new AbstractList<Object>()
    {
      @Override
      public Object get(int index)
      {
        return pointers[index];
      }

      @Override
      public Object set(int index, Object element)
      {
        int retVal = pointers[index];
        pointers[index] = (Integer) element;
        return retVal;
      }

      @Override
      public int size()
      {
        return pointers.length;
      }
    });
  }

  @Test
  public void testSanity()
  {
    new RowsAndColumnsHelper()
        .expectColumn("int", new int[]{0, 1, 2, 3, 4})
        .allColumnsRegistered()
        .validate(
            new RearrangedRowsAndColumns(
                new int[]{4, 2, 0, 3, 1},
                MapOfColumnsRowsAndColumns.of(
                    "int",
                    new IntArrayColumn(new int[]{2, 4, 1, 3, 0})
                )
            ));
  }
}
