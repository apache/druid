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

package org.apache.druid.query.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.operator.Operator.Signal;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Test;

public class NaiveSortOperatorTest
{
  @Test
  public void testNoInputisHandledCorrectly()
  {
    NaiveSortOperator op = new NaiveSortOperator(
        InlineScanOperator.make(),
        ImmutableList.of(ColumnWithDirection.ascending("someColumn"))
    );

    new OperatorTestHelper()
        .withPushFn(() -> (someRac) -> Signal.GO)
        .runToCompletion(op);
  }

  @Test
  public void testSortAscending()
  {
    RowsAndColumns rac1 = racForColumn("c", new int[] {5, 3, 1});
    RowsAndColumns rac2 = racForColumn("c", new int[] {2, 6, 4});

    NaiveSortOperator op = new NaiveSortOperator(
        InlineScanOperator.make(rac1, rac2),
        ImmutableList.of(ColumnWithDirection.ascending("c"))
    );

    new OperatorTestHelper()
        .expectAndStopAfter(
            new RowsAndColumnsHelper()
                .expectColumn("c", new int[] {1, 2, 3, 4, 5, 6})
        )
        .runToCompletion(op);
  }

  @Test
  public void testSortDescending()
  {
    RowsAndColumns rac1 = racForColumn("c", new int[] {5, 3, 1});
    RowsAndColumns rac2 = racForColumn("c", new int[] {2, 6, 4});

    NaiveSortOperator op = new NaiveSortOperator(
        InlineScanOperator.make(rac1, rac2),
        ImmutableList.of(ColumnWithDirection.descending("c"))
    );

    new OperatorTestHelper()
        .expectAndStopAfter(
            new RowsAndColumnsHelper()
                .expectColumn("c", new int[] {6, 5, 4, 3, 2, 1})
        )
        .runToCompletion(op);
  }

  private MapOfColumnsRowsAndColumns racForColumn(String k1, Object arr)
  {
    if (int.class.equals(arr.getClass().getComponentType())) {
      return racForColumn(k1, new IntArrayColumn((int[]) arr));
    }
    throw new IllegalArgumentException("Not yet supported");
  }

  private MapOfColumnsRowsAndColumns racForColumn(String k1, Column v1)
  {
    return MapOfColumnsRowsAndColumns.fromMap(ImmutableMap.of(k1, v1));
  }

}
