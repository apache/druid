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

import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("ConstantConditions")
public class OperatorSequenceTest
{
  @Test
  public void testAccumulateAndYielderJustOne()
  {
    OperatorSequence seq = new OperatorSequence(
        () -> InlineScanOperator.make(MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{1})))
    );

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper()
        .expectColumn("hi", new int[]{1})
        .allColumnsRegistered();

    Assert.assertEquals(
        1,
        seq.accumulate(
            0,
            (accumulated, in) -> {
              helper.validate(in);
              return accumulated + 1;
            }
        ).intValue()
    );

    Yielder<Integer> yielder = seq.toYielder(0, new YieldingAccumulator<Integer, RowsAndColumns>()
    {
      @Override
      public Integer accumulate(Integer accumulated, RowsAndColumns in)
      {
        this.yield();
        helper.validate(in);
        return accumulated + 1;
      }
    });

    Assert.assertFalse(yielder.isDone());
    Assert.assertEquals(1, yielder.get().intValue());

    yielder = yielder.next(0);
    Assert.assertTrue(yielder.isDone());
  }

  @Test
  public void testAccumulateAndYielderMultiple()
  {
    OperatorSequence seq = new OperatorSequence(
        () -> InlineScanOperator.make(
            MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{1})),
            MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{2})),
            MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{3, 4})),
            MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{5, 6, 7, 8})),
            MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{9, 10, 11})),
            MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{12, 13, 14, 15}))
        )
    );

    Assert.assertEquals(
        120,
        seq.accumulate(
            0,
            (accumulated, in) -> {
              final ColumnAccessor col = in.findColumn("hi").toAccessor();
              for (int i = 0; i < col.numRows(); ++i) {
                accumulated += col.getInt(i);
              }
              return accumulated;
            }
        ).intValue()
    );

    // Never yield
    Yielder<Integer> yielder = seq.toYielder(0, new YieldingAccumulator<Integer, RowsAndColumns>()
    {
      @Override
      public Integer accumulate(Integer accumulated, RowsAndColumns in)
      {
        final ColumnAccessor col = in.findColumn("hi").toAccessor();
        for (int i = 0; i < col.numRows(); ++i) {
          accumulated += col.getInt(i);
        }
        return accumulated;
      }
    });

    Assert.assertEquals(120, yielder.get().intValue());
    Assert.assertTrue(yielder.isDone());

    // Yield at the very end...
    yielder = seq.toYielder(0, new YieldingAccumulator<Integer, RowsAndColumns>()
    {
      @Override
      public Integer accumulate(Integer accumulated, RowsAndColumns in)
      {
        final ColumnAccessor col = in.findColumn("hi").toAccessor();
        for (int i = 0; i < col.numRows(); ++i) {
          accumulated += col.getInt(i);
        }
        if (accumulated == 120) {
          this.yield();
        }
        return accumulated;
      }
    });

    Assert.assertEquals(120, yielder.get().intValue());
    Assert.assertFalse(yielder.isDone());

    yielder = yielder.next(0);
    Assert.assertTrue(yielder.isDone());

    // Aggregate each RAC and yield.
    yielder = seq.toYielder(0, new YieldingAccumulator<Integer, RowsAndColumns>()
    {
      @Override
      public Integer accumulate(Integer accumulated, RowsAndColumns in)
      {
        this.yield();
        final ColumnAccessor col = in.findColumn("hi").toAccessor();
        for (int i = 0; i < col.numRows(); ++i) {
          accumulated += col.getInt(i);
        }
        return accumulated;
      }
    });

    int[] expectedTotals = new int[]{1, 2, 7, 26, 30, 54};

    for (int expectedTotal : expectedTotals) {
      Assert.assertEquals(expectedTotal, yielder.get().intValue());
      Assert.assertFalse(yielder.isDone());
      yielder = yielder.next(0);
    }
    Assert.assertTrue(yielder.isDone());
  }
}
