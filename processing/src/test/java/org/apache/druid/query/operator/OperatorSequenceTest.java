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
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.frame.MapOfColumnsRowsAndColumns;
import org.junit.Assert;
import org.junit.Test;

public class OperatorSequenceTest
{
  @Test
  public void testSanity()
  {
    OperatorSequence seq = new OperatorSequence(
        () -> InlineScanOperator.make(MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{1})))
    );

    Assert.assertEquals(1, seq.accumulate(0, (accumulated, in) -> accumulated + 1).intValue());

    Yielder<Integer> yielder = seq.toYielder(0, new YieldingAccumulator<Integer, RowsAndColumns>()
    {
      @Override
      public Integer accumulate(Integer accumulated, RowsAndColumns in)
      {
        yield();
        return accumulated + 1;
      }
    });
    Assert.assertFalse(yielder.isDone());
    Assert.assertEquals(1, yielder.get().intValue());

    yielder = yielder.next(0);
    Assert.assertTrue(yielder.isDone());
  }
}
