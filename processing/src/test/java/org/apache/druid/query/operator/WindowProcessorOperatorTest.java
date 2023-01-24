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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

public class WindowProcessorOperatorTest
{
  @Test
  public void testJustRunsTheProcessor()
  {
    RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "colA", new IntArrayColumn(new int[]{1, 2, 3}),
            "colB", new IntArrayColumn(new int[]{3, 2, 1})
        )
    );

    WindowProcessorOperator op = new WindowProcessorOperator(
        new Processor()
        {
          @Override
          public RowsAndColumns process(RowsAndColumns incomingPartition)
          {
            return incomingPartition;
          }

          @Override
          public boolean validateEquivalent(Processor otherProcessor)
          {
            return true;
          }
        },
        InlineScanOperator.make(rac)
    );

    new OperatorTestHelper()
        .withPushFn(
            rowsAndColumns -> {
              Assert.assertSame(rac, rowsAndColumns);
              return Operator.Signal.GO;
            }
        )
        .runToCompletion(op);
  }
}
