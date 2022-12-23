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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

public class NaivePartitioningOperatorTest
{
  @Test
  public void testDefaultImplementation()
  {
    RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    );

    NaivePartitioningOperator op = new NaivePartitioningOperator(
        ImmutableList.of("sorted"),
        InlineScanOperator.make(rac)
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{0, 0, 0})
                .expectColumn("unsorted", new int[]{3, 54, 21}),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1})
                .expectColumn("unsorted", new int[]{1, 5}),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2})
                .expectColumn("unsorted", new int[]{54}),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{4, 4, 4})
                .expectColumn("unsorted", new int[]{2, 3, 92})
        )
        .runToCompletion(op);
  }

  @Test
  public void testStopMidStream()
  {
    RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    );

    NaivePartitioningOperator op = new NaivePartitioningOperator(
        ImmutableList.of("sorted"),
        InlineScanOperator.make(rac)
    );

    new OperatorTestHelper()
        .expectAndStopAfter(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{0, 0, 0})
                .expectColumn("unsorted", new int[]{3, 54, 21}),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1})
                .expectColumn("unsorted", new int[]{1, 5})
        )
        .runToCompletion(op);
  }

  @Test
  public void testFailUnsorted()
  {
    RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4}),
            "unsorted", new IntArrayColumn(new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
        )
    );

    NaivePartitioningOperator op = new NaivePartitioningOperator(
        ImmutableList.of("unsorted"),
        InlineScanOperator.make(rac)
    );


    boolean exceptionThrown = false;
    try {
      new OperatorTestHelper()
          .withPushFn(
              rac1 -> {
                Assert.fail("I shouldn't be called, an exception should've been thrown.");
                return true;
              }
          )
          .runToCompletion(op);
    }
    catch (ISE ex) {
      Assert.assertEquals("Pre-sorted data required, rows[1] and [2] were not in order", ex.getMessage());
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }
}
