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
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Test;

import java.util.function.BiFunction;

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
                .expectColumn("unsorted", new int[]{3, 54, 21})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1})
                .expectColumn("unsorted", new int[]{1, 5})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2})
                .expectColumn("unsorted", new int[]{54})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{4, 4, 4})
                .expectColumn("unsorted", new int[]{2, 3, 92})
                .allColumnsRegistered()
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
  public void testDoesNotValidateSort()
  {
    BiFunction<Integer, Integer, RowsAndColumnsHelper> singleHelperMaker =
        (sorted, unsorted) ->
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{sorted})
                .expectColumn("unsorted", new int[]{unsorted})
                .allColumnsRegistered();

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

    new OperatorTestHelper()
        .expectRowsAndColumns(
            singleHelperMaker.apply(0, 3),
            singleHelperMaker.apply(0, 54),
            singleHelperMaker.apply(0, 21),
            singleHelperMaker.apply(1, 1),
            singleHelperMaker.apply(1, 5),
            singleHelperMaker.apply(2, 54),
            singleHelperMaker.apply(4, 2),
            singleHelperMaker.apply(4, 3),
            singleHelperMaker.apply(4, 92)
        )
        .runToCompletion(op);
  }
}
