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
import org.apache.druid.error.DruidException;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.function.BiFunction;

public class GlueingPartitioningOperatorTest
{
  @Test
  public void testDefaultImplementation()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10, 20, 20, 11})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("sorted")
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1})
                .expectColumn("unsorted", new int[]{10, 10, 10})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2, 2})
                .expectColumn("unsorted", new int[]{20, 20})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1})
                .expectColumn("unsorted", new int[]{11})
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }

  @Test
  public void testDefaultImplementationWithMultipleRACs()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10, 20, 20, 11})
        )
    );
    RowsAndColumns rac2 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{50, 51, 52, 53, 54, 55})
        )
    );
    RowsAndColumns rac3 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{70, 71, 72, 73, 74})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1, rac2, rac3);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("sorted")
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1})
                .expectColumn("unsorted", new int[]{10, 10, 10})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2, 2})
                .expectColumn("unsorted", new int[]{20, 20})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1, 1})
                .expectColumn("unsorted", new int[]{11, 50, 51, 52})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2, 2})
                .expectColumn("unsorted", new int[]{53, 54})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1})
                .expectColumn("unsorted", new int[]{55, 70, 71})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2, 2})
                .expectColumn("unsorted", new int[]{72, 73})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1})
                .expectColumn("unsorted", new int[]{74})
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }

  @Test
  public void testDefaultImplementationWithMultipleConcatenationBetweenRACs()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10})
        )
    );
    RowsAndColumns rac2 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1}),
            "unsorted", new IntArrayColumn(new int[]{20, 20})
        )
    );
    RowsAndColumns rac3 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 2}),
            "unsorted", new IntArrayColumn(new int[]{30, 40})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1, rac2, rac3);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("sorted")
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1, 1, 1})
                .expectColumn("unsorted", new int[]{10, 10, 20, 20, 30})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2})
                .expectColumn("unsorted", new int[]{40})
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }

  @Test
  public void testDefaultImplementationWithNoPartitionColumns()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10, 20, 20, 11})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        Collections.emptyList()
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1, 2, 2, 1})
                .expectColumn("unsorted", new int[]{10, 10, 10, 20, 20, 11})
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }

  @Test
  public void testDefaultImplementationWithMultipleRACsAndNoPartitionColumns()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10, 20, 20, 11})
        )
    );
    RowsAndColumns rac2 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{50, 51, 52, 53, 54, 55})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1, rac2);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        Collections.emptyList()
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1, 2, 2, 1, 1, 1, 1, 2, 2, 1})
                .expectColumn("unsorted", new int[]{10, 10, 10, 20, 20, 11, 50, 51, 52, 53, 54, 55})
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }

  @Test
  public void testMaxRowsConstraintViolation()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("sorted"),
        2
    );

    boolean exceptionThrown = false;
    try {
      new OperatorTestHelper()
          .expectRowsAndColumns()
          .runToCompletion(op);
    }
    catch (DruidException e) {
      Assert.assertEquals(
          e.getMessage(),
          "Too many rows to process (requested = 3, max = 2)."
      );
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void testMaxRowsConstraintViolationWhenGlueing()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10})
        )
    );
    RowsAndColumns rac2 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 2, 3}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1, rac2);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("sorted"),
        3
    );

    boolean exceptionThrown = false;
    try {
      new OperatorTestHelper()
          .expectRowsAndColumns()
          .runToCompletion(op);
    }
    catch (DruidException e) {
      Assert.assertEquals(
          e.getMessage(),
          "Too many rows to process (requested = 4, max = 3)."
      );
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void testMaxRowsConstraintWhenGlueing()
  {
    RowsAndColumns rac1 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10})
        )
    );
    RowsAndColumns rac2 = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{2, 2, 2}),
            "unsorted", new IntArrayColumn(new int[]{20, 20, 20})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac1, rac2);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("sorted"),
        3
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1})
                .expectColumn("unsorted", new int[]{10, 10, 10})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2, 2, 2})
                .expectColumn("unsorted", new int[]{20, 20, 20})
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }

  @Test
  public void testStopMidStream()
  {
    RowsAndColumns rac = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "sorted", new IntArrayColumn(new int[]{1, 1, 1, 2, 2, 1}),
            "unsorted", new IntArrayColumn(new int[]{10, 10, 10, 20, 20, 11})
        )
    );

    InlineScanOperator inlineScanOperator = InlineScanOperator.make(rac);

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("sorted")
    );

    new OperatorTestHelper()
        .expectAndStopAfter(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1})
                .expectColumn("unsorted", new int[]{10, 10, 10})
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{2, 2})
                .expectColumn("unsorted", new int[]{20, 20})
                .allColumnsRegistered()
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

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        InlineScanOperator.make(rac),
        ImmutableList.of("unsorted")
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
