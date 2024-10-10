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

public class GlueingPartitioningOperatorTest
{
  @Test
  public void testPartitioning()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1, 1, 2, 2, 1)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column")
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            expectedSimpleRac(1, 1, 1),
            expectedSimpleRac(2, 2),
            expectedSimpleRac(1)
        )
        .runToCompletion(op);
  }

  @Test
  public void testPartitioningWithMultipleRACs()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1, 1, 2, 2, 1),
        makeSimpleRac(1, 1, 1, 2, 2, 1),
        makeSimpleRac(1, 1, 2, 2, 1)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column")
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            expectedSimpleRac(1, 1, 1),
            expectedSimpleRac(2, 2),
            expectedSimpleRac(1, 1, 1, 1),
            expectedSimpleRac(2, 2),
            expectedSimpleRac(1, 1, 1),
            expectedSimpleRac(2, 2),
            expectedSimpleRac(1)
        )
        .runToCompletion(op);
  }

  @Test
  public void testPartitioningWithMultipleConcatenationBetweenRACs()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1),
        makeSimpleRac(1, 1),
        makeSimpleRac(1, 2)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column")
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            expectedSimpleRac(1, 1, 1, 1, 1),
            expectedSimpleRac(2)
        )
        .runToCompletion(op);
  }

  @Test
  public void testPartitioningWithNoGlueing()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 2, 3),
        makeSimpleRac(4, 5, 6),
        makeSimpleRac(7, 8, 9)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column")
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            expectedSimpleRac(1),
            expectedSimpleRac(2),
            expectedSimpleRac(3),
            expectedSimpleRac(4),
            expectedSimpleRac(5),
            expectedSimpleRac(6),
            expectedSimpleRac(7),
            expectedSimpleRac(8),
            expectedSimpleRac(9)
        )
        .runToCompletion(op);
  }

  @Test
  public void testPartitioningWithNoPartitionColumns()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1, 1, 2, 2, 1),
        makeSimpleRac(1, 1, 1, 2, 2, 1)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        Collections.emptyList()
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            expectedSimpleRac(1, 1, 1, 2, 2, 1, 1, 1, 1, 2, 2, 1)
        )
        .runToCompletion(op);
  }

  @Test
  public void testMaxRowsConstraintViolation()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1, 1)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column"),
        2
    );

    Assert.assertThrows(
        "Too many rows to process (requested = 3, max = 2).",
        DruidException.class,
        () -> new OperatorTestHelper().expectRowsAndColumns().runToCompletion(op)
    );
  }

  @Test
  public void testMaxRowsConstraintViolationWhenGlueing()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1, 1),
        makeSimpleRac(1, 2, 3)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column"),
        3
    );

    Assert.assertThrows(
        "Too many rows to process (requested = 4, max = 3).",
        DruidException.class,
        () -> new OperatorTestHelper().expectRowsAndColumns().runToCompletion(op)
    );
  }

  @Test
  public void testMaxRowsConstraintWhenGlueing()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1, 1),
        makeSimpleRac(2, 2, 2)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column"),
        3
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            expectedSimpleRac(1, 1, 1),
            expectedSimpleRac(2, 2, 2)
        )
        .runToCompletion(op);
  }

  @Test
  public void testStopMidStream()
  {
    InlineScanOperator inlineScanOperator = InlineScanOperator.make(
        makeSimpleRac(1, 1, 1, 2, 2, 1)
    );

    GlueingPartitioningOperator op = new GlueingPartitioningOperator(
        inlineScanOperator,
        ImmutableList.of("column")
    );

    new OperatorTestHelper()
        .expectAndStopAfter(
            expectedSimpleRac(1, 1, 1),
            expectedSimpleRac(2, 2)
        )
        .runToCompletion(op);
  }

  private RowsAndColumns makeSimpleRac(int... values)
  {
    return MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of("column", new IntArrayColumn(values))
    );
  }

  private RowsAndColumnsHelper expectedSimpleRac(int... values)
  {
    return new RowsAndColumnsHelper()
        .expectColumn("column", values)
        .allColumnsRegistered();
  }
}
