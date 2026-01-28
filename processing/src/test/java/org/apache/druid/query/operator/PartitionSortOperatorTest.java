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

public class PartitionSortOperatorTest
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

    PartitionSortOperator op = new PartitionSortOperator(
        inlineScanOperator,
        ImmutableList.of(new ColumnWithDirection("unsorted", ColumnWithDirection.Direction.ASC))
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("sorted", new int[]{1, 1, 1, 1, 2, 2})
                .expectColumn("unsorted", new int[]{10, 10, 10, 11, 20, 20})
                .allColumnsRegistered()
        )
        .runToCompletion(op);
  }
}
