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

package org.apache.druid.segment;

import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.HistogramAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class EmptyCursorHolderTest
{
  private static final RowSignature INSPECTOR = RowSignature.builder().add("x", ColumnType.LONG).build();
  private static final VirtualColumns VECTORIZABLE_VIRTUAL_COLUMNS = VirtualColumns.create(
      new ExpressionVirtualColumn("v", "x + 1", ColumnType.LONG, TestExprMacroTable.INSTANCE)
  );
  private static final CursorBuildSpec ASC = CursorBuildSpec.builder()
                                                            .setPreferredOrdering(Cursors.ascendingTimeOrder())
                                                            .build();
  private static final CursorBuildSpec DESC = CursorBuildSpec.builder()
                                                             .setPreferredOrdering(Cursors.descendingTimeOrder())
                                                             .build();

  @Test
  void testForTimeOrderAscendingAdvertisesAscendingTime()
  {
    CursorHolder holder = EmptyCursorHolder.forSpec(ASC, INSPECTOR);
    Assertions.assertEquals(
        Cursors.ascendingTimeOrder(),
        holder.getOrdering()
    );
  }

  @Test
  void testForTimeOrderDescendingAdvertisesDescendingTime()
  {
    CursorHolder holder = EmptyCursorHolder.forSpec(DESC, INSPECTOR);
    Assertions.assertEquals(
        Cursors.descendingTimeOrder(),
        holder.getOrdering()
    );
  }

  @Test
  void testForTimeOrderNoneAdvertisesNoOrdering()
  {
    CursorHolder holder = EmptyCursorHolder.forSpec(CursorBuildSpec.FULL_SCAN, INSPECTOR);
    Assertions.assertEquals(
        List.of(),
        holder.getOrdering()
    );
  }

  @Test
  void testForSpecNonTimeOrderingAdvertisedVerbatim()
  {
    // An empty cursor trivially satisfies any ordering, so a non-__time preferred ordering is advertised verbatim.
    final List<OrderBy> ordering = List.of(OrderBy.ascending("dim"));
    final CursorBuildSpec spec = CursorBuildSpec.builder().setPreferredOrdering(ordering).build();
    final CursorHolder holder = EmptyCursorHolder.forSpec(spec, INSPECTOR);
    Assertions.assertEquals(ordering, holder.getOrdering());
  }

  @Test
  void testCursorIsAlwaysDone()
  {
    Assertions.assertTrue(EmptyCursorHolder.forSpec(ASC, INSPECTOR).asCursor().isDone());
    Assertions.assertTrue(EmptyCursorHolder.forSpec(DESC, INSPECTOR).asCursor().isDone());
    Assertions.assertTrue(EmptyCursorHolder.forSpec(CursorBuildSpec.FULL_SCAN, INSPECTOR).asCursor().isDone());
  }

  @Test
  void testCanVectorizeWithoutVirtualColumnsOrAggregators()
  {
    Assertions.assertTrue(EmptyCursorHolder.forSpec(CursorBuildSpec.FULL_SCAN, INSPECTOR).canVectorize());
    Assertions.assertTrue(EmptyCursorHolder.forSpec(ASC, INSPECTOR).canVectorize());
    Assertions.assertTrue(EmptyCursorHolder.forSpec(DESC, INSPECTOR).canVectorize());
  }

  @Test
  void testCanVectorizeWithVectorizableVirtualColumnsAndAggregators()
  {
    final CursorBuildSpec spec =
        CursorBuildSpec.builder(ASC)
                       .setVirtualColumns(VECTORIZABLE_VIRTUAL_COLUMNS)
                       .setAggregators(List.of(new CountAggregatorFactory("c"), new LongSumAggregatorFactory("s", "v")))
                       .build();
    final CursorHolder holder = EmptyCursorHolder.forSpec(spec, INSPECTOR);
    Assertions.assertTrue(holder.canVectorize());
    Assertions.assertEquals(Cursors.ascendingTimeOrder(), holder.getOrdering());
  }

  @Test
  void testCannotVectorizeWithNonVectorizableAggregator()
  {
    final CursorBuildSpec spec =
        CursorBuildSpec.builder(DESC)
                       .setAggregators(List.of(new HistogramAggregatorFactory("h", "x", List.of(1f))))
                       .build();
    final CursorHolder holder = EmptyCursorHolder.forSpec(spec, INSPECTOR);
    Assertions.assertFalse(holder.canVectorize());
    Assertions.assertEquals(Cursors.descendingTimeOrder(), holder.getOrdering());
    Assertions.assertTrue(holder.asCursor().isDone());
  }

  @Test
  void testCannotVectorizeWhenVirtualColumnVectorizationDisabled()
  {
    final CursorBuildSpec spec =
        CursorBuildSpec.builder()
                       .setVirtualColumns(VECTORIZABLE_VIRTUAL_COLUMNS)
                       .setQueryContext(
                           QueryContext.of(Map.of(QueryContextParameters.VECTORIZE_VIRTUAL_COLUMNS.getName(), "false"))
                       )
                       .build();
    Assertions.assertFalse(EmptyCursorHolder.forSpec(spec, INSPECTOR).canVectorize());
  }
}
