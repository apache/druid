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

package org.apache.druid.segment.projections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.IsBooleanFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class ProjectionsTest
{
  @Test
  void testSchemaMatchSimple()
  {
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "some_projection",
            null,
            null,
            VirtualColumns.EMPTY,
            Arrays.asList("a", "b"),
            new AggregatorFactory[]{new LongSumAggregatorFactory("a_projection", "a")},
            Arrays.asList(OrderBy.ascending("a"), OrderBy.ascending("b"))
        ),
        12345
    );
    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setPreferredOrdering(ImmutableList.of())
                                                     .setAggregators(
                                                         List.of(
                                                             new LongSumAggregatorFactory("a", "a")
                                                         )
                                                     )
                                                     .build();

    Projections.ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        (projectionName, columnName) -> true
    );
    Projections.ProjectionMatch expected = new Projections.ProjectionMatch(
        CursorBuildSpec.builder()
                       .setAggregators(ImmutableList.of(new LongSumAggregatorFactory("a", "a")))
                       .setPhysicalColumns(ImmutableSet.of("a_projection"))
                       .setPreferredOrdering(ImmutableList.of())
                       .build(),
        ImmutableMap.of("a", "a_projection")
    );
    Assertions.assertEquals(expected, projectionMatch);
  }

  @Test
  void testRewriteFilter()
  {
    Filter xeqfoo = new EqualityFilter("x", ColumnType.STRING, "foo", null);
    Filter xeqfoo2 = new EqualityFilter("x", ColumnType.STRING, "foo", null);
    Filter xeqbar = new EqualityFilter("x", ColumnType.STRING, "bar", null);
    Filter yeqbar = new EqualityFilter("y", ColumnType.STRING, "bar", null);
    Filter zeq123 = new EqualityFilter("z", ColumnType.LONG, 123L, null);

    Filter queryFilter = xeqfoo2;
    Assertions.assertInstanceOf(
        Projections.ProjectionFilterMatch.class,
        Projections.rewriteFilter(xeqfoo, queryFilter)
    );

    queryFilter = yeqbar;
    Assertions.assertNull(Projections.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(xeqfoo, yeqbar));
    Assertions.assertEquals(
        yeqbar,
        Projections.rewriteFilter(xeqfoo, queryFilter)
    );

    queryFilter = new AndFilter(List.of(new OrFilter(List.of(xeqfoo, xeqbar)), yeqbar));
    Assertions.assertNull(Projections.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new IsBooleanFilter(xeqfoo, true), yeqbar));
    Assertions.assertEquals(yeqbar, Projections.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new IsBooleanFilter(xeqfoo, false), yeqbar));
    Assertions.assertNull(Projections.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new AndFilter(List.of(xeqfoo, yeqbar)), zeq123));
    Assertions.assertEquals(
        new AndFilter(List.of(yeqbar, zeq123)),
        Projections.rewriteFilter(xeqfoo, queryFilter)
    );
  }
}