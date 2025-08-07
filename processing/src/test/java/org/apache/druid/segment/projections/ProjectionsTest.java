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

import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.IsBooleanFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

class ProjectionsTest
{
  @Test
  void testSchemaMatchSimple()
  {
    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.LONG)
                                         .add("b", ColumnType.STRING)
                                         .add("c", ColumnType.LONG)
                                         .build();
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .groupingColumns(new LongDimensionSchema("a"), new StringDimensionSchema("b"))
                               .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                               .build()
                               .toMetadataSchema(),
        12345
    );
    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setPreferredOrdering(List.of())
                                                     .setAggregators(
                                                         List.of(
                                                             new LongSumAggregatorFactory("c", "c")
                                                         )
                                                     )
                                                     .build();

    Projections.ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        new RowSignatureChecker(baseTable)
    );
    Projections.ProjectionMatch expected = new Projections.ProjectionMatch(
        CursorBuildSpec.builder()
                       .setAggregators(List.of(new LongSumAggregatorFactory("c", "c")))
                       .setPhysicalColumns(Set.of("c_sum"))
                       .setPreferredOrdering(List.of())
                       .build(),
        Map.of("c", "c_sum")
    );
    Assertions.assertEquals(expected, projectionMatch);
  }

  @Test
  void testSchemaMatchFilter()
  {
    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.LONG)
                                         .add("b", ColumnType.STRING)
                                         .add("c", ColumnType.LONG)
                                         .build();
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .filter(new EqualityFilter("b", ColumnType.STRING, "foo", null))
                               .groupingColumns(new LongDimensionSchema("a"))
                               .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                               .build()
                               .toMetadataSchema(),
        12345
    );
    CursorBuildSpec cursorBuildSpecNoFilter = CursorBuildSpec.builder()
                                                             .setPreferredOrdering(List.of())
                                                             .setAggregators(
                                                                 List.of(
                                                                     new LongSumAggregatorFactory("c", "c")
                                                                 )
                                                             )
                                                             .build();

    Assertions.assertNull(
        Projections.matchAggregateProjection(
            spec.getSchema(),
            cursorBuildSpecNoFilter,
            new RowSignatureChecker(baseTable)
        )
    );
    CursorBuildSpec cursorBuildSpecWithFilter = CursorBuildSpec.builder()
                                                               .setPreferredOrdering(List.of())
                                                               .setFilter(
                                                                   new EqualityFilter(
                                                                       "b",
                                                                       ColumnType.STRING,
                                                                       "foo",
                                                                       null
                                                                   )
                                                               )
                                                               .setAggregators(
                                                                   List.of(
                                                                       new LongSumAggregatorFactory("c", "c")
                                                                   )
                                                               )
                                                               .build();
    Projections.ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpecWithFilter,
        new RowSignatureChecker(baseTable)
    );
    Projections.ProjectionMatch expected = new Projections.ProjectionMatch(
        CursorBuildSpec.builder()
                       .setAggregators(List.of(new LongSumAggregatorFactory("c", "c")))
                       .setPhysicalColumns(Set.of("c_sum"))
                       .setPreferredOrdering(List.of())
                       .build(),
        Map.of("c", "c_sum")
    );
    Assertions.assertEquals(expected, projectionMatch);
  }

  @Test
  void testSchemaMatchFilterIncludedInProjection()
  {
    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.LONG)
                                         .add("b", ColumnType.STRING)
                                         .add("c", ColumnType.LONG)
                                         .build();
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .filter(new LikeDimFilter("b", "foo%", null, null))
                               .groupingColumns(new LongDimensionSchema("a"), new StringDimensionSchema("b"))
                               .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                               .build()
                               .toMetadataSchema(),
        12345
    );
    CursorBuildSpec cursorBuildSpecNoFilter = CursorBuildSpec.builder()
                                                             .setPreferredOrdering(List.of())
                                                             .setGroupingColumns(List.of("a", "b"))
                                                             .setAggregators(
                                                                 List.of(
                                                                     new LongSumAggregatorFactory("c", "c")
                                                                 )
                                                             )
                                                             .build();

    Assertions.assertNull(
        Projections.matchAggregateProjection(
            spec.getSchema(),
            cursorBuildSpecNoFilter,
            new RowSignatureChecker(baseTable)
        )
    );
    CursorBuildSpec cursorBuildSpecWithFilter = CursorBuildSpec.builder()
                                                               .setGroupingColumns(List.of("a", "b"))
                                                               .setPreferredOrdering(List.of())
                                                               .setFilter(
                                                                   new LikeDimFilter(
                                                                       "b",
                                                                       "foo%",
                                                                       null,
                                                                       null
                                                                   ).toFilter()
                                                               )
                                                               .setAggregators(
                                                                   List.of(
                                                                       new LongSumAggregatorFactory("c", "c")
                                                                   )
                                                               )
                                                               .build();
    Projections.ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpecWithFilter,
        new RowSignatureChecker(baseTable)
    );
    Projections.ProjectionMatch expected = new Projections.ProjectionMatch(
        CursorBuildSpec.builder()
                       .setGroupingColumns(List.of("a", "b"))
                       .setAggregators(List.of(new LongSumAggregatorFactory("c", "c")))
                       .setPhysicalColumns(Set.of("a", "b", "c_sum"))
                       .setPreferredOrdering(List.of())
                       .build(),
        Map.of("c", "c_sum")
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

    queryFilter = new AndFilter(
        List.of(
            new EqualityFilter("a", ColumnType.STRING, "foo", null),
            new EqualityFilter("b", ColumnType.STRING, "bar", null),
            new EqualityFilter("c", ColumnType.STRING, "baz", null)
        )
    );
    Assertions.assertEquals(
        new EqualityFilter("b", ColumnType.STRING, "bar", null),
        Projections.rewriteFilter(
            new AndFilter(
                List.of(
                    new EqualityFilter("a", ColumnType.STRING, "foo", null),
                    new EqualityFilter("c", ColumnType.STRING, "baz", null)
                )
            ),
            queryFilter
        )
    );
  }

  private static class RowSignatureChecker implements Projections.PhysicalColumnChecker
  {
    private final RowSignature rowSignature;

    private RowSignatureChecker(RowSignature rowSignature)
    {
      this.rowSignature = rowSignature;
    }

    @Override
    public boolean check(String projectionName, String columnName)
    {
      return rowSignature.contains(columnName);
    }
  }
}
