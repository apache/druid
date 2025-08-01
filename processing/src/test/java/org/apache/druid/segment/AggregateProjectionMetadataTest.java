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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.IsBooleanFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

class AggregateProjectionMetadataTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  void testSerde() throws JsonProcessingException
  {
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "some_projection",
            "time",
            new EqualityFilter("a", ColumnType.STRING, "a", null),
            VirtualColumns.create(
                Granularities.toVirtualColumn(Granularities.HOUR, "time")
            ),
            Arrays.asList("a", "b", "time", "c", "d"),
            new AggregatorFactory[]{
                new CountAggregatorFactory("count"),
                new LongSumAggregatorFactory("e", "e")
            },
            Arrays.asList(
                OrderBy.ascending("a"),
                OrderBy.ascending("b"),
                OrderBy.ascending("time"),
                OrderBy.ascending("c"),
                OrderBy.ascending("d")
            )
        ),
        12345
    );
    Assertions.assertEquals(
        spec,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), AggregateProjectionMetadata.class)
    );
  }


  @Test
  void testComparator()
  {
    SortedSet<AggregateProjectionMetadata> metadataBest = new ObjectAVLTreeSet<>(AggregateProjectionMetadata.COMPARATOR);
    AggregateProjectionMetadata good = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "good",
            "theTime",
            null,
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("theTime", "a", "b", "c"),
            new AggregatorFactory[]{new CountAggregatorFactory("chocula")},
            Arrays.asList(
                OrderBy.ascending("theTime"),
                OrderBy.ascending("a"),
                OrderBy.ascending("b"),
                OrderBy.ascending("c")
            )
        ),
        123
    );
    // same row count, but less grouping columns aggs more better
    AggregateProjectionMetadata betterLessGroupingColumns = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "betterLessGroupingColumns",
            "theTime",
            null,
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("c", "d", "theTime"),
            new AggregatorFactory[]{new CountAggregatorFactory("chocula")},
            Arrays.asList(
                OrderBy.ascending("c"),
                OrderBy.ascending("d"),
                OrderBy.ascending("theTime")
            )
        ),
        123
    );
    // same grouping columns, but more aggregators
    AggregateProjectionMetadata evenBetterMoreAggs = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "evenBetterMoreAggs",
            "theTime",
            null,
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("c", "d", "theTime"),
            new AggregatorFactory[]{
                new CountAggregatorFactory("chocula"),
                new LongSumAggregatorFactory("e", "e")
            },
            Arrays.asList(
                OrderBy.ascending("c"),
                OrderBy.ascending("d"),
                OrderBy.ascending("theTime")
            )
        ),
        123
    );
    // small rows is best
    AggregateProjectionMetadata best = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "best",
            null,
            null,
            VirtualColumns.EMPTY,
            Arrays.asList("f", "g"),
            new AggregatorFactory[0],
            Arrays.asList(OrderBy.ascending("f"), OrderBy.ascending("g"))
        ),
        10
    );
    metadataBest.add(good);
    metadataBest.add(betterLessGroupingColumns);
    metadataBest.add(evenBetterMoreAggs);
    metadataBest.add(best);
    Assertions.assertEquals(best, metadataBest.first());
    Assertions.assertArrayEquals(
        new AggregateProjectionMetadata[]{best, evenBetterMoreAggs, betterLessGroupingColumns, good},
        metadataBest.toArray()
    );
  }

  @Test
  void testInvalidName()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                null,
                null,
                null,
                null,
                null,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("count"))
            ),
            0
        )
    );
    Assertions.assertEquals(
        "projection schema name cannot be null or empty",
        t.getMessage()
    );

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "",
                null,
                null,
                null,
                null,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("count"))
            ),
            0
        )
    );
    Assertions.assertEquals(
        "projection schema name cannot be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testInvalidGrouping()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "other_projection",
                null,
                null,
                null,
                null,
                null,
                null
            ),
            0
        )
    );
    Assertions.assertEquals(
        "projection schema[other_projection] groupingColumns and aggregators must not both be null or empty",
        t.getMessage()
    );

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "other_projection",
                null,
                null,
                null,
                Collections.emptyList(),
                null,
                null
            ),
            0
        )
    );
    Assertions.assertEquals(
        "projection schema[other_projection] groupingColumns and aggregators must not both be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testInvalidOrdering()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "no order",
                null,
                null,
                null,
                null,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                null
            ),
            0
        )
    );
    Assertions.assertEquals(
        "projection schema[no order] ordering must not be null",
        t.getMessage()
    );

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "",
                null,
                null,
                null,
                null,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("count"))
            ),
            0
        )
    );
    Assertions.assertEquals(
        "projection schema name cannot be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(AggregateProjectionMetadata.class).usingGetClass().verify();
  }

  @Test
  void testEqualsAndHashcodeSchema()
  {
    EqualsVerifier.forClass(AggregateProjectionMetadata.Schema.class)
                  .withIgnoredFields("orderingWithTimeSubstitution", "timeColumnPosition", "effectiveGranularity")
                  .usingGetClass()
                  .verify();
  }

  @Test
  void testSchemaMatchSimple()
  {
    // arrange
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
                                                     .setAggregators(ImmutableList.of(new LongSumAggregatorFactory(
                                                         "a",
                                                         "a"
                                                     )))
                                                     .build();
    // act & assert
    Projections.ProjectionMatch projectionMatch = spec.getSchema()
                                                      .matches(cursorBuildSpec, (projectionName, columnName) -> true);
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
        AggregateProjectionMetadata.ProjectionFilterMatch.class,
        AggregateProjectionMetadata.rewriteFilter(xeqfoo, queryFilter)
    );

    queryFilter = yeqbar;
    Assertions.assertNull(AggregateProjectionMetadata.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(xeqfoo, yeqbar));
    Assertions.assertEquals(
        yeqbar,
        AggregateProjectionMetadata.rewriteFilter(xeqfoo, queryFilter)
    );

    queryFilter = new AndFilter(List.of(new OrFilter(List.of(xeqfoo, xeqbar)), yeqbar));
    Assertions.assertNull(AggregateProjectionMetadata.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new IsBooleanFilter(xeqfoo, true), yeqbar));
    Assertions.assertEquals(yeqbar, AggregateProjectionMetadata.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new IsBooleanFilter(xeqfoo, false), yeqbar));
    Assertions.assertNull(AggregateProjectionMetadata.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new AndFilter(List.of(xeqfoo, yeqbar)), zeq123));
    Assertions.assertEquals(
        new AndFilter(List.of(yeqbar, zeq123)),
        AggregateProjectionMetadata.rewriteFilter(xeqfoo, queryFilter)
    );
  }
}
