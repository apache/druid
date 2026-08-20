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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.DateTime;
import org.joda.time.Interval;
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
                                                     .setPhysicalColumns(Set.of("c"))
                                                     .setPreferredOrdering(List.of())
                                                     .setAggregators(
                                                         List.of(
                                                             new LongSumAggregatorFactory("c", "c")
                                                         )
                                                     )
                                                     .build();

    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        Intervals.ETERNITY,
        new RowSignatureChecker(baseTable)
    );
    ProjectionMatch expected = new ProjectionMatch(
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
  void testSchemaMatchDifferentTimeZone_hourlyMatches()
  {
    VirtualColumn ptHourlyFloor = new ExpressionVirtualColumn(
        "__ptHourly",
        "timestamp_floor(__time, 'PT1H', null, 'America/Los_Angeles')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    VirtualColumn hourlyFloor = new ExpressionVirtualColumn(
        "__hourly",
        "timestamp_floor(__time, 'PT1H', null, null)",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.LONG)
                                         .add("b", ColumnType.STRING)
                                         .add("c", ColumnType.LONG)
                                         .build();
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .virtualColumns(hourlyFloor)
                               .groupingColumns(new LongDimensionSchema("__hourly"), new LongDimensionSchema("a"))
                               .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                               .build()
                               .toMetadataSchema(),
        12345
    );
    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(VirtualColumns.create(ptHourlyFloor))
                                                     .setPhysicalColumns(Set.of("__time", "c"))
                                                     .setPreferredOrdering(List.of())
                                                     .setAggregators(List.of(new LongSumAggregatorFactory("c", "c")))
                                                     .build();

    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        Intervals.ETERNITY,
        new RowSignatureChecker(baseTable)
    );
    ProjectionMatch expected = new ProjectionMatch(
        CursorBuildSpec.builder()
                       .setAggregators(List.of(new LongSumAggregatorFactory("c", "c")))
                       .setVirtualColumns(VirtualColumns.create(ptHourlyFloor))
                       .setPhysicalColumns(Set.of("__time", "c_sum"))
                       .setPreferredOrdering(List.of())
                       .build(),
        Map.of("c", "c_sum")
    );
    Assertions.assertEquals(expected, projectionMatch);
  }

  @Test
  void testSchemaMatchDifferentTimeZone_dailyDoesNotMatch()
  {
    VirtualColumn ptDailyFloor = new ExpressionVirtualColumn(
        "__ptDaily",
        "timestamp_floor(__time, 'P1D', null, 'America/Los_Angeles')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    VirtualColumn dailyFloor = new ExpressionVirtualColumn(
        "__daily",
        "timestamp_floor(__time, 'P1D', null, null)",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.LONG)
                                         .add("b", ColumnType.STRING)
                                         .add("c", ColumnType.LONG)
                                         .build();
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .virtualColumns(dailyFloor)
                               .groupingColumns(new LongDimensionSchema("__daily"), new LongDimensionSchema("a"))
                               .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                               .build()
                               .toMetadataSchema(),
        12345
    );
    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setVirtualColumns(VirtualColumns.create(ptDailyFloor))
                                                     .setPhysicalColumns(Set.of("__time", "c"))
                                                     .setPreferredOrdering(List.of())
                                                     .setAggregators(List.of(new LongSumAggregatorFactory("c", "c")))
                                                     .build();

    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        Intervals.ETERNITY,
        new RowSignatureChecker(baseTable)
    );
    Assertions.assertNull(projectionMatch);
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
                                                             .setPhysicalColumns(Set.of("c"))
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
            Intervals.ETERNITY,
            new RowSignatureChecker(baseTable)
        )
    );
    CursorBuildSpec cursorBuildSpecWithFilter = CursorBuildSpec.builder()
                                                               .setPhysicalColumns(Set.of("b", "c"))
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
    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpecWithFilter,
        Intervals.ETERNITY,
        new RowSignatureChecker(baseTable)
    );
    ProjectionMatch expected = new ProjectionMatch(
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
  void testSchemaFilterRejectedWhenQueryFilterCannotBeRewritten()
  {
    // The query VC v0 := upper(b) is equivalent to the projection's b_upper, so matchQueryVirtualColumns remaps
    // v0 -> b_upper. The query's filter references v0 but can't rewrite its required columns, so it can't be remapped
    // into the projection's column namespace: the match must be rejected (fall back to the base table) rather than
    // throwing from rewriteRequiredColumns.
    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.LONG)
                                         .add("b", ColumnType.STRING)
                                         .add("c", ColumnType.LONG)
                                         .build();
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .filter(new EqualityFilter("b", ColumnType.STRING, "foo", null))
                               .virtualColumns(
                                   new ExpressionVirtualColumn("b_upper", "upper(b)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
                               )
                               .groupingColumns(new StringDimensionSchema("b_upper"), new LongDimensionSchema("a"))
                               .aggregators(new LongSumAggregatorFactory("c_sum", "c"))
                               .build()
                               .toMetadataSchema(),
        12345
    );
    CursorBuildSpec query = CursorBuildSpec.builder()
                                           .setVirtualColumns(
                                               VirtualColumns.create(
                                                   new ExpressionVirtualColumn("v0", "upper(b)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
                                               )
                                           )
                                           .setFilter(new NoRewriteFilter("v0"))
                                           .setPhysicalColumns(Set.of("b", "c"))
                                           .setPreferredOrdering(List.of())
                                           .build();

    Assertions.assertNull(
        Projections.matchAggregateProjection(
            spec.getSchema(),
            query,
            Intervals.ETERNITY,
            new RowSignatureChecker(baseTable)
        )
    );
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
                                                             .setPhysicalColumns(Set.of("a", "b", "c"))
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
            Intervals.ETERNITY,
            new RowSignatureChecker(baseTable)
        )
    );
    CursorBuildSpec cursorBuildSpecWithFilter = CursorBuildSpec.builder()
                                                               .setPhysicalColumns(Set.of("a", "b", "c"))
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
    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpecWithFilter,
        Intervals.ETERNITY,
        new RowSignatureChecker(baseTable)
    );
    ProjectionMatch expected = new ProjectionMatch(
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
  public void testSchemaMatchIntervalEternity()
  {
    final DateTime time = Granularities.DAY.bucketStart(DateTimes.nowUtc());
    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.STRING)
                                         .build();

    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .groupingColumns(new StringDimensionSchema("a"))
                               .build()
                               .toMetadataSchema(),
        12345
    );

    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setPhysicalColumns(Set.of("a"))
                                                     .setGroupingColumns(List.of("a"))
                                                     .build();

    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        Intervals.ETERNITY,
        new RowSignatureChecker(baseTable)
    );
    ProjectionMatch expected = new ProjectionMatch(
        CursorBuildSpec.builder()
                       .setPhysicalColumns(Set.of("a"))
                       .setGroupingColumns(List.of("a"))
                       .setAggregators(List.of())
                       .build(),
        Map.of()
    );
    Assertions.assertEquals(expected, projectionMatch);

    // projection with no time column can still match cursor build spec with eternity interval
    projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        new Interval(time, time.plusHours(1)),
        new RowSignatureChecker(baseTable)
    );

    Assertions.assertEquals(expected, projectionMatch);
  }

  @Test
  public void testSchemaMatchIntervalProjectionGranularityEternity()
  {
    final DateTime time = Granularities.DAY.bucketStart(DateTimes.nowUtc());

    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.STRING)
                                         .build();

    // hour granularity projection
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .groupingColumns(
                                   new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
                                   new StringDimensionSchema("a")
                               )
                               .virtualColumns(
                                   Granularities.toVirtualColumn(
                                       Granularities.HOUR,
                                       Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                   )
                               )
                               .build()
                               .toMetadataSchema(),
        12345
    );

    // eternity interval cursor build spec with granularity set
    CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                     .setPhysicalColumns(Set.of("__time", "a"))
                                                     .setGroupingColumns(List.of("v0", "a"))
                                                     .setVirtualColumns(
                                                         VirtualColumns.create(
                                                             Granularities.toVirtualColumn(Granularities.HOUR, "v0")
                                                         )
                                                     )
                                                     .build();


    ProjectionMatch expectedWithGranularity = new ProjectionMatch(
        CursorBuildSpec.builder()
                       .setPhysicalColumns(Set.of("__time", "a"))
                       .setGroupingColumns(List.of("v0", "a"))
                       .setAggregators(List.of())
                       .build(),
        Map.of("v0", "__time")
    );

    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        Intervals.ETERNITY,
        new RowSignatureChecker(baseTable)
    );
    Assertions.assertEquals(expectedWithGranularity, projectionMatch);

    projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpec,
        new Interval(time, time.plusHours(1)),
        new RowSignatureChecker(baseTable)
    );

    Assertions.assertEquals(expectedWithGranularity, projectionMatch);

  }

  @Test
  public void testSchemaMatchIntervalProjectionGranularity()
  {
    final DateTime time = Granularities.DAY.bucketStart(DateTimes.nowUtc());

    RowSignature baseTable = RowSignature.builder()
                                         .addTimeColumn()
                                         .add("a", ColumnType.STRING)
                                         .build();

    // hour granularity projection
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        AggregateProjectionSpec.builder("some_projection")
                               .groupingColumns(
                                   new LongDimensionSchema(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME),
                                   new StringDimensionSchema("a")
                               )
                               .virtualColumns(
                                   Granularities.toVirtualColumn(
                                       Granularities.HOUR,
                                       Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                   )
                               )
                               .build()
                               .toMetadataSchema(),
        12345
    );

    Interval day = new Interval(time, time.plusDays(1));
    Interval hour = new Interval(time, time.plusHours(1));
    Interval partial = new Interval(time, time.plusMinutes(42));
    // aligned interval cursor build spec
    CursorBuildSpec cursorBuildSpecHourInterval = CursorBuildSpec.builder()
                                                                 .setInterval(hour)
                                                                 .setPhysicalColumns(Set.of("a"))
                                                                 .setGroupingColumns(List.of("a"))
                                                                 .build();

    ProjectionMatch expected = new ProjectionMatch(
        CursorBuildSpec.builder()
                       .setInterval(hour)
                       .setPhysicalColumns(Set.of("a"))
                       .setGroupingColumns(List.of("a"))
                       .setAggregators(List.of())
                       .build(),
        Map.of()
    );
    ProjectionMatch projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpecHourInterval,
        day,
        new RowSignatureChecker(baseTable)
    );
    Assertions.assertEquals(expected, projectionMatch);


    // partial interval does not align with projection granularity (and does not contain data interval)
    CursorBuildSpec cursorBuildSpecPartialInterval = CursorBuildSpec.builder()
                                                                    .setInterval(partial)
                                                                    .setPhysicalColumns(Set.of("a"))
                                                                    .setGroupingColumns(List.of("a"))
                                                                    .build();

    projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpecPartialInterval,
        day,
        new RowSignatureChecker(baseTable)
    );
    Assertions.assertNull(projectionMatch);

    Interval wonky = new Interval(time, time.plusHours(1).plusMinutes(12));
    CursorBuildSpec cursorBuildSpecUnalignedButContaining = CursorBuildSpec.builder()
                                                                           .setInterval(wonky)
                                                                           .setPhysicalColumns(Set.of("a"))
                                                                           .setGroupingColumns(List.of("a"))
                                                                           .build();
    expected = new ProjectionMatch(
        CursorBuildSpec.builder()
                       .setInterval(wonky)
                       .setPhysicalColumns(Set.of("a"))
                       .setGroupingColumns(List.of("a"))
                       .setAggregators(List.of())
                       .build(),
        Map.of()
    );
    projectionMatch = Projections.matchAggregateProjection(
        spec.getSchema(),
        cursorBuildSpecUnalignedButContaining,
        hour,
        new RowSignatureChecker(baseTable)
    );
    Assertions.assertEquals(expected, projectionMatch);
  }

  private static RowSignature sig(String name, ColumnType type)
  {
    return RowSignature.builder().add(name, type).build();
  }

  @Test
  void testGetClusterGroupSegmentInternalFileName()
  {
    // Smoosh layout: __base$<id0>_<id1>...<idK>/<col>. The IDs encode the group's clustering identity via the
    // summary's per-column dictionaries.
    Assertions.assertEquals(
        "__base$0/tenant",
        Projections.getClusterGroupSegmentInternalFileName(List.of(0), "tenant")
    );
    Assertions.assertEquals(
        "__base$5/__time",
        Projections.getClusterGroupSegmentInternalFileName(List.of(5), "__time")
    );
    Assertions.assertEquals(
        "__base$0_1_3/__time",
        Projections.getClusterGroupSegmentInternalFileName(List.of(0, 1, 3), "__time")
    );
    Assertions.assertEquals("__base$42/", Projections.getClusterGroupSegmentInternalFilePrefix(List.of(42)));
    Assertions.assertEquals("__base$1_2/", Projections.getClusterGroupSegmentInternalFilePrefix(List.of(1, 2)));
  }

  @Test
  void testIsAllowedClusteringType()
  {
    Assertions.assertTrue(Projections.isAllowedClusteringType(ColumnType.STRING));
    Assertions.assertTrue(Projections.isAllowedClusteringType(ColumnType.LONG));
    Assertions.assertTrue(Projections.isAllowedClusteringType(ColumnType.DOUBLE));
    Assertions.assertTrue(Projections.isAllowedClusteringType(ColumnType.FLOAT));
    Assertions.assertFalse(Projections.isAllowedClusteringType(null));
    Assertions.assertFalse(Projections.isAllowedClusteringType(ColumnType.STRING_ARRAY));
    Assertions.assertFalse(Projections.isAllowedClusteringType(ColumnType.UNKNOWN_COMPLEX));
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
