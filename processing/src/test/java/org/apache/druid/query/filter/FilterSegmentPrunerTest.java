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

package org.apache.druid.query.filter;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

class FilterSegmentPrunerTest
{
  @Test
  void testNullFilter()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new FilterSegmentPruner(null, null, null)
    );
    Assertions.assertEquals("filter must not be null", t.getMessage());
  }

  @Test
  void testPrune()
  {
    DimFilter range_a = new RangeFilter("dim1", ColumnType.STRING, null, "aaa", null, null, null);
    DimFilter expression_b = new ExpressionDimFilter("dim2 == 'c'", null, TestExprMacroTable.INSTANCE);

    String interval1 = "2026-02-18T00:00:00Z/2026-02-19T00:00:00Z";
    String interval2 = "2026-02-19T00:00:00Z/2026-02-20T00:00:00Z";

    DataSegment seg1 = makeDataSegment(interval1, makeRange("dim1", 0, null, "abc"));
    DataSegment seg2 = makeDataSegment(interval1, makeRange("dim1", 1, "abc", "lmn"));
    DataSegment seg3 = makeDataSegment(interval1, makeRange("dim1", 2, "lmn", null));
    DataSegment seg4 = makeDataSegment(interval2, makeRange("dim2", 0, null, "b"));
    DataSegment seg5 = makeDataSegment(interval2, makeRange("dim2", 1, "b", "j"));
    DataSegment seg6 = makeDataSegment(interval2, makeRange("dim2", 2, "j", "s"));
    DataSegment seg7 = makeDataSegment(interval2, makeRange("dim2", 3, "s", "t"));

    List<DataSegment> segs = List.of(seg1, seg2, seg3, seg4, seg5, seg6, seg7);

    FilterSegmentPruner prunerRange = new FilterSegmentPruner(range_a, null, null);
    FilterSegmentPruner prunerEmptyFields = new FilterSegmentPruner(range_a, Collections.emptySet(), null);
    FilterSegmentPruner prunerExpression = new FilterSegmentPruner(expression_b, null, null);

    // prune twice to exercise cache
    Assertions.assertEquals(Set.of(seg1, seg4, seg5, seg6, seg7), prunerRange.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.of(seg1, seg4, seg5, seg6, seg7), prunerRange.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerExpression.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerExpression.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerEmptyFields.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerEmptyFields.prune(segs, Function.identity()));
  }

  @Test
  void testPruneVirtualColumn()
  {
    VirtualColumns shardVirtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("vdim1", "concat(dim1, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    VirtualColumns shardVirtualColumnsDifferentName = VirtualColumns.create(
        new ExpressionVirtualColumn("vdifferentname", "concat(dim1, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );

    String interval1 = "2026-02-18T00:00:00Z/2026-02-19T00:00:00Z";

    DataSegment seg1 = makeDataSegment(
        interval1,
        makeRange(List.of("vdim1"), shardVirtualColumns, 0, null, StringTuple.create("abcfoo"))
    );
    DataSegment seg2 = makeDataSegment(
        interval1,
        makeRange(List.of("vdim1"), shardVirtualColumns, 1, StringTuple.create("abcfoo"), StringTuple.create("lmnfoo"))
    );
    // same virtual column with a different name in this segment
    DataSegment seg3 = makeDataSegment(
        interval1,
        makeRange(List.of("vdifferentname"), shardVirtualColumnsDifferentName, 2, StringTuple.create("lmnfoo"), null)
    );

    List<DataSegment> segs = List.of(seg1, seg2, seg3);

    // same expression, same name
    VirtualColumns queryVirtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("vdim1", "concat(dim1, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    DimFilter range_a = new RangeFilter("vdim1", ColumnType.STRING, null, "aaa", null, null, null);
    FilterSegmentPruner prunerRange = new FilterSegmentPruner(range_a, null, queryVirtualColumns);
    FilterSegmentPruner prunerEmptyFields = new FilterSegmentPruner(range_a, Collections.emptySet(), queryVirtualColumns);
    // prune twice to exercise cache
    Assertions.assertEquals(Set.of(seg1), prunerRange.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.of(seg1), prunerRange.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerEmptyFields.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerEmptyFields.prune(segs, Function.identity()));

    // same expression, different name
    queryVirtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("v0", "concat(dim1, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    range_a = new RangeFilter("v0", ColumnType.STRING, null, "aaa", null, null, null);
    prunerRange = new FilterSegmentPruner(range_a, null, queryVirtualColumns);
    prunerEmptyFields = new FilterSegmentPruner(range_a, Collections.emptySet(), queryVirtualColumns);

    // prune twice to exercise cache
    Assertions.assertEquals(Set.of(seg1), prunerRange.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.of(seg1), prunerRange.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerEmptyFields.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerEmptyFields.prune(segs, Function.identity()));
  }

  @Test
  void testCombineWithFilterPruner()
  {
    DimFilter filterA = new RangeFilter("dim1", ColumnType.STRING, null, "aaa", null, null, null);
    DimFilter filterB = new RangeFilter("dim2", ColumnType.STRING, null, "bbb", null, null, null);

    FilterSegmentPruner prunerA = new FilterSegmentPruner(filterA, null, VirtualColumns.EMPTY);
    FilterSegmentPruner prunerB = new FilterSegmentPruner(filterB, null, VirtualColumns.EMPTY);

    SegmentPruner combined = prunerA.combine(prunerB);
    Assertions.assertInstanceOf(FilterSegmentPruner.class, combined);

    // combined pruner should prune based on both filters
    String interval1 = "2026-01-01T00:00:00Z/2026-01-02T00:00:00Z";
    DataSegment includedByBoth = makeDataSegment(interval1, makeRange("dim1", 0, null, "abc"));
    DataSegment excludedByDim1 = makeDataSegment(interval1, makeRange("dim1", 1, "lmn", null));
    DataSegment excludedByDim2 = makeDataSegment(interval1, makeRange("dim2", 0, "ccc", null));

    Assertions.assertTrue(combined.include(includedByBoth));
    Assertions.assertFalse(combined.include(excludedByDim1));
    Assertions.assertFalse(combined.include(excludedByDim2));
  }

  @Test
  void testCombineWithCompositePruner()
  {
    DimFilter filterA = new RangeFilter("dim1", ColumnType.STRING, null, "aaa", null, null, null);
    DimFilter filterB = new RangeFilter("dim2", ColumnType.STRING, null, "bbb", null, null, null);

    FilterSegmentPruner filterPrunerA = new FilterSegmentPruner(filterA, null, VirtualColumns.EMPTY);
    FilterSegmentPruner filterPrunerB = new FilterSegmentPruner(filterB, null, VirtualColumns.EMPTY);

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(filterPrunerB);
    CompositeSegmentPruner composite = new CompositeSegmentPruner(pruners);

    // FilterSegmentPruner.combine(CompositeSegmentPruner) should delegate to composite
    SegmentPruner combined = filterPrunerA.combine(composite);
    Assertions.assertInstanceOf(CompositeSegmentPruner.class, combined);

    String interval1 = "2026-01-01T00:00:00Z/2026-01-02T00:00:00Z";
    DataSegment excludedByDim1 = makeDataSegment(interval1, makeRange("dim1", 0, "lmn", null));
    Assertions.assertFalse(combined.include(excludedByDim1));
  }

  @Test
  void testCombineWithUnknownPrunerType()
  {
    DimFilter filterA = new RangeFilter("dim1", ColumnType.STRING, null, "aaa", null, null, null);
    FilterSegmentPruner filterPruner = new FilterSegmentPruner(filterA, null, VirtualColumns.EMPTY);

    SegmentPruner other = new SegmentPruner()
    {
      @Override
      public boolean include(DataSegment segment)
      {
        return false;
      }

      @Override
      public SegmentPruner combine(SegmentPruner other)
      {
        return this;
      }
    };

    SegmentPruner combined = filterPruner.combine(other);
    Assertions.assertInstanceOf(CompositeSegmentPruner.class, combined);
  }

  @Test
  void testPruneVirtualColumnWithDependentVirtualColumn()
  {
    VirtualColumns shardVirtualColumns = VirtualColumns.create(
        new NestedFieldVirtualColumn("obj", "$.a", "n0", ColumnType.STRING),
        new ExpressionVirtualColumn("e0", "lower(\"n0\")", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    // same expressions, different names
    VirtualColumns shardVirtualColumnsDifferentNames = VirtualColumns.create(
        new NestedFieldVirtualColumn("obj", "$.a", "n1", ColumnType.STRING),
        new ExpressionVirtualColumn("e1", "lower(\"n1\")", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );

    VirtualColumns shardVirtualColumnsDifferent = VirtualColumns.create(
        new NestedFieldVirtualColumn("obj", "$.b", "n0", ColumnType.STRING),
        new ExpressionVirtualColumn("e0", "lower(\"n0\")", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );

    String interval1 = "2026-02-18T00:00:00Z/2026-02-19T00:00:00Z";
    String interval2 = "2026-02-19T00:00:00Z/2026-02-20T00:00:00Z";
    String interval3 = "2026-02-20T00:00:00Z/2026-02-21T00:00:00Z";
    DataSegment seg1 = makeDataSegment(
        interval1,
        makeRange(List.of("e0"), shardVirtualColumns, 0, null, StringTuple.create("f"))
    );
    DataSegment seg2 = makeDataSegment(
        interval1,
        makeRange(List.of("e0"), shardVirtualColumns, 1, StringTuple.create("f"), null)
    );
    // same partitioning but different names in these segments
    DataSegment seg3 = makeDataSegment(
        interval2,
        makeRange(List.of("e1"), shardVirtualColumnsDifferentNames, 0, null, StringTuple.create("f"))
    );
    DataSegment seg4 = makeDataSegment(
        interval2,
        makeRange(List.of("e1"), shardVirtualColumnsDifferentNames, 1, StringTuple.create("f"), null)
    );
    DataSegment seg5 = makeDataSegment(
        interval3,
        makeRange(List.of("e0"), shardVirtualColumnsDifferent, 0, null, StringTuple.create("f"))
    );
    DataSegment seg6 = makeDataSegment(
        interval3,
        makeRange(List.of("e0"), shardVirtualColumnsDifferent, 1, StringTuple.create("f"), null)
    );

    List<DataSegment> segs = List.of(seg1, seg2, seg3, seg4, seg5, seg6);

    // query uses its own names
    VirtualColumns queryVirtualColumns = VirtualColumns.create(
        new NestedFieldVirtualColumn("obj", "$.a", "v0", ColumnType.STRING),
        new ExpressionVirtualColumn("v1", "lower(\"v0\")", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    // lower($.a) < "f" should prune the second half of the range (seg2 and seg4)
    DimFilter filter = new RangeFilter("v1", ColumnType.STRING, null, "f", false, true, null);
    FilterSegmentPruner pruner = new FilterSegmentPruner(filter, null, queryVirtualColumns);

    // prune twice to exercise cache
    Assertions.assertEquals(Set.of(seg1, seg3, seg5, seg6), pruner.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.of(seg1, seg3, seg5, seg6), pruner.prune(segs, Function.identity()));

  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(FilterSegmentPruner.class)
                  .usingGetClass()
                  .withIgnoredFields("rangeCache", "shardEquivalenceCache")
                  .verify();
  }

  private ShardSpec makeRange(
      String column,
      int partitionNumber,
      @Nullable String start,
      @Nullable String end
  )
  {
    return makeRange(
        List.of(column),
        partitionNumber,
        start == null ? null : StringTuple.create(start),
        end == null ? null : StringTuple.create(end)
    );
  }

  private ShardSpec makeRange(
      List<String> columns,
      int partitionNumber,
      @Nullable StringTuple start,
      @Nullable StringTuple end
  )
  {
    return makeRange(
        columns,
        null,
        partitionNumber,
        start,
        end
    );
  }

  private ShardSpec makeRange(
      List<String> columns,
      VirtualColumns virtualColumns,
      int partitionNumber,
      @Nullable StringTuple start,
      @Nullable StringTuple end
  )
  {
    return new DimensionRangeShardSpec(
        columns,
        virtualColumns,
        start,
        end,
        partitionNumber,
        0
    );
  }

  private DataSegment makeDataSegment(String intervalString, ShardSpec shardSpec)
  {
    Interval interval = Intervals.of(intervalString);
    return DataSegment.builder(SegmentId.of("prune-test", interval, "0", shardSpec))
                      .shardSpec(shardSpec)
                      .build();
  }
}
