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
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

class CompositeSegmentPrunerTest
{
  private static final DimFilter FILTER_DIM1 = new RangeFilter(
      "dim1",
      ColumnType.STRING,
      null,
      "aaa",
      null,
      null,
      null
  );
  private static final DimFilter FILTER_DIM2 = new RangeFilter(
      "dim2",
      ColumnType.STRING,
      null,
      "bbb",
      null,
      null,
      null
  );

  @Test
  void testIncludeAllPrunersPass()
  {
    DataSegment seg = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, null, "zzz"));
    FilterSegmentPruner filter = new FilterSegmentPruner(FILTER_DIM1, null, VirtualColumns.EMPTY);
    // a pruner that always includes
    SegmentPruner alwaysInclude = new AlwaysIncludePruner();

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(filter);
    pruners.add(alwaysInclude);
    CompositeSegmentPruner composite = new CompositeSegmentPruner(pruners);

    Assertions.assertTrue(composite.include(seg));
  }

  @Test
  void testIncludeOnePrunerExcludes()
  {
    // seg has dim1 range [lmn, null) which is outside filter range (null, aaa)
    DataSegment seg = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, "lmn", null));
    FilterSegmentPruner filter = new FilterSegmentPruner(FILTER_DIM1, null, VirtualColumns.EMPTY);
    SegmentPruner alwaysInclude = new AlwaysIncludePruner();

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(filter);
    pruners.add(alwaysInclude);
    CompositeSegmentPruner composite = new CompositeSegmentPruner(pruners);

    Assertions.assertFalse(composite.include(seg));
  }

  @Test
  void testPrune()
  {
    DataSegment seg1 = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, null, "abc"));
    DataSegment seg2 = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 1, "lmn", null));
    FilterSegmentPruner filter = new FilterSegmentPruner(FILTER_DIM1, null, VirtualColumns.EMPTY);
    SegmentPruner alwaysInclude = new AlwaysIncludePruner();

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(filter);
    pruners.add(alwaysInclude);
    CompositeSegmentPruner composite = new CompositeSegmentPruner(pruners);

    Assertions.assertEquals(
        Set.of(seg1),
        composite.prune(List.of(seg1, seg2), Function.identity())
    );
  }

  @Test
  void testValidateRejectsMultipleFilterPruners()
  {
    FilterSegmentPruner filter1 = new FilterSegmentPruner(FILTER_DIM1, null, VirtualColumns.EMPTY);
    FilterSegmentPruner filter2 = new FilterSegmentPruner(FILTER_DIM2, null, VirtualColumns.EMPTY);

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(filter1);
    pruners.add(filter2);

    Assertions.assertThrows(DruidException.class, () -> new CompositeSegmentPruner(pruners));
  }

  @Test
  void testCombineWithFilterPrunerFoldsIntoExisting()
  {
    FilterSegmentPruner filter1 = new FilterSegmentPruner(FILTER_DIM1, null, VirtualColumns.EMPTY);
    SegmentPruner alwaysInclude = new AlwaysIncludePruner();

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(filter1);
    pruners.add(alwaysInclude);
    CompositeSegmentPruner composite = new CompositeSegmentPruner(pruners);

    FilterSegmentPruner filter2 = new FilterSegmentPruner(FILTER_DIM2, null, VirtualColumns.EMPTY);
    SegmentPruner combined = composite.combine(filter2);

    // result should be a composite with the filter pruners merged into one
    Assertions.assertInstanceOf(CompositeSegmentPruner.class, combined);

    // seg has dim1 < aaa (included by filter1) but dim2 starts at "ccc" (excluded by filter2)
    DataSegment includedBoth = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, null, "abc"));
    DataSegment excludedByDim2 = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim2", 0, "ccc", null));

    Assertions.assertTrue(combined.include(includedBoth));
    Assertions.assertFalse(combined.include(excludedByDim2));
  }

  @Test
  void testCombineWithFilterPrunerAddsWhenNoExistingFilter()
  {
    SegmentPruner alwaysInclude = new AlwaysIncludePruner();

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(alwaysInclude);
    CompositeSegmentPruner composite = new CompositeSegmentPruner(pruners);

    FilterSegmentPruner filter = new FilterSegmentPruner(FILTER_DIM1, null, VirtualColumns.EMPTY);
    SegmentPruner combined = composite.combine(filter);

    Assertions.assertInstanceOf(CompositeSegmentPruner.class, combined);

    // the filter should be active in the result
    DataSegment excluded = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, "lmn", null));
    Assertions.assertFalse(combined.include(excluded));
  }

  @Test
  void testCombineWithComposite()
  {
    FilterSegmentPruner filter1 = new FilterSegmentPruner(FILTER_DIM1, null, VirtualColumns.EMPTY);
    SegmentPruner alwaysInclude = new AlwaysIncludePruner();

    Set<SegmentPruner> pruners1 = new LinkedHashSet<>();
    pruners1.add(filter1);
    pruners1.add(alwaysInclude);
    CompositeSegmentPruner composite1 = new CompositeSegmentPruner(pruners1);

    FilterSegmentPruner filter2 = new FilterSegmentPruner(FILTER_DIM2, null, VirtualColumns.EMPTY);

    Set<SegmentPruner> pruners2 = new LinkedHashSet<>();
    pruners2.add(filter2);
    CompositeSegmentPruner composite2 = new CompositeSegmentPruner(pruners2);

    SegmentPruner combined = composite1.combine(composite2);

    Assertions.assertInstanceOf(CompositeSegmentPruner.class, combined);

    // filter pruners from both should be merged, not cause a validation error
    DataSegment excludedByDim1 = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, "lmn", null));
    DataSegment excludedByDim2 = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim2", 0, "ccc", null));
    DataSegment included = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, null, "abc"));

    Assertions.assertFalse(combined.include(excludedByDim1));
    Assertions.assertFalse(combined.include(excludedByDim2));
    Assertions.assertTrue(combined.include(included));
  }

  @Test
  void testCombineWithOtherPrunerType()
  {
    SegmentPruner alwaysInclude = new AlwaysIncludePruner();

    Set<SegmentPruner> pruners = new LinkedHashSet<>();
    pruners.add(alwaysInclude);
    CompositeSegmentPruner composite = new CompositeSegmentPruner(pruners);

    NeverIncludePruner neverInclude = new NeverIncludePruner();
    SegmentPruner combined = composite.combine(neverInclude);

    Assertions.assertInstanceOf(CompositeSegmentPruner.class, combined);

    DataSegment seg = makeDataSegment("2026-01-01/2026-01-02", makeRange("dim1", 0, null, "abc"));
    Assertions.assertFalse(combined.include(seg));
  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(CompositeSegmentPruner.class).usingGetClass().verify();
  }

  private ShardSpec makeRange(String column, int partitionNumber, String start, String end)
  {
    return new DimensionRangeShardSpec(
        List.of(column),
        null,
        start == null ? null : StringTuple.create(start),
        end == null ? null : StringTuple.create(end),
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

  /**
   * Simple test pruner that always includes segments.
   */
  private static class AlwaysIncludePruner implements SegmentPruner
  {
    @Override
    public boolean include(DataSegment segment)
    {
      return true;
    }

    @Override
    public SegmentPruner combine(SegmentPruner other)
    {
      return other;
    }
  }

  /**
   * Simple test pruner that never includes segments.
   */
  private static class NeverIncludePruner implements SegmentPruner
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
  }
}
