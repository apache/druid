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

package org.apache.druid.msq.querykit;

import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.msq.querykit.scan.ScanQueryStageProcessor;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.ProjectDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Tests for {@link BaseLeafStageProcessor}.
 */
public class BaseLeafStageProcessorTest
{
  @Test
  public void test_getPruner_unionUnderBroadcastJoin()
  {
    final DataSegment segmentA = makeSegment("a", "b");
    final DataSegment segmentZ = makeSegment("z", null);

    final DataSource union = new UnionDataSource(
        List.of(
            new InputNumberDataSource(0),
            new ProjectDataSource(
                new InputNumberDataSource(1),
                VirtualColumns.create(
                    new ExpressionVirtualColumn("dim1", Parser.identifier("dim2"), ColumnType.STRING)
                )
            ),
            FilteredDataSource.create(
                new InputNumberDataSource(3),
                new EqualityFilter("dim1", ColumnType.STRING, "zzz", null)
            )
        )
    );

    final DataSource join = JoinDataSource.create(
        union,
        new InputNumberDataSource(2),
        "j.",
        "dim2 == \"j.k\"",
        JoinType.INNER,
        null,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );

    final ScanQueryStageProcessor processor = new ScanQueryStageProcessor(
        Druids.newScanQueryBuilder()
              .dataSource(join)
              .intervals(QuerySegmentSpec.ETERNITY)
              .filters(new EqualityFilter("dim1", ColumnType.STRING, "abc", null))
              .columns("dim1")
              .build()
    );

    // Arm 0 reads dim1 directly, so only the query filter applies.
    final SegmentPruner arm0Pruner = processor.getPruner(null, 0);
    Assertions.assertTrue(arm0Pruner.include(segmentA));
    Assertions.assertFalse(arm0Pruner.include(segmentZ));

    // Arm 1 presents dim2 as dim1, so the segments' ranges on dim1 say nothing about the query filter.
    final SegmentPruner arm1Pruner = processor.getPruner(null, 1);
    Assertions.assertTrue(arm1Pruner == null || arm1Pruner.include(segmentZ));

    // Input 2 is broadcast, so it gets no pruner.
    Assertions.assertNull(processor.getPruner(null, 2));

    // Arm 3's own filter applies on top of the query filter.
    Assertions.assertFalse(processor.getPruner(null, 3).include(segmentA));
  }

  /**
   * Creates a segment of table "foo" whose values of "dim1" are in [start, end).
   */
  private static DataSegment makeSegment(final String start, @Nullable final String end)
  {
    final DimensionRangeShardSpec shardSpec = new DimensionRangeShardSpec(
        List.of("dim1"),
        null,
        StringTuple.create(start),
        end == null ? null : StringTuple.create(end),
        0,
        null
    );
    return DataSegment.builder(SegmentId.of("foo", Intervals.of("2000/2001"), "0", shardSpec))
                      .shardSpec(shardSpec)
                      .build();
  }
}
