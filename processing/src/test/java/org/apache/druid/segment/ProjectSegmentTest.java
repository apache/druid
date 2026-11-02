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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link ProjectSegment}, over a segment with an aggregate projection.
 */
public class ProjectSegmentTest extends InitializedNullHandlingTest
{
  private static final Closer CLOSER = Closer.create();
  private static final VirtualColumns RENAME_DIM = VirtualColumns.create(
      new ExpressionVirtualColumn("x", Parser.identifier("dim"), null)
  );

  /**
   * Segment with string column "dim" and long column "m", and an aggregate projection "p" that sums "m" by "dim".
   */
  private static Segment base;

  @BeforeAll
  public static void setUpClass(@TempDir final File tempDir)
  {
    final List<String> dims = ImmutableList.of("a", "a", "b");
    final List<InputRow> rows = new ArrayList<>();
    for (int i = 0; i < dims.size(); i++) {
      rows.add(
          new MapBasedInputRow(
              DateTimes.of("2000-01-01").plusHours(i),
              ImmutableList.of("dim", "m"),
              ImmutableMap.of("dim", dims.get(i), "m", i + 1L)
          )
      );
    }

    final QueryableIndex index =
        IndexBuilder.create()
                    .tmpDir(tempDir)
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMinTimestamp(DateTimes.of("2000-01-01").getMillis())
                            .withDimensionsSpec(
                                DimensionsSpec.builder()
                                              .setDimensions(
                                                  ImmutableList.of(
                                                      new StringDimensionSchema("dim"),
                                                      new LongDimensionSchema("m")
                                                  )
                                              )
                                              .build()
                            )
                            .withRollup(false)
                            .withProjections(
                                ImmutableList.of(
                                    AggregateProjectionSpec.builder("p")
                                                           .groupingColumns(new StringDimensionSchema("dim"))
                                                           .aggregators(new LongSumAggregatorFactory("sum_m", "m"))
                                                           .build()
                                )
                            )
                            .build()
                    )
                    .rows(rows)
                    .buildMMappedIndex();
    base = new QueryableIndexSegment(CLOSER.register(index), SegmentId.dummy("test"));
  }

  @AfterAll
  public static void tearDownClass() throws IOException
  {
    CLOSER.close();
  }

  @Test
  public void test_topNOptimizationInspector_overFilteredSegment()
  {
    // A lexicographic topN may stop scanning the dictionary early when every id is present, which a filter below the
    // projection makes untrue.
    final Segment projected = new ProjectSegment(new FilteredSegment(base, DimFilters.dimEquals("dim", "b")), RENAME_DIM);
    Assertions.assertFalse(projected.as(TopNOptimizationInspector.class).areAllDictionaryIdsPresent());
  }

  @Test
  public void test_aggregateProjection_readThroughProject()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .setDimensions(new DefaultDimensionSpec("dim", "d0"))
                    .setAggregatorSpecs(new LongSumAggregatorFactory("a0", "m"))
                    .build();
    final CursorFactory cursorFactory = new ProjectSegment(base, RENAME_DIM).as(CursorFactory.class);

    // Grouping on "dim" and summing "m" matches projection "p", and the cursor presents the aggregator's combined
    // values under its name.
    try (CursorHolder holder = cursorFactory.makeCursorHolder(GroupingEngine.makeCursorBuildSpec(query, null))) {
      Assertions.assertTrue(holder.isPreAggregated());
      Assertions.assertEquals(
          List.of(List.of("a", 3L), List.of("b", 3L)),
          SmallSegmentTestHelper.readRows(holder.asCursor(), "dim", "a0")
      );
    }
  }
}
