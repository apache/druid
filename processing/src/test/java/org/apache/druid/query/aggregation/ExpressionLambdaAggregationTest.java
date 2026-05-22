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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Result;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Verifies that {@link ExpressionLambdaAggregatorFactory} can be used as an ingest-time metric for primitive numeric
 * types.
 */
public class ExpressionLambdaAggregationTest extends InitializedNullHandlingTest
{
  private static final String DIM = "groupKey";
  private static final String LONG_FIELD = "longField";
  private static final String DOUBLE_FIELD = "doubleField";
  private static final DateTime TIMESTAMP = DateTimes.of("2020-01-01");

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private QueryableIndex mergedIndex;
  private Segment segment;

  @After
  public void tearDown()
  {
    if (segment != null) {
      CloseableUtils.closeAndWrapExceptions(segment);
    }
    if (mergedIndex != null) {
      CloseableUtils.closeAndWrapExceptions(mergedIndex);
    }
  }

  @Test
  public void testNumericExpressionLambdaIngestRollupViaMerge() throws Exception
  {
    // Three rows sharing the same (timestamp, dim) so they roll up into a single output row during merge.
    // longField values: 1 (0b001), 2 (0b010), 4 (0b100) — sum=7, bitwiseOr=7
    // doubleField values: 1.5, 2.0, 0.25 — sum=3.75
    final List<InputRow> rows = List.of(
        row(1L, 1.5),
        row(2L, 2.0),
        row(4L, 0.25)
    );

    final ExpressionLambdaAggregatorFactory longSum = new ExpressionLambdaAggregatorFactory(
        "long_sum",
        Set.of(LONG_FIELD),
        null,
        "0",
        null,
        null,
        false,
        false,
        "__acc + " + LONG_FIELD,
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    // BitwiseSqlAggregator-style: same single-field, op("__acc", field) fold
    final ExpressionLambdaAggregatorFactory bitwiseOr = new ExpressionLambdaAggregatorFactory(
        "bitwise_or",
        ImmutableSet.of(LONG_FIELD),
        null,
        "0",
        null,
        null,
        false,
        false,
        "bitwiseOr(\"__acc\", \"" + LONG_FIELD + "\")",
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    final ExpressionLambdaAggregatorFactory doubleSum = new ExpressionLambdaAggregatorFactory(
        "double_sum",
        ImmutableSet.of(DOUBLE_FIELD),
        null,
        "0.0",
        null,
        null,
        false,
        false,
        "__acc + " + DOUBLE_FIELD,
        null,
        null,
        null,
        null,
        TestExprMacroTable.INSTANCE
    );

    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withQueryGranularity(Granularities.NONE)
        .withRollup(true)
        .withDimensionsSpec(
            DimensionsSpec.builder()
                          .setDimensions(ImmutableList.of(new StringDimensionSchema(DIM)))
                          .build()
        )
        .withMetrics(
            new CountAggregatorFactory("count"),
            longSum,
            bitwiseOr,
            doubleSum
        )
        .build();

    mergedIndex = IndexBuilder.create()
                              .tmpDir(tempFolder.newFolder())
                              .schema(schema)
                              .intermediaryPersistSize(1)
                              .rows(rows)
                              .buildMMappedMergedIndex();

    segment = new QueryableIndexSegment(mergedIndex, SegmentId.dummy("test"));

    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("test")
                                        .granularity(Granularities.ALL)
                                        .intervals("1970/2050")
                                        .aggregators(
                                            new LongSumAggregatorFactory("count", "count"),
                                            longSum.getCombiningFactory(),
                                            bitwiseOr.getCombiningFactory(),
                                            doubleSum.getCombiningFactory()
                                        )
                                        .build();

    final AggregationTestHelper helper =
        AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(Collections.emptyList(), tempFolder);

    final Sequence<Result<TimeseriesResultValue>> seq = helper.runQueryOnSegmentsObjs(
        ImmutableList.of(segment),
        query
    );
    final TimeseriesResultValue result = Iterables.getOnlyElement(seq.toList()).getValue();

    // Three input rows rolled up into one, count reflects rollup happened
    Assert.assertEquals(3L, result.getLongMetric("count").longValue());
    Assert.assertEquals(7L, result.getLongMetric("long_sum").longValue());
    Assert.assertEquals(7L, result.getLongMetric("bitwise_or").longValue());
    Assert.assertEquals(3.75, result.getDoubleMetric("double_sum").doubleValue(), 0.0);
  }

  private static InputRow row(long longVal, double doubleVal)
  {
    return new MapBasedInputRow(
        TIMESTAMP,
        ImmutableList.of(DIM),
        ImmutableMap.of(
            DIM, "a",
            LONG_FIELD, longVal,
            DOUBLE_FIELD, doubleVal
        )
    );
  }
}
