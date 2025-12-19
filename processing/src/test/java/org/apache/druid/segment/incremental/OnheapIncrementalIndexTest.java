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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class OnheapIncrementalIndexTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSpecSerde() throws JsonProcessingException
  {
    OnheapIncrementalIndex.Spec spec = new OnheapIncrementalIndex.Spec(true);
    Assert.assertEquals(spec, MAPPER.readValue(MAPPER.writeValueAsString(spec), OnheapIncrementalIndex.Spec.class));
  }

  @Test
  public void testProjectionHappyPath()
  {
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder()
                                                  .setDimensions(List.of(
                                                      new StringDimensionSchema("string"),
                                                      new LongDimensionSchema("long")
                                                  ))
                                                  .build();
    AggregatorFactory aggregatorFactory = new DoubleSumAggregatorFactory("double", "double");
    AggregateProjectionSpec projectionSpec =
        AggregateProjectionSpec.builder("proj")
                               .groupingColumns(new StringDimensionSchema("string"))
                               .aggregators(
                                   new LongSumAggregatorFactory("sum_long", "long"),
                                   new DoubleSumAggregatorFactory("double", "double")
                               )
                               .build();

    IncrementalIndex index = IndexBuilder.create()
                                         .schema(IncrementalIndexSchema.builder()
                                                                       .withDimensionsSpec(dimensionsSpec)
                                                                       .withRollup(true)
                                                                       .withMetrics(aggregatorFactory)
                                                                       .withProjections(List.of(projectionSpec))
                                                                       .build())
                                         .buildIncrementalIndex();
    Assert.assertNotNull(index.getProjection("proj"));
  }

  @Test
  public void testProjectionDuplicatedName()
  {
    // arrange
    DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;
    AggregatorFactory aggregatorFactory = new DoubleSumAggregatorFactory("double", "double");
    AggregateProjectionSpec.Builder bob = new AggregateProjectionSpec.Builder().aggregators(aggregatorFactory);
    // act & assert
    DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> IndexBuilder.create()
                          .schema(IncrementalIndexSchema.builder()
                                                        .withDimensionsSpec(dimensionsSpec)
                                                        .withRollup(true)
                                                        .withMetrics(aggregatorFactory)
                                                        .withProjections(
                                                            List.of(
                                                                bob.name("proj").build(),
                                                                bob.name("proj").build()
                                                            )
                                                        )
                                                        .build())
                          .buildIncrementalIndex()
    );
    Assert.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
    Assert.assertEquals("duplicate projection[proj]", e.getMessage());
  }

  @Test
  public void testSpecEqualsAndHashCode()
  {
    EqualsVerifier.forClass(OnheapIncrementalIndex.Spec.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testBadProjectionMismatchedDimensionTypes()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () ->
            IndexBuilder.create()
                        .schema(
                            IncrementalIndexSchema.builder()
                                                  .withDimensionsSpec(
                                                      DimensionsSpec.builder()
                                                                    .setDimensions(
                                                                        List.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withProjections(
                                                      List.of(
                                                          AggregateProjectionSpec.builder("mismatched dims")
                                                                                 .groupingColumns(new LongDimensionSchema("string"))
                                                                                 .build()
                                                      )
                                                  )
                                                  .build()
                        ).buildIncrementalIndex()
    );
    Assert.assertEquals(
        "projection[mismatched dims] contains dimension[string] with different type[LONG] than type[STRING] in base table",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionDimensionNoVirtualColumnOrBaseTable()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () ->
            IndexBuilder.create()
                        .schema(
                            IncrementalIndexSchema.builder()
                                                  .withDimensionsSpec(
                                                      DimensionsSpec.builder()
                                                                    .setDimensions(
                                                                        List.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withProjections(
                                                      List.of(
                                                          AggregateProjectionSpec.builder("sad grouping column")
                                                                                 .virtualColumns(
                                                                                     new ExpressionVirtualColumn(
                                                                                         "v0",
                                                                                         "cast(long, 'double')",
                                                                                         ColumnType.DOUBLE,
                                                                                         TestExprMacroTable.INSTANCE
                                                                                     )
                                                                                 )
                                                                                 .groupingColumns(
                                                                                     new DoubleDimensionSchema("v0"),
                                                                                     new StringDimensionSchema("missing")
                                                                                 )
                                                                                 .build()
                                                      )
                                                  )
                                                  .build()
                        ).buildIncrementalIndex()
    );
    Assert.assertEquals(
        "projection[sad grouping column] contains dimension[missing] that is not present on the base table or a virtual column",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionVirtualColumnNoDimension()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () ->
            IndexBuilder.create()
                        .schema(
                            IncrementalIndexSchema.builder()
                                                  .withDimensionsSpec(
                                                      DimensionsSpec.builder()
                                                                    .setDimensions(
                                                                        List.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withProjections(
                                                      List.of(
                                                          AggregateProjectionSpec.builder("sad virtual column")
                                                                                 .virtualColumns(
                                                                                     new ExpressionVirtualColumn(
                                                                                         "v0",
                                                                                         "double",
                                                                                         ColumnType.DOUBLE,
                                                                                         TestExprMacroTable.INSTANCE
                                                                                     )
                                                                                 )
                                                                                 .groupingColumns(
                                                                                     new LongDimensionSchema("long")
                                                                                 )
                                                                                 .build()
                                                      )
                                                  )
                                                  .build()
                        ).buildIncrementalIndex()
    );
    Assert.assertEquals(
        "projection[sad virtual column] contains virtual column[v0] that references an input[double] which is not a dimension in the base table",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionRollupMismatchedAggType()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () ->
            IndexBuilder.create()
                        .schema(
                            IncrementalIndexSchema.builder()
                                                  .withDimensionsSpec(
                                                      DimensionsSpec.builder()
                                                                    .setDimensions(
                                                                        List.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withRollup(true)
                                                  .withMetrics(
                                                      new DoubleSumAggregatorFactory("sum_double", "sum_double")
                                                  )
                                                  .withProjections(
                                                      List.of(
                                                          AggregateProjectionSpec.builder("mismatched agg")
                                                                                 .groupingColumns(new StringDimensionSchema(
                                                                                     "string"))
                                                                                 .aggregators(
                                                                                     new LongSumAggregatorFactory(
                                                                                         "sum_double",
                                                                                         "sum_double"
                                                                                     )
                                                                                 )
                                                                                 .build()
                                                      )
                                                  )
                                                  .build()
                        ).buildIncrementalIndex()
    );
    Assert.assertEquals(
        "projection[mismatched agg] contains aggregator[sum_double] that is not the 'combining' aggregator of base table aggregator[sum_double]",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionRollupBadAggInput()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () ->
            IndexBuilder.create()
                        .schema(
                            IncrementalIndexSchema.builder()
                                                  .withDimensionsSpec(
                                                      DimensionsSpec.builder()
                                                                    .setDimensions(
                                                                        List.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withRollup(true)
                                                  .withMetrics(
                                                      new DoubleSumAggregatorFactory("double", "double")
                                                  )
                                                  .withProjections(
                                                      List.of(
                                                          AggregateProjectionSpec.builder("renamed agg")
                                                                                 .groupingColumns(new StringDimensionSchema(
                                                                                     "string"))
                                                                                 .aggregators(
                                                                                     new LongSumAggregatorFactory(
                                                                                         "sum_long",
                                                                                         "long"
                                                                                     ),
                                                                                     new DoubleSumAggregatorFactory(
                                                                                         "sum_double",
                                                                                         "double"
                                                                                     )
                                                                                 )
                                                                                 .build()
                                                      )
                                                  )
                                                  .build()
                        ).buildIncrementalIndex()
    );
    Assert.assertEquals(
        "projection[renamed agg] contains aggregator[sum_double] that references aggregator[double] in base table but this is not supported, projection aggregators which reference base table aggregates must be 'combining' aggregators with the same name as the base table column",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionVirtualColumnAggInput()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () ->
            IndexBuilder.create()
                        .schema(
                            IncrementalIndexSchema.builder()
                                                  .withDimensionsSpec(
                                                      DimensionsSpec.builder()
                                                                    .setDimensions(
                                                                        List.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withProjections(
                                                      List.of(
                                                          AggregateProjectionSpec.builder("sad agg virtual column")
                                                                                 .virtualColumns(
                                                                                     new ExpressionVirtualColumn(
                                                                                         "v0",
                                                                                         "long + 100",
                                                                                         ColumnType.LONG,
                                                                                         TestExprMacroTable.INSTANCE
                                                                                     )
                                                                                 )
                                                                                 .groupingColumns(
                                                                                     new LongDimensionSchema("long")
                                                                                 )
                                                                                 .aggregators(
                                                                                     new LongSumAggregatorFactory(
                                                                                         "v0_sum",
                                                                                         "v0"
                                                                                     )
                                                                                 )
                                                                                 .build()
                                                      )
                                                  )
                                                  .build()
                        ).buildIncrementalIndex()
    );
    Assert.assertEquals(
        "projection[sad agg virtual column] contains aggregator[v0_sum] that is has required field[v0] which is a virtual column, this is not yet supported",
        t.getMessage()
    );
  }


  @Test
  public void testTimestampOutOfRange()
  {
    // arrange
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder()
                                                  .setDimensions(List.of(
                                                      new StringDimensionSchema("string"),
                                                      new LongDimensionSchema("long")
                                                  ))
                                                  .build();
    AggregatorFactory aggregatorFactory = new DoubleSumAggregatorFactory("double", "double");
    AggregateProjectionSpec projectionSpec =
        AggregateProjectionSpec.builder("proj")
                               .groupingColumns(new StringDimensionSchema("string"))
                               .aggregators(
                                   new LongSumAggregatorFactory("sum_long", "long"),
                                   new DoubleSumAggregatorFactory("double", "double")
                               )
                               .build();

    final DateTime minTimestamp = DateTimes.nowUtc();
    final DateTime outOfRangeTimestamp = DateTimes.nowUtc().minusDays(1);
    final DateTime outOfRangeProjectionTimestamp = Granularities.YEAR.bucketStart(outOfRangeTimestamp);

    final IncrementalIndex index = IndexBuilder.create()
                                               .schema(IncrementalIndexSchema.builder()
                                                                             .withDimensionsSpec(dimensionsSpec)
                                                                             .withRollup(true)
                                                                             .withMetrics(aggregatorFactory)
                                                                             .withProjections(List.of(projectionSpec))
                                                                             .withMinTimestamp(minTimestamp.getMillis())
                                                                             .build())
                                               .buildIncrementalIndex();

    IncrementalIndexAddResult addResult = index.add(
        new MapBasedInputRow(
            minTimestamp,
            List.of("string", "long"),
            Map.of(
                "string", "hello",
                "long", 10L
            )
        )
    );
    Assert.assertTrue(addResult.isRowAdded());

    final Map<String, Object> rowMap = new LinkedHashMap<>();
    rowMap.put("string", "hello");
    rowMap.put("long", 10L);

    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> index.add(
            new MapBasedInputRow(
                outOfRangeTimestamp.getMillis(),
                List.of("string", "long"),
                rowMap
            )
        )
    );

    Assert.assertEquals(
        "Cannot add row[{timestamp="
        + outOfRangeTimestamp
        + ", event={string=hello, long=10}, dimensions=[string, long]}] because it is below the minTimestamp["
        + minTimestamp
        + "]",
        t.getMessage()
    );

    AggregateProjectionSpec projectionSpecYear =
        AggregateProjectionSpec.builder("proj")
                               .virtualColumns(Granularities.toVirtualColumn(Granularities.YEAR, "g"))
                               .groupingColumns(new StringDimensionSchema("string"), new LongDimensionSchema("g"))
                               .aggregators(
                                   new LongSumAggregatorFactory("sum_long", "long"),
                                   new DoubleSumAggregatorFactory("double", "double")
                               )
                               .build();
    IncrementalIndex index2 = IndexBuilder.create()
                                          .schema(IncrementalIndexSchema.builder()
                                                                        .withDimensionsSpec(dimensionsSpec)
                                                                        .withRollup(true)
                                                                        .withMetrics(aggregatorFactory)
                                                                        .withProjections(List.of(projectionSpecYear))
                                                                        .withMinTimestamp(minTimestamp.getMillis())
                                                                        .build())
                                          .buildIncrementalIndex();

    t = Assert.assertThrows(
        DruidException.class,
        () -> index2.add(
            new MapBasedInputRow(
                minTimestamp,
                List.of("string", "long"),
                rowMap
            )
        )
    );

    Assert.assertEquals(
        "Cannot add row[{timestamp="
        + minTimestamp
        + ", event={string=hello, long=10}, dimensions=[string, long]}] to projection[proj] because projection effective timestamp["
        + outOfRangeProjectionTimestamp
        + "] is below the minTimestamp["
        + minTimestamp + "]",
        t.getMessage()
    );
  }
}
