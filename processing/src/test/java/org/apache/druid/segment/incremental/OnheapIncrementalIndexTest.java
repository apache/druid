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
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.TestColumnSelectorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestObjectColumnSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionPlan;
import org.apache.druid.segment.virtual.ExpressionSelectors;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class OnheapIncrementalIndexTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSpecSerde() throws JsonProcessingException
  {
    OnheapIncrementalIndex.Spec spec = new OnheapIncrementalIndex.Spec(true);
    Assertions.assertEquals(spec, MAPPER.readValue(MAPPER.writeValueAsString(spec), OnheapIncrementalIndex.Spec.class));
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
    Assertions.assertNotNull(index.getProjection("proj"));
  }

  @Test
  public void testProjectionDuplicatedName()
  {
    // arrange
    DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;
    AggregatorFactory aggregatorFactory = new DoubleSumAggregatorFactory("double", "double");
    AggregateProjectionSpec.Builder bob = new AggregateProjectionSpec.Builder().aggregators(aggregatorFactory);
    // act & assert
    DruidException e = Assertions.assertThrows(
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
    Assertions.assertEquals(DruidException.Category.DEFENSIVE, e.getCategory());
    Assertions.assertEquals("duplicate projection[proj]", e.getMessage());
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
    Throwable t = Assertions.assertThrows(
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
    Assertions.assertEquals(
        "projection[mismatched dims] contains dimension[string] with different type[LONG] than type[STRING] in base table",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionDimensionNoVirtualColumnOrBaseTable()
  {
    Throwable t = Assertions.assertThrows(
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
    Assertions.assertEquals(
        "projection[sad grouping column] contains dimension[missing] that is not present on the base table or a virtual column",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionVirtualColumnNoDimension()
  {
    Throwable t = Assertions.assertThrows(
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
    Assertions.assertEquals(
        "projection[sad virtual column] contains virtual column[v0] that references an input[double] which is not a dimension in the base table",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionRollupMismatchedAggType()
  {
    Throwable t = Assertions.assertThrows(
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
    Assertions.assertEquals(
        "projection[mismatched agg] contains aggregator[sum_double] that is not the 'combining' aggregator of base table aggregator[sum_double]",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionRollupBadAggInput()
  {
    Throwable t = Assertions.assertThrows(
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
    Assertions.assertEquals(
        "projection[renamed agg] contains aggregator[sum_double] that references aggregator[double] in base table but this is not supported, projection aggregators which reference base table aggregates must be 'combining' aggregators with the same name as the base table column",
        t.getMessage()
    );
  }

  @Test
  public void testBadProjectionVirtualColumnAggInput()
  {
    Throwable t = Assertions.assertThrows(
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
    Assertions.assertEquals(
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

    final DateTime minTimestamp = DateTimes.of("2001-01-01T01:00:00");
    final DateTime minTimestampProjectionTimestamp = Granularities.YEAR.bucketStart(minTimestamp);
    final DateTime outOfRangeTimestamp = minTimestamp.minusDays(1);

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
    Assertions.assertTrue(addResult.isRowAdded());

    final Map<String, Object> rowMap = new LinkedHashMap<>();
    rowMap.put("string", "hello");
    rowMap.put("long", 10L);

    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> index.add(
            new MapBasedInputRow(
                outOfRangeTimestamp.getMillis(),
                List.of("string", "long"),
                rowMap
            )
        )
    );

    Assertions.assertEquals(
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

    t = Assertions.assertThrows(
        DruidException.class,
        () -> index2.add(
            new MapBasedInputRow(
                minTimestamp,
                List.of("string", "long"),
                rowMap
            )
        )
    );

    Assertions.assertEquals(
        "Cannot add row[{timestamp="
        + minTimestamp
        + ", event={string=hello, long=10}, dimensions=[string, long]}] to projection[proj] because projection effective timestamp["
        + minTimestampProjectionTimestamp
        + "] is below the minTimestamp["
        + minTimestamp + "]",
        t.getMessage()
    );
  }

  @Test
  public void testExpressionPlanCacheUsesExpressionIdentity()
  {
    final OnheapIncrementalIndex.CachingColumnSelectorFactory selectorFactory = makeCachingColumnSelectorFactory();
    final Expr expression = Parser.parse("value", TestExprMacroTable.INSTANCE);
    final Expr structurallyEqualExpression = Parser.parse("value", TestExprMacroTable.INSTANCE);
    final Expr differentExpression = Parser.parse("other", TestExprMacroTable.INSTANCE);

    Assertions.assertNotSame(expression, structurallyEqualExpression);

    final ExpressionPlan firstPlan = selectorFactory.getExpressionPlan(expression);
    final ExpressionPlan sameExpressionPlan = selectorFactory.getExpressionPlan(expression);
    Assertions.assertNotSame(firstPlan, sameExpressionPlan);
    Assertions.assertSame(expression, firstPlan.getExpression());
    Assertions.assertSame(expression, sameExpressionPlan.getExpression());

    final ExpressionPlan structurallyEqualPlan = selectorFactory.getExpressionPlan(structurallyEqualExpression);
    Assertions.assertNotSame(firstPlan, structurallyEqualPlan);
    Assertions.assertSame(structurallyEqualExpression, structurallyEqualPlan.getExpression());

    final ExpressionPlan differentPlan = selectorFactory.getExpressionPlan(differentExpression);
    Assertions.assertNotSame(structurallyEqualPlan, differentPlan);
    Assertions.assertSame(differentExpression, differentPlan.getExpression());

    Assertions.assertNotSame(firstPlan, selectorFactory.getExpressionPlan(expression));
    Assertions.assertNotSame(structurallyEqualPlan, selectorFactory.getExpressionPlan(structurallyEqualExpression));
    Assertions.assertNotSame(differentPlan, selectorFactory.getExpressionPlan(differentExpression));

    final Expr functionExpression = Parser.parse("value + 1", TestExprMacroTable.INSTANCE);
    final ExpressionPlan functionPlan = selectorFactory.getExpressionPlan(functionExpression);
    final ExpressionPlan sameFunctionPlan = selectorFactory.getExpressionPlan(functionExpression);
    Assertions.assertNotSame(functionPlan, sameFunctionPlan);
    Assertions.assertNotSame(functionPlan.getExpression(), sameFunctionPlan.getExpression());

    final Expr constantExpression = Parser.parse("'constant'", TestExprMacroTable.INSTANCE);
    final ExpressionPlan firstConstantPlan = selectorFactory.getExpressionPlan(constantExpression);
    final ExpressionPlan secondConstantPlan = selectorFactory.getExpressionPlan(constantExpression);
    Assertions.assertNotSame(firstConstantPlan.getExpression(), secondConstantPlan.getExpression());
  }

  @Test
  public void testExpressionPlanCacheIsSafeForConcurrentReplacement() throws Exception
  {
    final OnheapIncrementalIndex.CachingColumnSelectorFactory selectorFactory = makeCachingColumnSelectorFactory();
    final Expr firstExpression = Parser.parse("value", TestExprMacroTable.INSTANCE);
    final Expr secondExpression = Parser.parse("other", TestExprMacroTable.INSTANCE);
    final int threadCount = 8;
    final int iterationsPerThread = 2_000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final ExecutorService executor = Execs.multiThreaded(threadCount, "expression-plan-cache-test-%d");

    try {
      final List<Future<?>> futures = new ArrayList<>(threadCount);
      for (int threadNumber = 0; threadNumber < threadCount; threadNumber++) {
        final int thread = threadNumber;
        futures.add(executor.submit(() -> {
          startLatch.await();
          for (int iteration = 0; iteration < iterationsPerThread; iteration++) {
            final Expr expectedExpression = ((thread + iteration) & 1) == 0 ? firstExpression : secondExpression;
            final ExpressionPlan plan = selectorFactory.getExpressionPlan(expectedExpression);
            Assertions.assertSame(expectedExpression, plan.getExpression());
          }
          return null;
        }));
      }

      startLatch.countDown();
      for (final Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    }
    finally {
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testExpressionFactorizationDoesNotShareMutableState()
  {
    final OnheapIncrementalIndex.CachingColumnSelectorFactory selectorFactory = makeCachingColumnSelectorFactory();
    final Expr expression = Parser.parse("value + 1", TestExprMacroTable.INSTANCE);

    final ColumnValueSelector<?> firstSelector = ExpressionSelectors.makeExprEvalSelector(selectorFactory, expression);
    final ColumnValueSelector<?> secondSelector = ExpressionSelectors.makeExprEvalSelector(selectorFactory, expression);
    Assertions.assertNotSame(firstSelector, secondSelector);

    final Expr constantExpression = Parser.parse("'constant'", TestExprMacroTable.INSTANCE);
    final ColumnValueSelector<ExprEval> firstConstantSelector =
        ExpressionSelectors.makeExprEvalSelector(selectorFactory, constantExpression);
    final ColumnValueSelector<ExprEval> secondConstantSelector =
        ExpressionSelectors.makeExprEvalSelector(selectorFactory, constantExpression);
    final ColumnValueSelector<ExprEval> thirdConstantSelector =
        ExpressionSelectors.makeExprEvalSelector(selectorFactory, constantExpression);
    Assertions.assertNotSame(firstConstantSelector.getObject(), secondConstantSelector.getObject());
    Assertions.assertNotSame(secondConstantSelector.getObject(), thirdConstantSelector.getObject());

    final AggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
        "sum",
        null,
        "value + 1",
        TestExprMacroTable.INSTANCE
    );
    final Aggregator firstAggregator = aggregatorFactory.factorize(selectorFactory);
    final Aggregator secondAggregator = aggregatorFactory.factorize(selectorFactory);

    Assertions.assertNotSame(firstAggregator, secondAggregator);
    firstAggregator.aggregate();
    Assertions.assertEquals(2L, firstAggregator.getLong());
    Assertions.assertTrue(secondAggregator.isNull());
  }

  @Test
  public void testExpressionIngestionProducesExpectedResults()
  {
    final int rowCount = 10_000;
    final int rollupKeyCount = 100;
    final List<InputRow> rows = new ArrayList<>(rowCount);
    long expectedSum = 0L;
    for (int rowNumber = 0; rowNumber < rowCount; rowNumber++) {
      final long value = rowNumber % 100;
      rows.add(
          new MapBasedInputRow(
              0L,
              Collections.singletonList("key"),
              ImmutableMap.of(
                  "key", "key-" + rowNumber % rollupKeyCount,
                  "value", value
              )
          )
      );
      expectedSum += value + 1;
    }

    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("key"))))
                .withMetrics(
                    new LongSumAggregatorFactory(
                        "sum",
                        null,
                        "value + 1",
                        TestExprMacroTable.INSTANCE
                    )
                )
                .withRollup(true)
                .build()
        )
        .setMaxRowCount(rowCount + 1)
        .build();

    try {
      for (final InputRow row : rows) {
        index.add(row);
      }

      long actualSum = 0L;
      for (final IncrementalIndexRow row : index.getFacts().keySet()) {
        actualSum += index.getMetricLongValue(row.getRowIndex(), 0);
      }

      Assertions.assertEquals(rollupKeyCount, index.numRows());
      Assertions.assertEquals(expectedSum, actualSum);
    }
    finally {
      index.close();
    }
  }

  private static OnheapIncrementalIndex.CachingColumnSelectorFactory makeCachingColumnSelectorFactory()
  {
    final TestObjectColumnSelector<Long> valueSelector = new TestObjectColumnSelector<Long>()
    {
      @Override
      public Class<Long> classOfObject()
      {
        return Long.class;
      }

      @Override
      public Long getObject()
      {
        return 1L;
      }
    };

    final ColumnSelectorFactory delegate = new TestColumnSelectorFactory()
        .addColumnSelector("value", valueSelector)
        .addCapabilities("value", null)
        .addColumnSelector("other", valueSelector)
        .addCapabilities("other", null);
    return new OnheapIncrementalIndex.CachingColumnSelectorFactory(delegate);
  }
}
