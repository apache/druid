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
import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

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
                                                                        ImmutableList.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withProjections(
                                                      ImmutableList.of(
                                                          new AggregateProjectionSpec(
                                                              "mismatched dims",
                                                              VirtualColumns.EMPTY,
                                                              ImmutableList.of(
                                                                  new LongDimensionSchema("string")
                                                              ),
                                                              null
                                                          )
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
                                                                        ImmutableList.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withProjections(
                                                      ImmutableList.of(
                                                          new AggregateProjectionSpec(
                                                              "sad virtual column",
                                                              VirtualColumns.create(
                                                                  new ExpressionVirtualColumn(
                                                                      "v0",
                                                                      "double",
                                                                      ColumnType.DOUBLE,
                                                                      TestExprMacroTable.INSTANCE
                                                                  )
                                                              ),
                                                              ImmutableList.of(
                                                                  new LongDimensionSchema("long")
                                                              ),
                                                              null
                                                          )
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
                                                                        ImmutableList.of(
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
                                                      ImmutableList.of(
                                                          new AggregateProjectionSpec(
                                                              "mismatched agg",
                                                              VirtualColumns.EMPTY,
                                                              ImmutableList.of(
                                                                  new StringDimensionSchema("string")
                                                              ),
                                                              new AggregatorFactory[] {
                                                                  new LongSumAggregatorFactory("sum_double", "sum_double")
                                                              }
                                                          )
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
                                                                        ImmutableList.of(
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
                                                      ImmutableList.of(
                                                          new AggregateProjectionSpec(
                                                              "renamed agg",
                                                              VirtualColumns.EMPTY,
                                                              ImmutableList.of(
                                                                  new StringDimensionSchema("string")
                                                              ),
                                                              new AggregatorFactory[] {
                                                                  new LongSumAggregatorFactory("sum_long", "long"),
                                                                  new DoubleSumAggregatorFactory("sum_double", "double")
                                                              }
                                                          )
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
                                                                        ImmutableList.of(
                                                                            new StringDimensionSchema("string"),
                                                                            new LongDimensionSchema("long")
                                                                        )
                                                                    )
                                                                    .build()
                                                  )
                                                  .withProjections(
                                                      ImmutableList.of(
                                                          new AggregateProjectionSpec(
                                                              "sad agg virtual column",
                                                              VirtualColumns.create(
                                                                  new ExpressionVirtualColumn(
                                                                      "v0",
                                                                      "long + 100",
                                                                      ColumnType.LONG,
                                                                      TestExprMacroTable.INSTANCE
                                                                  )
                                                              ),
                                                              ImmutableList.of(
                                                                  new LongDimensionSchema("long")
                                                              ),
                                                              new AggregatorFactory[] {
                                                                  new LongSumAggregatorFactory("v0_sum", "v0")
                                                              }
                                                          )
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
}
