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

package org.apache.druid.query.aggregation.exact.cardinality.bitmap64.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.exact.cardinality.bitmap64.Bitmap64ExactCardinalityBuildAggregatorFactory;
import org.apache.druid.query.aggregation.exact.cardinality.bitmap64.Bitmap64ExactCardinalityModule;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.sql.calcite.util.datasets.TestDataSet;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Test;

@SqlTestFrameworkConfig.ComponentSupplier(Bitmap64ExactCardinalitySqlAggregatorTest.Bitmap64ExactCardinalitySqlAggComponentSupplier.class)
public class Bitmap64ExactCardinalitySqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "numfoo";

  public static class Bitmap64ExactCardinalitySqlAggComponentSupplier extends StandardComponentSupplier
  {
    public Bitmap64ExactCardinalitySqlAggComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(super.getCoreModule(), new Bitmap64ExactCardinalityModule());
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      Bitmap64ExactCardinalityModule.registerSerde();

      ObjectMapper jsonMapper = CalciteTests.getJsonMapper();
      new Bitmap64ExactCardinalityModule().getJacksonModules().forEach(jsonMapper::registerModule);

      final QueryableIndex index =
          IndexBuilder.create(jsonMapper)
                      .tmpDir(tempDirProducer.newTempFolder())
                      .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                      .schema(
                          IncrementalIndexSchema.builder()
                              .withDimensionsSpec(TestDataSet.NUMFOO.getInputRowSchema().getDimensionsSpec())
                              .withMetrics(
                                  new Bitmap64ExactCardinalityBuildAggregatorFactory("unique_m1_values", "m1")
                              )
                              .withRollup(false)
                              .build()
                      )
                      .rows(TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS)
                      .buildMMappedIndex();

      return walker.add(
          DataSegment.builder()
                     .dataSource(DATA_SOURCE)
                     .interval(index.getDataInterval())
                     .version("1")
                     .shardSpec(new LinearShardSpec(0))
                     .size(0)
                     .build(),
          index
      );
    }
  }

  @Test
  public void testExactCardinalityOnLongColumn()
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT BITMAP64_EXACT_CARDINALITY(l1) FROM " + DATA_SOURCE)
        .expectedResults(ImmutableList.of(
            new Object[]{3L} // l1 values: 7, 325323, 0 (distinct count = 3)
        ))
        .run();
  }

  @Test
  public void testExactCardinalityOnPreAggregatedColumn()
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT BITMAP64_EXACT_CARDINALITY(unique_m1_values) FROM " + DATA_SOURCE)
        .expectedResults(ImmutableList.of(
            new Object[]{6L} // m1 string inputs: "1.0"-"6.0" are 6 unique values
        ))
        .run();
  }

  @Test
  public void testExactCardinalityWithGroupBy()
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT __time, BITMAP64_EXACT_CARDINALITY(l1) FROM " + DATA_SOURCE + " GROUP BY __time ORDER BY __time")
        .expectedResults(ImmutableList.of(
            new Object[]{946684800000L, 1L}, // 2000-01-01, l1=7L
            new Object[]{946771200000L, 1L}, // 2000-01-02, l1=325323L
            new Object[]{946857600000L, 1L}, // 2000-01-03, l1=0L
            new Object[]{978307200000L, 0L}, // 2001-01-01, l1 is null
            new Object[]{978393600000L, 0L}, // 2001-01-02, l1 is null
            new Object[]{978480000000L, 0L}  // 2001-01-03, l1 is null
        ))
        .run();
  }

  @Test
  public void testExactCardinalityOnPreAggregatedWithGroupBy()
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT __time, BITMAP64_EXACT_CARDINALITY(unique_m1_values) FROM "
             + DATA_SOURCE
             + " GROUP BY __time ORDER BY __time")
        .expectedResults(ImmutableList.of(
            new Object[]{946684800000L, 1L}, // 2000-01-01, m1="1.0"
            new Object[]{946771200000L, 1L}, // 2000-01-02, m1="2.0"
            new Object[]{946857600000L, 1L}, // 2000-01-03, m1="3.0"
            new Object[]{978307200000L, 1L}, // 2001-01-01, m1="4.0"
            new Object[]{978393600000L, 1L}, // 2001-01-02, m1="5.0"
            new Object[]{978480000000L, 1L}  // 2001-01-03, m1="6.0"
        ))
        .run();
  }

  @Test
  public void testExactCardinalityWithFilter()
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT BITMAP64_EXACT_CARDINALITY(l1) FROM " + DATA_SOURCE + " WHERE l1 > 20")
        .expectedResults(ImmutableList.of(
            new Object[]{1L} // l1 values > 20: 325323L (distinct count = 1)
        ))
        .run();
  }
}
