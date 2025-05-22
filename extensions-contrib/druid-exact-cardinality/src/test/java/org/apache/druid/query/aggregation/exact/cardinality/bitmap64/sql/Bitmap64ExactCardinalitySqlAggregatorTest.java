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

import com.google.common.collect.ImmutableList;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.exact.cardinality.bitmap64.Bitmap64ExactCardinalityBuildAggregatorFactory;
import org.apache.druid.query.aggregation.exact.cardinality.bitmap64.Bitmap64ExactCardinalityModule;
import org.apache.druid.query.aggregation.exact.cardinality.bitmap64.sql.Bitmap64ExactCardinalitySqlAggregatorTest.Bitmap64ExactCardinalitySqlAggComponentSupplier;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Test;

@SqlTestFrameworkConfig.ComponentSupplier(Bitmap64ExactCardinalitySqlAggComponentSupplier.class)
public class Bitmap64ExactCardinalitySqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "numfoo";

  static class Bitmap64ExactCardinalitySqlAggComponentSupplier extends StandardComponentSupplier
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
      final QueryableIndex index =
          IndexBuilder.create()
                      .tmpDir(tempDirProducer.newTempFolder())
                      .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                      .schema(new IncrementalIndexSchema.Builder()
                                  .withMetrics(
                                      new Bitmap64ExactCardinalityBuildAggregatorFactory(
                                          "unique_m1_values",
                                          "m1"
                                      )
                                  )
                                  .withRollup(false)
                                  .build())
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
    testBuilder()
        .sql("SELECT BITMAP64_EXACT_CARDINALITY(m1) FROM " + DATA_SOURCE)
        .expectedResults(ImmutableList.of(
            new Object[]{4L}
        ))
        .run();
  }

  @Test
  public void testExactCardinalityOnPreAggregatedColumn()
  {
    testBuilder()
        .sql("SELECT BITMAP64_EXACT_CARDINALITY(unique_m1_values) FROM " + DATA_SOURCE)
        .expectedResults(ImmutableList.of(
            new Object[]{4L} // Expecting 4, as 10, 20, 30, 40 are the unique values
        ))
        .run();
  }

  @Test
  public void testExactCardinalityWithGroupBy()
  {
    testBuilder()
        .sql("SELECT __time, BITMAP64_EXACT_CARDINALITY(m1) FROM " + DATA_SOURCE + " GROUP BY __time ORDER BY __time")
        .expectedResults(ImmutableList.of(
            new Object[]{1466985600000L, 3L},
            new Object[]{1467072000000L, 2L}
        ))
        .run();
  }

  @Test
  public void testExactCardinalityOnPreAggregatedWithGroupBy()
  {
    testBuilder()
        .sql("SELECT __time, BITMAP64_EXACT_CARDINALITY(unique_m1_values) FROM "
             + DATA_SOURCE
             + " GROUP BY __time ORDER BY __time")
        .expectedResults(ImmutableList.of(
            new Object[]{1466985600000L, 3L},
            new Object[]{1467072000000L, 2L}
        ))
        .run();
  }

  @Test
  public void testExactCardinalityWithFilter()
  {
    testBuilder()
        .sql("SELECT BITMAP64_EXACT_CARDINALITY(m1) FROM " + DATA_SOURCE + " WHERE m1 > 20")
        .expectedResults(ImmutableList.of(
            new Object[]{2L} // 30, 40
        ))
        .run();
  }
}
