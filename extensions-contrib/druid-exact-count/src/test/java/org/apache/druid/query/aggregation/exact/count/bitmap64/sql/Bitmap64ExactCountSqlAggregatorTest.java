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

package org.apache.druid.query.aggregation.exact.count.bitmap64.sql;

import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.exact.count.bitmap64.Bitmap64ExactCountBuildAggregatorFactory;
import org.apache.druid.query.aggregation.exact.count.bitmap64.Bitmap64ExactCountMergeAggregatorFactory;
import org.apache.druid.query.aggregation.exact.count.bitmap64.Bitmap64ExactCountModule;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.sql.calcite.util.datasets.TestDataSet;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

@SqlTestFrameworkConfig.ComponentSupplier(Bitmap64ExactCountSqlAggregatorTest.Bitmap64ExactCountSqlAggComponentSupplier.class)
public class Bitmap64ExactCountSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "numfoo";

  public static class Bitmap64ExactCountSqlAggComponentSupplier extends StandardComponentSupplier
  {
    public Bitmap64ExactCountSqlAggComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(super.getCoreModule(), new Bitmap64ExactCountModule());
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      Bitmap64ExactCountModule.registerSerde();
      final QueryableIndex index =
          IndexBuilder.create(CalciteTests.getJsonMapper())
                      .tmpDir(tempDirProducer.newTempFolder())
                      .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                      .schema(
                          IncrementalIndexSchema.builder()
                                                .withDimensionsSpec(TestDataSet.NUMFOO.getInputRowSchema()
                                                                                      .getDimensionsSpec())
                                                .withMetrics(
                                                    new Bitmap64ExactCountBuildAggregatorFactory(
                                                        "unique_m1_values",
                                                        "m1"
                                                    ),
                                                    new HyperUniquesAggregatorFactory(
                                                        "hyper_unique_m1",
                                                        "m1"
                                                    )
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
  public void testExactCountOnStringColumnTypeThrowsError()
  {
    cannotVectorize();
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> testQuery(
            "SELECT BITMAP64_EXACT_COUNT(dim1) FROM " + DATA_SOURCE,
            ImmutableList.of(),
            ImmutableList.of()
        )
    );

    Assertions.assertTrue(e.getMessage().contains("Aggregation [BITMAP64_EXACT_COUNT] does not support type [VARCHAR]"));
  }

  @Test
  public void testExactCountOnHyperUniqueColumnTypeThrowsError()
  {
    DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> testQuery(
            "SELECT BITMAP64_EXACT_COUNT(hyper_unique_m1) FROM " + DATA_SOURCE,
            ImmutableList.of(),
            ImmutableList.of()
        )
    );

    Assertions.assertTrue(e.getMessage().contains(
        "Cannot apply 'BITMAP64_EXACT_COUNT' to arguments of type 'BITMAP64_EXACT_COUNT(<COMPLEX<HYPERUNIQUE>>)'"));
  }

  @Test
  public void testExactCountOnPreAggregatedColumn()
  {
    cannotVectorize();
    String sql = "SELECT BITMAP64_EXACT_COUNT(unique_m1_values) FROM " + DATA_SOURCE;
    testQuery(
        sql,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new Bitmap64ExactCountMergeAggregatorFactory(
                              "a0",
                              "unique_m1_values"
                          )
                      )
                  )
                  .context(Collections.emptyMap())
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L} // m1 string inputs: "1.0"-"6.0" are 6 unique values
        )
    );
  }

  @Test
  public void testExactCountWithGroupBy()
  {
    cannotVectorize();
    String sql = "SELECT __time, BITMAP64_EXACT_COUNT(l1) FROM " + DATA_SOURCE + " "
                 + "GROUP BY __time "
                 + "ORDER BY __time";
    testQuery(
        sql,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(ImmutableList.of(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(
                            ImmutableList.of(
                                new Bitmap64ExactCountBuildAggregatorFactory("a0", "l1")
                            )
                        )
                        .setContext(Collections.emptyMap())
                        .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1L}, // 2000-01-01, l1=7L
            new Object[]{946771200000L, 1L}, // 2000-01-02, l1=325323L
            new Object[]{946857600000L, 1L}, // 2000-01-03, l1=0L
            new Object[]{978307200000L, 0L}, // 2001-01-01, l1 is null
            new Object[]{978393600000L, 0L}, // 2001-01-02, l1 is null
            new Object[]{978480000000L, 0L}  // 2001-01-03, l1 is null
        )
    );
  }

  @Test
  public void testExactCountOnPreAggregatedWithGroupBy()
  {
    cannotVectorize();
    String sql = "SELECT __time, BITMAP64_EXACT_COUNT(unique_m1_values) FROM " + DATA_SOURCE + " "
                 + "GROUP BY __time "
                 + "ORDER BY __time";

    testQuery(
        sql,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(ImmutableList.of(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                        .setAggregatorSpecs(
                            ImmutableList.of(
                                new Bitmap64ExactCountMergeAggregatorFactory("a0", "unique_m1_values")
                            )
                        )
                        .setContext(Collections.emptyMap())
                        .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1L}, // 2000-01-01, m1="1.0"
            new Object[]{946771200000L, 1L}, // 2000-01-02, m1="2.0"
            new Object[]{946857600000L, 1L}, // 2000-01-03, m1="3.0"
            new Object[]{978307200000L, 1L}, // 2001-01-01, m1="4.0"
            new Object[]{978393600000L, 1L}, // 2001-01-02, m1="5.0"
            new Object[]{978480000000L, 1L}  // 2001-01-03, m1="6.0"
        )
    );
  }
}
