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

package org.apache.druid.query.topn;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.nary.TrinaryFn;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class NestedDataTopNQueryTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataTopNQueryTest.class);

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final AggregationTestHelper helper;
  private final TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>> segmentsGenerator;
  private final Closer closer;

  public NestedDataTopNQueryTest(
      TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>> segmentGenerator
  )
  {
    NestedDataModule.registerHandlersAndSerde();
    this.helper = AggregationTestHelper.createTopNQueryAggregationTestHelper(
        NestedDataModule.getJacksonModulesList(),
        tempFolder
    );
    this.segmentsGenerator = segmentGenerator;
    this.closer = Closer.create();
  }

  @Parameterized.Parameters(name = "segments = {0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    final List<TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>>> segmentsGenerators =
        NestedDataTestUtils.getSegmentGenerators();

    for (TrinaryFn<AggregationTestHelper, TemporaryFolder, Closer, List<Segment>> generatorFn : segmentsGenerators) {
      constructors.add(new Object[]{generatorFn});
    }
    return constructors;
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testGroupBySomeField()
  {
    TopNQuery topN = new TopNQueryBuilder().dataSource("test_datasource")
                                           .granularity(Granularities.ALL)
                                           .intervals(Collections.singletonList(Intervals.ETERNITY))
                                           .dimension(DefaultDimensionSpec.of("v0"))
                                           .virtualColumns(new NestedFieldVirtualColumn("nest", "$.x", "v0"))
                                           .aggregators(new CountAggregatorFactory("count"))
                                           .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                                           .threshold(10)
                                           .build();


    Sequence<Result<TopNResultValue>> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder, closer), topN);

    Sequence<Object[]> resultsSeq = new TopNQueryQueryToolChest(new TopNQueryConfig()).resultsAsArrays(topN, seq);

    List<Object[]> results = resultsSeq.toList();

    verifyResults(
        results,
        ImmutableList.of(
            new Object[]{1609459200000L, null, 8L},
            new Object[]{1609459200000L, "100", 2L},
            new Object[]{1609459200000L, "200", 2L},
            new Object[]{1609459200000L, "300", 4L}
        )
    );
  }

  @Test
  public void testGroupBySomeFieldAggregateSomeField()
  {
    TopNQuery topN = new TopNQueryBuilder().dataSource("test_datasource")
                                           .granularity(Granularities.ALL)
                                           .intervals(Collections.singletonList(Intervals.ETERNITY))
                                           .dimension(DefaultDimensionSpec.of("v0"))
                                           .virtualColumns(
                                               new NestedFieldVirtualColumn("nest", "$.x", "v0"),
                                               new NestedFieldVirtualColumn("nest", "$.x", "v1", ColumnType.DOUBLE)
                                           )
                                           .aggregators(new DoubleSumAggregatorFactory("a0", "v1", null, TestExprMacroTable.INSTANCE))
                                           .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                                           .threshold(10)
                                           .build();


    Sequence<Result<TopNResultValue>> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(helper, tempFolder, closer), topN);

    Sequence<Object[]> resultsSeq = new TopNQueryQueryToolChest(new TopNQueryConfig()).resultsAsArrays(topN, seq);

    List<Object[]> results = resultsSeq.toList();

    verifyResults(
        results,
        ImmutableList.of(
            new Object[]{1609459200000L, null, NullHandling.defaultDoubleValue()},
            new Object[]{1609459200000L, "100", 200.0},
            new Object[]{1609459200000L, "200", 400.0},
            new Object[]{1609459200000L, "300", 1200.0}
        )
    );
  }

  private static void verifyResults(List<Object[]> results, List<Object[]> expected)
  {
    Assert.assertEquals(expected.size(), results.size());

    for (int i = 0; i < expected.size(); i++) {
      LOG.info("result #%d, %s", i, Arrays.toString(results.get(i)));
      Assert.assertArrayEquals(
          StringUtils.format("result #%d", i + 1),
          expected.get(i),
          results.get(i)
      );
    }
  }
}
