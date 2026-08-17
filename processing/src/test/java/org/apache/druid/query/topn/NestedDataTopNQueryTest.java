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
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
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
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

@ParameterizedClass

@MethodSource("constructorFeeder")
public class NestedDataTopNQueryTest extends InitializedNullHandlingTest
{
  private static final Logger LOG = new Logger(NestedDataTopNQueryTest.class);

  @RegisterExtension
  public final TemporaryFolderExtension tempFolder = new TemporaryFolderExtension();

  private final AggregationTestHelper helper;
  private final BiFunction<TemporaryFolderExtension, Closer, List<Segment>> segmentsGenerator;
  private final Closer closer;

  public NestedDataTopNQueryTest(
      BiFunction<TemporaryFolderExtension, Closer, List<Segment>> segmentGenerator
  )
  {
    BuiltInTypesModule.registerHandlersAndSerde();
    this.helper = AggregationTestHelper.createTopNQueryAggregationTestHelperWithTemporaryFolderExtension(
        BuiltInTypesModule.getJacksonModulesList(),
        tempFolder
    );
    this.segmentsGenerator = segmentGenerator;
    this.closer = Closer.create();
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    final List<BiFunction<TemporaryFolderExtension, Closer, List<Segment>>> segmentsGenerators =
        NestedDataTestUtils.getSegmentGeneratorsWithTempDir(NestedDataTestUtils.SIMPLE_DATA_FILE);

    for (BiFunction<TemporaryFolderExtension, Closer, List<Segment>> generatorFn : segmentsGenerators) {
      constructors.add(new Object[]{generatorFn});
    }
    return constructors;
  }

  @AfterEach
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


    Sequence<Result<TopNResultValue>> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(tempFolder, closer), topN);

    Sequence<Object[]> resultsSeq = new TopNQueryQueryToolChest(new TopNQueryConfig()).resultsAsArrays(topN, seq);

    List<Object[]> results = resultsSeq.toList();

    verifyResults(
        results,
        ImmutableList.of(
            new Object[]{1672531200000L, null, 8L},
            new Object[]{1672531200000L, "100", 2L},
            new Object[]{1672531200000L, "200", 2L},
            new Object[]{1672531200000L, "300", 4L}
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


    Sequence<Result<TopNResultValue>> seq = helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(tempFolder, closer), topN);

    Sequence<Object[]> resultsSeq = new TopNQueryQueryToolChest(new TopNQueryConfig()).resultsAsArrays(topN, seq);

    List<Object[]> results = resultsSeq.toList();

    verifyResults(
        results,
        ImmutableList.of(
            new Object[]{1672531200000L, null, null},
            new Object[]{1672531200000L, "100", 200.0},
            new Object[]{1672531200000L, "200", 400.0},
            new Object[]{1672531200000L, "300", 1200.0}
        )
    );
  }

  private static void verifyResults(List<Object[]> results, List<Object[]> expected)
  {
    Assertions.assertEquals(expected.size(), results.size());

    for (int i = 0; i < expected.size(); i++) {
      LOG.info("result #%d, %s", i, Arrays.toString(results.get(i)));
      Assertions.assertArrayEquals(
          expected.get(i),
          results.get(i),
          StringUtils.format("result #%d", i + 1)
      );
    }
  }
}
