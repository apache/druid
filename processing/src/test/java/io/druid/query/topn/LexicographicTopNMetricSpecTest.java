/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.topn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.LegacyDataSource;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.LegacyDimensionSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.query.ordering.StringComparatorsTest;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.CloserRule;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

@RunWith(Parameterized.class)
public class LexicographicTopNMetricSpecTest
{
  @Parameterized.Parameters
  public static Iterable<?> constructorFeeder() throws IOException
  {
    return IncrementalIndexTest.constructorFeeder();
  }

  private final IncrementalIndexTest.IndexCreator indexCreator;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public CloserRule closerRule = new CloserRule(true);

  private IncrementalIndex index;

  public LexicographicTopNMetricSpecTest(IncrementalIndexTest.IndexCreator indexCreator)
  {
    this.indexCreator = indexCreator;
  }

  @Before
  public void setUp()
  {
    index = closerRule.closeLater(indexCreator.createIndex());
  }

  @Test
  public void testCrazyUnicode() throws Exception
  {
    final List<String> strings = new ArrayList<>(new HashSet<>(StringComparatorsTest.STRINGS));
    final Random random = new Random(5432160L);
    Collections.shuffle(strings, random);
    final List<String> dimensions = Collections.singletonList("dim1");
    for (final String str : strings) {
      if (str.isEmpty()) {
        continue;
      }
      index.add(new MapBasedInputRow(0, dimensions, ImmutableMap.<String, Object>of(
          "dim1",
          str
      )));
    }

    Collections.sort(strings, StringComparators.LEXICOGRAPHIC);

    final IndexIO indexIO = new IndexIO(new DefaultObjectMapper(), new ColumnConfig()
    {
      @Override
      public int columnCacheSizeBytes()
      {
        return 0;
      }
    });
    final IndexMerger indexMerger = new IndexMerger(new DefaultObjectMapper(), indexIO);
    File outDir = indexMerger.persist(index, temporaryFolder.newFolder(), new IndexSpec());
    final QueryableIndex queryableIndex = closerRule.closeLater(indexIO.loadIndex(outDir));
    final String segmentId = "segmentID";
    final QueryableIndexSegment segment = new QueryableIndexSegment(segmentId, queryableIndex);

    TopNQueryConfig config = new TopNQueryConfig();
    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(
        config,
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        TestQueryRunners.getPool(),
        chest,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    QueryRunner<Result<TopNResultValue>> runner = QueryRunnerTestHelper.makeQueryRunner(
        factory,
        segment
    );
    for (int i = 0; i < strings.size() - 1; ++i) {
      final String failString = String.format("Failed on %d [%s] vs [%s]", i, strings.get(i), strings.get(i + 1));
      final TopNQuery topNQuery = new TopNQuery(
          new LegacyDataSource("datasource"),
          new LegacyDimensionSpec("dim1"),
          new LexicographicTopNMetricSpec(strings.get(i)),
          1,
          new MultipleIntervalSegmentSpec(ImmutableList.of(segment.getDataInterval())),
          null,
          QueryGranularity.ALL,
          ImmutableList.<AggregatorFactory>of(new CountAggregatorFactory("cnt")),
          null,
          null
      );
      final Map<String, Object> context = new HashMap<>();
      final Sequence<Result<TopNResultValue>> resultSequence = runner.run(topNQuery, context);
      final List<Result<TopNResultValue>> results = Sequences.toList(
          resultSequence,
          new ArrayList<Result<TopNResultValue>>()
      );
      Assert.assertEquals(failString, 1, results.size());
      final TopNResultValue resultValue = results.get(0).getValue();
      Assert.assertEquals(failString, 1, resultValue.getValue().size());
      final DimensionAndMetricValueExtractor valueExtractor = resultValue.getValue().get(0);
      Assert.assertEquals(failString, strings.get(i + 1), valueExtractor.getStringDimensionValue("dim1"));
      Assert.assertEquals(failString, 1, valueExtractor.getLongMetric("cnt").intValue());
    }
  }
}