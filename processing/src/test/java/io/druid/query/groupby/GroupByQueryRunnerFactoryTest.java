/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.Row;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.segment.CloserRule;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQueryRunnerFactoryTest
{
  @Rule
  public CloserRule closerRule = new CloserRule(true);

  @Test
  public void testMergeRunnersEnsureGroupMerging() throws Exception
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("tags", "tags")))
        .setAggregatorSpecs(
            Arrays.asList(
                new AggregatorFactory[]
                    {
                        new CountAggregatorFactory("count")
                    }
            )
        )
        .build();

    final QueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig());

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, Map responseContext)
          {
            return factory.getToolchest().mergeResults(
                new QueryRunner()
                {
                  @Override
                  public Sequence run(QueryPlus queryPlus, Map responseContext)
                  {
                    final Query query = queryPlus.getQuery();
                    try {
                      return new MergeSequence(
                          query.getResultOrdering(),
                          Sequences.simple(
                              Arrays.asList(
                                  factory.createRunner(createSegment()).run(queryPlus, responseContext),
                                  factory.createRunner(createSegment()).run(queryPlus, responseContext)
                              )
                          )
                      );
                    }
                    catch (Exception e) {
                      Throwables.propagate(e);
                      return null;
                    }
                  }
                }
            ).run(queryPlus, responseContext);
          }
        }
    );

    Sequence<Row> result = mergedRunner.run(query, Maps.newHashMap());

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "tags", "t2", "count", 4L)
    );

    TestHelper.assertExpectedObjects(expectedResults, Sequences.toList(result, new ArrayList<Row>()), "");
  }

  private Segment createSegment() throws Exception
  {
    IncrementalIndex incrementalIndex = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setConcurrentEventAdd(true)
        .setMaxRowCount(5000)
        .buildOnheap();

    StringInputRowParser parser = new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("product", "tags")), null, null),
            "\t",
            ImmutableList.of("timestamp", "product", "tags"),
            false,
            0
        ),
        "UTF-8"
    );

    String[] rows = new String[]{
        "2011-01-12T00:00:00.000Z,product_1,t1",
        "2011-01-13T00:00:00.000Z,product_2,t2",
        "2011-01-14T00:00:00.000Z,product_3,t2",
    };

    for (String row : rows) {
      incrementalIndex.add(parser.parse(row));
    }

    closerRule.closeLater(incrementalIndex);

    return new IncrementalIndexSegment(incrementalIndex, "test");
  }
}
