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

package org.apache.druid.query.groupby;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class GroupByQueryRunnerFactoryTest
{
  @Rule
  public CloserRule closerRule = new CloserRule(true);

  private GroupByQueryRunnerFactory factory;
  private Closer resourceCloser;

  @Before
  public void setup()
  {
    this.resourceCloser = Closer.create();
    final TestGroupByBuffers buffers = resourceCloser.register(TestGroupByBuffers.createDefault());
    this.factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig(), buffers);
  }

  @After
  public void teardown() throws IOException
  {
    factory = null;
    resourceCloser.close();
  }

  @Test
  public void testMergeRunnersEnsureGroupMerging()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(Granularities.ALL)
        .setDimensions(new DefaultDimensionSpec("tags", "tags"))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
          {
            return factory.getToolchest().mergeResults(
                new QueryRunner()
                {
                  @Override
                  public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
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
                      throw new RuntimeException(e);
                    }
                  }
                }
            ).run(queryPlus, responseContext);
          }
        }
    );

    Sequence<ResultRow> result = mergedRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t1", "count", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(query, "1970-01-01T00:00:00.000Z", "tags", "t2", "count", 4L)
    );

    TestHelper.assertExpectedObjects(expectedResults, result.toList(), "");
  }

  private Segment createSegment() throws Exception
  {
    IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(5000)
        .build();

    StringInputRowParser parser = new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("product", "tags"))),
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

    return new IncrementalIndexSegment(incrementalIndex, SegmentId.dummy("test"));
  }
}
