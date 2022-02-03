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

package org.apache.druid.query.aggregation.collectset;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CollectSetGroupByQueryTest
{
  private GroupByQueryRunnerFactory factory;
  private Closer resourceCloser;

  @Before
  public void setup()
  {
    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);
    final Pair<GroupByQueryRunnerFactory, Closer> factoryCloserPair = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        config
    );
    factory = factoryCloserPair.lhs;
    resourceCloser = factoryCloserPair.rhs;
  }

  @After
  public void teardown() throws IOException
  {
    resourceCloser.close();
  }

  @Test
  public void testGroupByWithCollectSetAgg() throws Exception
  {
    IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.SECOND)
                .build()
        )
        .setConcurrentEventAdd(true)
        .setMaxRowCount(1000)
        .buildOnheap();

    for (InputRow inputRow : CollectSetTestHelper.INPUT_ROWS) {
      index.add(inputRow);
    }

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimensions(
            new DefaultDimensionSpec(
                CollectSetTestHelper.DIMENSIONS[0],
                CollectSetTestHelper.DIMENSIONS[0]
            ),
            new DefaultDimensionSpec(
                CollectSetTestHelper.DIMENSIONS[1],
                CollectSetTestHelper.DIMENSIONS[1]
            )
        )
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Arrays.asList(
                    new OrderByColumnSpec(CollectSetTestHelper.DIMENSIONS[0], OrderByColumnSpec.Direction.ASCENDING),
                    new OrderByColumnSpec(CollectSetTestHelper.DIMENSIONS[1], OrderByColumnSpec.Direction.ASCENDING)
                ),
                10
            )
        )
        .setAggregatorSpecs(new CollectSetAggregatorFactory(CollectSetTestHelper.DIMENSIONS[2], CollectSetTestHelper.DIMENSIONS[2], null),
                            new CollectSetAggregatorFactory(CollectSetTestHelper.DIMENSIONS[3], CollectSetTestHelper.DIMENSIONS[3], null),
                            new CollectSetAggregatorFactory(CollectSetTestHelper.DIMENSIONS[3] + "a", CollectSetTestHelper.DIMENSIONS[3], 3))
        .build();

    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        factory.createRunner(incrementalIndexSegment),
        query
    );

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            CollectSetTestHelper.DIMENSIONS[0], "0",
            CollectSetTestHelper.DIMENSIONS[1], "android",
            CollectSetTestHelper.DIMENSIONS[2], Sets.newHashSet("image"),
            CollectSetTestHelper.DIMENSIONS[3], Sets.newHashSet("tag1", "tag4", "tag5", "tag6"),
            CollectSetTestHelper.DIMENSIONS[3] + "a", Sets.newHashSet("tag1", "tag4", "tag5")
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            CollectSetTestHelper.DIMENSIONS[0], "0",
            CollectSetTestHelper.DIMENSIONS[1], "iphone",
            CollectSetTestHelper.DIMENSIONS[2], Sets.newHashSet("video", "text"),
            CollectSetTestHelper.DIMENSIONS[3], Sets.newHashSet("tag1", "tag2", "tag3", "tag4", "tag5", "tag7", "tag8"),
            CollectSetTestHelper.DIMENSIONS[3] + "a", Sets.newHashSet("tag4", "tag5", "tag7")
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            CollectSetTestHelper.DIMENSIONS[0], "1",
            CollectSetTestHelper.DIMENSIONS[1], "iphone",
            CollectSetTestHelper.DIMENSIONS[2], Sets.newHashSet("video"),
            CollectSetTestHelper.DIMENSIONS[3], ImmutableSet.of(),
            CollectSetTestHelper.DIMENSIONS[3] + "a", ImmutableSet.of()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            CollectSetTestHelper.DIMENSIONS[0], "2",
            CollectSetTestHelper.DIMENSIONS[1], "android",
            CollectSetTestHelper.DIMENSIONS[2], Sets.newHashSet("video"),
            CollectSetTestHelper.DIMENSIONS[3], Sets.newHashSet("tag2"),
            CollectSetTestHelper.DIMENSIONS[3] + "a", Sets.newHashSet("tag2")
        )
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "collectset");
  }
}
