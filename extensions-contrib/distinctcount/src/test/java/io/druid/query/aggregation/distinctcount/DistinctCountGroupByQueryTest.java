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

package io.druid.query.aggregation.distinctcount;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.common.config.NullHandling;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class DistinctCountGroupByQueryTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder() throws IOException
  {
    final List<Object[]> constructors = Lists.newArrayList();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }

    return constructors;
  }

  private GroupByQueryConfig config;

  public DistinctCountGroupByQueryTest(
      GroupByQueryConfig config
  )
  {
    this.config = config;
  }

  @Test
  public void testGroupByWithDistinctCountAgg() throws Exception
  {
    // groupBy V2 skips adding null entries while doing serde.
    boolean nullsInResult = config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2);
    config.setMaxIntermediateRows(10000);
    final GroupByQueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(config);

    IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.SECOND)
                .withMetrics(new CountAggregatorFactory("cnt"))
                .build()
        )
        .setConcurrentEventAdd(true)
        .setMaxRowCount(1000)
        .buildOnheap();

    String visitor_id = "visitor_id";
    String client_type = "client_type";
    long timestamp = System.currentTimeMillis();
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "0", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 1,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "1", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 2,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "2", client_type, "android")
        )
    );
    // Row with Null values
    index.add(
        new MapBasedInputRow(
            timestamp + 2,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of()
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 2,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "2")
        )
    );
    // Row with empty values
    index.add(
        new MapBasedInputRow(
            timestamp + 2,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "", client_type, "")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 2,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.<String, Object>of(visitor_id, "4", client_type, "")
        )
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    client_type,
                    client_type
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        client_type,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 10
            )
        )
        .setAggregatorSpecs(
            Lists.newArrayList(
                QueryRunnerTestHelper.rowsCount,
                new DistinctCountAggregatorFactory("UV", visitor_id, null)
            )
        )
        .build();
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(
        factory,
        factory.createRunner(incrementalIndexSegment),
        query
    );

    List<Row> expectedResults;
    if (NullHandling.useDefaultValuesForNull()) {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              client_type, "iphone",
              "UV", 2L,
              "rows", 2L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              client_type, "android",
              "UV", 1L,
              "rows", 1L
          ),
          // Both rows with null and empty string gets rolled up so rowCount is 3
          nullsInResult ?
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              "UV", 3L,
              "rows", 3L
          ) :
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              "client_type", null,
              "UV", 3L,
              "rows", 3L
          )
      );
    } else {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              client_type, "iphone",
              "UV", 2L,
              "rows", 2L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              client_type, "android",
              "UV", 1L,
              "rows", 1L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              client_type, "",
              "UV", 2L,
              "rows", 2L
          ),
          // client_type is null is this row
          nullsInResult ?
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              "UV", 1L,
              "rows", 2L
          ) :
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "1970-01-01T00:00:00.000Z",
              "client_type", null,
              "UV", 1L,
              "rows", 2L
          )
      );
    }
    TestHelper.assertExpectedObjects(expectedResults, results, "distinct-count");
  }
}
