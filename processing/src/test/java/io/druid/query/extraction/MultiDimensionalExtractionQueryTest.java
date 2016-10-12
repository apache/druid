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

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiDimensionalExtractionQueryTest
{
  private AggregationTestHelper helper;

  private static IncrementalIndex incrementalIndex;
  private static QueryableIndex queryableIndex;

  private static File persistedSegmentDir;

  public MultiDimensionalExtractionQueryTest() throws Exception
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        ImmutableList.<Module>of(), null
    );
  }

  @BeforeClass
  public static void setup() throws Exception
  {
    incrementalIndex = new OnheapIncrementalIndex(
        0,
        QueryGranularities.NONE,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count")
        },
        true,
        true,
        true,
        5000
    );

    StringInputRowParser parser = new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3")), null, null),
            "\t",
            ImmutableList.of("timestamp", "dim1", "dim2", "dim3")
        ),
        "UTF-8"
    );

    String[] rows = new String[]{
        "2011-01-12T00:00:00.000Z,A1:,B1,C1",
        "2011-01-13T00:00:00.000Z,A1,:B1,C2",
        "2011-01-14T00:00:00.000Z,A3,B3,C3",
    };

    for (String row : rows) {
      incrementalIndex.add(parser.parse(row));
    }

    persistedSegmentDir = Files.createTempDir();
    TestHelper.getTestIndexMerger()
        .persist(incrementalIndex, persistedSegmentDir, new IndexSpec());

    queryableIndex = TestHelper.getTestIndexIO().loadIndex(persistedSegmentDir);
  }

  @Test
  public void testDimsConcat()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource("xx")
        .setQuerySegmentSpec(new LegacySegmentSpec("1970/3000"))
        .setGranularity(QueryGranularities.ALL)
        .setDimensions(Lists.<DimensionSpec>newArrayList(
            new ExtractionDimensionSpec(
                ImmutableList.of("dim1", "dim2"),
                "concat",
                new DimsConcatExtractionFn(null, ":")
            )
            )
        )
        .setAggregatorSpecs(
            Arrays.asList(
                new AggregatorFactory[]
                    {
                        new CountAggregatorFactory("count")
                    }
            )
        )
        .build();

    Sequence<Row> result = helper.runQueryOnSegmentsObjs(
        ImmutableList.<Segment>of(
            new QueryableIndexSegment("sid1", queryableIndex),
            new IncrementalIndexSegment(incrementalIndex, "sid2")
        ),
        query
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "concat", "A1::B1", "count", 4L),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "concat", "A3:B3", "count", 2L)
    );

    TestHelper.assertExpectedObjects(expectedResults, Sequences.toList(result, new ArrayList<Row>()), "");
  }

  @AfterClass
  public static void cleanup() throws Exception
  {
    queryableIndex.close();
    incrementalIndex.close();
    FileUtils.deleteDirectory(persistedSegmentDir);
  }
}
