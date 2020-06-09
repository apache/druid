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

package org.apache.druid.query.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class SegmentMetadataUnionQueryTest extends InitializedNullHandlingTest
{
  static {
    NullHandling.initializeForTests();
  }

  private static final QueryRunnerFactory FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );
  private final QueryRunner runner;
  private final boolean mmap;

  public SegmentMetadataUnionQueryTest(
      QueryRunner runner,
      boolean mmap
  )
  {
    this.runner = runner;
    this.mmap = mmap;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{
            QueryRunnerTestHelper.makeUnionQueryRunner(
                FACTORY,
                new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), QueryRunnerTestHelper.SEGMENT_ID),
                null
            ),
            true,
        },
        new Object[]{
            QueryRunnerTestHelper.makeUnionQueryRunner(
                FACTORY,
                new IncrementalIndexSegment(TestIndex.getIncrementalTestIndex(), QueryRunnerTestHelper.SEGMENT_ID),
                null
            ),
            false
        }
    );
  }


  @Test
  public void testSegmentMetadataUnionQuery()
  {
    SegmentAnalysis expected = new SegmentAnalysis(
        QueryRunnerTestHelper.SEGMENT_ID.toString(),
        Collections.singletonList(Intervals.of("2011-01-12T00:00:00.000Z/2011-04-15T00:00:00.001Z")),
        ImmutableMap.of(
            "placement",
            new ColumnAnalysis(
                ValueType.STRING.toString(),
                false,
                mmap ? 43524 : 43056,
                1,
                "preferred",
                "preferred",
                null
            )
        ),
        mmap ? 800544 : 803324,
        4836,
        null,
        null,
        null,
        null
    );
    SegmentMetadataQuery query = new Druids.SegmentMetadataQueryBuilder()
        .dataSource(QueryRunnerTestHelper.UNION_DATA_SOURCE)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .toInclude(new ListColumnIncluderator(Collections.singletonList("placement")))
        .analysisTypes(
            SegmentMetadataQuery.AnalysisType.CARDINALITY,
            SegmentMetadataQuery.AnalysisType.SIZE,
            SegmentMetadataQuery.AnalysisType.INTERVAL,
            SegmentMetadataQuery.AnalysisType.MINMAX
        )
        .build();
    List result = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedObjects(ImmutableList.of(expected), result, "failed SegmentMetadata union query");
  }


}
