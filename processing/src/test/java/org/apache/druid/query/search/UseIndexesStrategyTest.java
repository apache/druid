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

package org.apache.druid.query.search;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.lookup.RetainedLookupTestHelper;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class UseIndexesStrategyTest extends InitializedNullHandlingTest
{
  private final RetainedLookupTestHelper lookup = new RetainedLookupTestHelper();

  @ParameterizedTest
  @CsvSource({"quality, 1", "quality, 1000", "absent, 1000"})
  public void testLookupReleasedAfterSearch(final String dimension, final int limit)
  {
    final Object2IntRBTreeMap<SearchHit> results = executor(dimension).execute(limit);
    Assertions.assertTrue(results.containsKey(new SearchHit("alias", "missing")));
    Assertions.assertEquals(1, lookup.getAcquisitions());
    Assertions.assertEquals(1, lookup.getReleases());
  }

  @Test
  public void testLookupReleasedOnExtractionFailure()
  {
    final RuntimeException failure = new RuntimeException("extraction failed");
    lookup.failOnApply(failure);
    Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> executor("quality").execute(1000)));
    Assertions.assertEquals(1, lookup.getAcquisitions());
    Assertions.assertEquals(1, lookup.getReleases());
  }

  private UseIndexesStrategy.IndexOnlyExecutor executor(final String dimension)
  {
    final SearchQuery query = Druids.newSearchQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                    .dimensions(ImmutableList.of(lookup.dimensionSpec(dimension, "alias")))
                                    .query("missing")
                                    .build();
    return new UseIndexesStrategy.IndexOnlyExecutor(
        query,
        new QueryableIndexSegment(TestIndex.getMMappedTestIndex(), SegmentId.dummy("test")),
        null,
        query.getDimensions()
    );
  }
}
