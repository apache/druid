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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.lookup.RetainedLookupTestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;

public class TopNLookupLifecycleTest extends InitializedNullHandlingTest
{
  private final RetainedLookupTestHelper lookup = new RetainedLookupTestHelper();
  private final TopNQueryQueryToolChest toolChest = new TopNQueryQueryToolChest(new TopNQueryConfig());
  private final TopNQuery query = new TopNQueryBuilder()
      .dataSource("test")
      .intervals(ImmutableList.of(Intervals.of("2000/2001")))
      .granularity(Granularities.ALL)
      .dimension(lookup.dimensionSpec("dim", "alias"))
      .metric("count")
      .aggregators(new CountAggregatorFactory("count"))
      .threshold(1)
      .build();

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testResultsRemainReadableAfterLookupReleased(final boolean closeEarly) throws IOException
  {
    final Sequence<Result<TopNResultValue>> sequence = run((queryPlus, context) -> results());
    Assertions.assertEquals(1, lookup.getAcquisitions());
    Assertions.assertEquals(0, lookup.getReleases());

    final List<Result<TopNResultValue>> results;
    if (closeEarly) {
      try (final Yielder<Result<TopNResultValue>> yielder = Yielders.each(sequence)) {
        Assertions.assertFalse(yielder.isDone());
        results = ImmutableList.of(yielder.get());
        Assertions.assertEquals(0, lookup.getReleases());
      }
    } else {
      results = sequence.toList();
    }

    Assertions.assertEquals(1, lookup.getReleases());
    final int applications = lookup.getApplications();
    Assertions.assertEquals(results.size(), applications);
    for (final Result<TopNResultValue> result : results) {
      Assertions.assertEquals("value", result.getValue().getValue().get(0).getDimensionValue("alias"));
      Assertions.assertEquals("value", result.getValue().getValue().get(0).getDimensionValue("alias"));
    }
    Assertions.assertEquals(applications, lookup.getApplications());
  }

  @Test
  public void testLookupReleasedOnRunnerFailure()
  {
    final RuntimeException failure = new RuntimeException("runner failed");
    Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> run((queryPlus, context) -> {
      throw failure;
    })));
    Assertions.assertEquals(1, lookup.getAcquisitions());
    Assertions.assertEquals(1, lookup.getReleases());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLookupReleasedOnExtractionFailure(final boolean yielding)
  {
    final RuntimeException failure = new RuntimeException("extraction failed");
    lookup.failOnApply(failure);
    final Sequence<Result<TopNResultValue>> sequence = run((queryPlus, context) -> results());
    Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> {
      if (yielding) {
        try (final Yielder<Result<TopNResultValue>> ignored = Yielders.each(sequence)) {
          Assertions.fail("Extraction must fail");
        }
      } else {
        sequence.toList();
      }
    }));
    Assertions.assertEquals(1, lookup.getReleases());
  }

  private Sequence<Result<TopNResultValue>> run(final QueryRunner<Result<TopNResultValue>> runner)
  {
    return toolChest.postMergeQueryDecoration(runner).run(QueryPlus.wrap(query), ResponseContext.createEmpty());
  }

  private Sequence<Result<TopNResultValue>> results()
  {
    final Result<TopNResultValue> result = new Result<>(
        DateTimes.of("2000"),
        TopNResultValue.create(ImmutableList.of(ImmutableMap.of("alias", "key", "count", 1L)))
    );
    return Sequences.simple(ImmutableList.of(result, result));
  }
}
