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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.FluentQueryRunner;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.LookupDimensionSpec;
import org.apache.druid.query.lookup.RetainedLookupTestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

public class GroupByLookupLifecycleTest extends InitializedNullHandlingTest
{
  private final RetainedLookupTestHelper lookup = new RetainedLookupTestHelper();
  private final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(null, null);
  private final GroupByQuery query = GroupByQuery.builder()
      .setDataSource("test")
      .setInterval("2000/2001")
      .setGranularity(Granularities.ALL)
      .setDimensions(lookup.dimensionSpec("dim", "alias"))
      .build();

  @ParameterizedTest
  @CsvSource({"false, false", "false, true", "true, false", "true, true"})
  public void testLookupReleasedAfterResults(final boolean bySegment, final boolean shouldFinalize)
  {
    final GroupByQuery queryToRun = query.withOverriddenContext(
        ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, bySegment, QueryContexts.FINALIZE_KEY, shouldFinalize)
    );
    final QueryRunner<ResultRow> baseRunner = new BySegmentQueryRunner<>(
        SegmentId.dummy("test"),
        DateTimes.of("2000"),
        (queryPlus, context) -> results()
    );
    final Sequence<ResultRow> sequence = run(queryToRun, baseRunner);
    Assertions.assertEquals(1, lookup.getAcquisitions());
    Assertions.assertEquals(0, lookup.getReleases());
    final List<?> results = sequence.toList();
    Assertions.assertEquals(1, lookup.getReleases());
    Assertions.assertEquals(2, lookup.getApplications());
    final List<?> rows;
    if (bySegment) {
      rows = ((BySegmentResultValue<?>) ((Result<?>) results.get(0)).getValue()).getResults();
    } else {
      rows = results;
    }
    for (final Object row : rows) {
      Assertions.assertEquals("value", ((ResultRow) row).get(query.getResultRowDimensionStart()));
    }
  }

  @Test
  public void testLookupReleasedOnEarlyClose() throws IOException
  {
    final Sequence<ResultRow> sequence = run(query, (queryPlus, context) -> results());
    final ResultRow result;
    try (final Yielder<ResultRow> yielder = Yielders.each(sequence)) {
      Assertions.assertFalse(yielder.isDone());
      result = yielder.get();
      Assertions.assertEquals(0, lookup.getReleases());
    }
    Assertions.assertEquals(1, lookup.getReleases());
    Assertions.assertEquals("value", result.get(query.getResultRowDimensionStart()));
  }

  @Test
  public void testLookupReleasedOnRunnerFailure()
  {
    final RuntimeException failure = new RuntimeException("runner failed");
    Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> run(query, (queryPlus, context) -> {
      throw failure;
    })));
    Assertions.assertEquals(1, lookup.getAcquisitions());
    Assertions.assertEquals(1, lookup.getReleases());
  }

  @Test
  public void testLookupReleasedOnPartialFinalizerConstruction()
  {
    final RuntimeException failure = new RuntimeException("second lookup failed");
    final LookupDimensionSpec failingDimension = Mockito.spy(lookup.dimensionSpec("other", "other"));
    Mockito.doThrow(failure).when(failingDimension).getExtractionFn(Mockito.any(Closer.class));
    final GroupByQuery queryToRun = new GroupByQuery.Builder(query)
        .setDimensions(lookup.dimensionSpec("dim", "alias"), failingDimension)
        .build();
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(RuntimeException.class, () -> run(queryToRun, (queryPlus, context) -> results()))
    );
    Assertions.assertEquals(1, lookup.getAcquisitions());
    Assertions.assertEquals(1, lookup.getReleases());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLookupReleasedOnExtractionFailure(final boolean yielding)
  {
    final RuntimeException failure = new RuntimeException("extraction failed");
    lookup.failOnApply(failure);
    final Sequence<ResultRow> sequence = run(query, (queryPlus, context) -> results());
    Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> {
      if (yielding) {
        try (final Yielder<ResultRow> ignored = Yielders.each(sequence)) {
          Assertions.fail("Extraction must fail");
        }
      } else {
        sequence.toList();
      }
    }));
    Assertions.assertEquals(1, lookup.getReleases());
  }

  private Sequence<ResultRow> run(final GroupByQuery queryToRun, final QueryRunner<ResultRow> runner)
  {
    return FluentQueryRunner.create(runner, toolChest)
                            .applyPostMergeDecoration()
                            .run(QueryPlus.wrap(queryToRun), ResponseContext.createEmpty());
  }

  private Sequence<ResultRow> results()
  {
    final ResultRow row = ResultRow.of("key");
    return Sequences.simple(ImmutableList.of(row, row));
  }
}
