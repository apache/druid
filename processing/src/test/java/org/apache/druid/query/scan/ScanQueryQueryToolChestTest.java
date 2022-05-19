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

package org.apache.druid.query.scan;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScanQueryQueryToolChestTest
{
  // Expected results for the resultsAsArrays test methods.
  private static final List<Object[]> ARRAY_RESULTS = ImmutableList.of(
      new Object[]{null, 3.2},
      new Object[]{"x", "y"}
  );

  private final ScanQueryQueryToolChest toolChest = new ScanQueryQueryToolChest(
      new ScanQueryConfig(),
      new DefaultGenericQueryMetricsFactory()
  );

  @Test
  public void test_resultArraySignature_columnsNotSpecified()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .build();

    Assert.assertEquals(RowSignature.empty(), toolChest.resultArraySignature(scanQuery));
  }

  @Test
  public void test_resultArraySignature_columnsNotSpecifiedLegacyMode()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .legacy(true)
              .build();

    Assert.assertEquals(RowSignature.empty(), toolChest.resultArraySignature(scanQuery));
  }

  @Test
  public void test_resultArraySignature_columnsSpecified()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .columns("foo", "bar")
              .build();

    Assert.assertEquals(
        RowSignature.builder().add("foo", null).add("bar", null).build(),
        toolChest.resultArraySignature(scanQuery)
    );
  }

  @Test
  public void test_resultArraySignature_columnsSpecifiedLegacyMode()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .columns("foo", "bar")
              .legacy(true)
              .build();

    Assert.assertEquals(
        RowSignature.builder().add("timestamp", null).add("foo", null).add("bar", null).build(),
        toolChest.resultArraySignature(scanQuery)
    );
  }

  @Test
  public void test_resultsAsArrays_columnsNotSpecifiedListResults()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(new Object[]{}, new Object[]{}),
        toolChest.resultsAsArrays(scanQuery, makeResults(ScanQuery.ResultFormat.RESULT_FORMAT_LIST))
    );
  }

  @Test
  public void test_resultsAsArrays_columnsNotSpecifiedCompactedListResults()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(new Object[]{}, new Object[]{}),
        toolChest.resultsAsArrays(scanQuery, makeResults(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST))
    );
  }

  @Test
  public void test_resultsAsArrays_columnsSpecifiedListResults()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .columns("foo", "bar")
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ARRAY_RESULTS,
        toolChest.resultsAsArrays(scanQuery, makeResults(ScanQuery.ResultFormat.RESULT_FORMAT_LIST))
    );
  }

  @Test
  public void test_resultsAsArrays_columnsSpecifiedCompactedListResults()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .columns("foo", "bar")
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ARRAY_RESULTS,
        toolChest.resultsAsArrays(scanQuery, makeResults(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST))
    );
  }

  /**
   * Returns results that are a single ScanResultValue with two rows, each row having columns "foo" and "bar".
   */
  private static Sequence<ScanResultValue> makeResults(final ScanQuery.ResultFormat resultFormat)
  {
    final List<Object> rows = new ArrayList<>();

    // Generate rows in the manner of ScanQueryEngine.
    switch (resultFormat) {
      case RESULT_FORMAT_LIST:
        ARRAY_RESULTS.forEach(arr -> {
          final Map<String, Object> m = new HashMap<>();
          m.put("foo", arr[0]);
          m.put("bar", arr[1]);
          rows.add(m);
        });
        break;
      case RESULT_FORMAT_COMPACTED_LIST:
        ARRAY_RESULTS.forEach(arr -> rows.add(Arrays.asList(arr)));
        break;
      default:
        throw new ISE("Cannot generate resultFormat '%s'", resultFormat);
    }

    return Sequences.simple(
        ImmutableList.of(
            new ScanResultValue(
                null,
                ImmutableList.of("foo", "bar"),
                rows
            )
        )
    );
  }
}
