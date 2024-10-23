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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScanQueryQueryToolChestTest
{

  static {
    NullHandling.initializeForTests();
    ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);
  }

  // Expected results for the resultsAsArrays test methods.
  private static final List<Object[]> ARRAY_RESULTS_1 = ImmutableList.of(
      new Object[]{null, 3.2},
      new Object[]{"x", "y"}
  );

  private static final List<Object[]> ARRAY_RESULTS_2 = ImmutableList.of(
      new Object[]{"str1", 3.2},
      new Object[]{"str2", 3.3}
  );

  private static final List<Object[]> ARRAY_RESULTS_3 = ImmutableList.of(
      new Object[]{3.4, "str3"},
      new Object[]{3.5, "str4"}
  );

  private final ScanQueryQueryToolChest toolChest = makeTestScanQueryToolChest();

  public static ScanQueryQueryToolChest makeTestScanQueryToolChest()
  {
    return new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance());
  }

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
        toolChest.resultsAsArrays(scanQuery, makeResults1(ScanQuery.ResultFormat.RESULT_FORMAT_LIST))
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
        toolChest.resultsAsArrays(scanQuery, makeResults1(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST))
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
        ARRAY_RESULTS_1,
        toolChest.resultsAsArrays(scanQuery, makeResults1(ScanQuery.ResultFormat.RESULT_FORMAT_LIST))
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
        ARRAY_RESULTS_1,
        toolChest.resultsAsArrays(scanQuery, makeResults1(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST))
    );
  }

  @Test
  public void test_resultsAsFrames_batchingWorksAsExpectedWithDistinctColumnTypes()
  {

    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .columns("foo", "bar", "foo2", "bar2", "foo3", "bar3")
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .build();

    List<FrameSignaturePair> frames =
        toolChest.resultsAsFrames(
            scanQuery,
            Sequences.concat(makeResults1(ScanQuery.ResultFormat.RESULT_FORMAT_LIST), results2(), results3()),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            true
        ).get().toList();


    Assert.assertEquals(3, frames.size());

    RowSignature resultRowSignature = RowSignature.builder()
                                                  .add("foo", null)
                                                  .add("bar", null)
                                                  .add("foo2", null)
                                                  .add("bar2", null)
                                                  .add("foo3", null)
                                                  .add("bar3", null)
                                                  .build();

    Sequence<Object[]> rows = new FrameBasedInlineDataSource(frames, resultRowSignature).getRowsAsSequence();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{null, StructuredData.wrap(3.2), null, null, null, null},
            new Object[]{StructuredData.wrap("x"), StructuredData.wrap("y"), null, null, null, null},
            new Object[]{null, null, "str1", 3.2, null, null},
            new Object[]{null, null, "str2", 3.3, null, null},
            new Object[]{null, null, null, null, 3.4, "str3"},
            new Object[]{null, null, null, null, 3.5, "str4"}
        ),
        rows
    );
  }

  @Test
  public void test_resultsAsFrames_batchingWorksAsExpectedWithMixedColumnTypes()
  {

    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .columns("foo", "bar", "foo2", "bar2", "foo3", "bar3")
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .build();

    List<FrameSignaturePair> frames =
        toolChest.resultsAsFrames(
            scanQuery,
            Sequences.concat(
                results2(),
                makeResults1(ScanQuery.ResultFormat.RESULT_FORMAT_LIST),
                makeResults1(ScanQuery.ResultFormat.RESULT_FORMAT_LIST),
                results3(),
                results2(),
                results2(),
                results3()
            ),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            true
        ).get().toList();


    Assert.assertEquals(5, frames.size());

    RowSignature resultRowSignature = RowSignature.builder()
                                                  .add("foo", null)
                                                  .add("bar", null)
                                                  .add("foo2", null)
                                                  .add("bar2", null)
                                                  .add("foo3", null)
                                                  .add("bar3", null)
                                                  .build();

    Sequence<Object[]> rows = new FrameBasedInlineDataSource(frames, resultRowSignature).getRowsAsSequence();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            // results2
            new Object[]{null, null, "str1", 3.2, null, null},
            new Object[]{null, null, "str2", 3.3, null, null},
            // results1
            new Object[]{null, StructuredData.wrap(3.2), null, null, null, null},
            new Object[]{StructuredData.wrap("x"), StructuredData.wrap("y"), null, null, null, null},
            // results1
            new Object[]{null, StructuredData.wrap(3.2), null, null, null, null},
            new Object[]{StructuredData.wrap("x"), StructuredData.wrap("y"), null, null, null, null},
            // results3
            new Object[]{null, null, null, null, 3.4, "str3"},
            new Object[]{null, null, null, null, 3.5, "str4"},
            // results2
            new Object[]{null, null, "str1", 3.2, null, null},
            new Object[]{null, null, "str2", 3.3, null, null},
            // results2
            new Object[]{null, null, "str1", 3.2, null, null},
            new Object[]{null, null, "str2", 3.3, null, null},
            // results3
            new Object[]{null, null, null, null, 3.4, "str3"},
            new Object[]{null, null, null, null, 3.5, "str4"}
        ),
        rows
    );
  }


  @Test
  public void test_resultsAsFrames_batchingWorksAsExpectedWithSameColumnTypes()
  {

    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .dataSource("foo")
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
              .columns("foo", "bar", "foo2", "bar2", "foo3", "bar3")
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .build();

    List<FrameSignaturePair> frames =
        toolChest.resultsAsFrames(
            scanQuery,
            Sequences.concat(results2(), results2()),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            true
        ).get().toList();


    Assert.assertEquals(1, frames.size());

    RowSignature resultRowSignature = RowSignature.builder()
                                                  .add("foo", null)
                                                  .add("bar", null)
                                                  .add("foo2", null)
                                                  .add("bar2", null)
                                                  .add("foo3", null)
                                                  .add("bar3", null)
                                                  .build();

    Sequence<Object[]> rows = new FrameBasedInlineDataSource(frames, resultRowSignature).getRowsAsSequence();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{null, null, "str1", 3.2, null, null},
            new Object[]{null, null, "str2", 3.3, null, null},
            new Object[]{null, null, "str1", 3.2, null, null},
            new Object[]{null, null, "str2", 3.3, null, null}
        ),
        rows
    );
  }

  /**
   * Returns results that are a single ScanResultValue with two rows, each row having columns "foo" and "bar".
   */
  private static Sequence<ScanResultValue> makeResults1(final ScanQuery.ResultFormat resultFormat)
  {
    final List<Object> rows = new ArrayList<>();

    // Generate rows in the manner of ScanQueryEngine.
    switch (resultFormat) {
      case RESULT_FORMAT_LIST:
        ARRAY_RESULTS_1.forEach(arr -> {
          final Map<String, Object> m = new HashMap<>();
          m.put("foo", arr[0]);
          m.put("bar", arr[1]);
          rows.add(m);
        });
        break;
      case RESULT_FORMAT_COMPACTED_LIST:
        ARRAY_RESULTS_1.forEach(arr -> rows.add(Arrays.asList(arr)));
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

  /**
   * Returns results that are a single ScanResultValue with two rows, each row having columns "foo2" and "bar2". This
   * generates results in the format of {@link ScanQuery.ResultFormat#RESULT_FORMAT_LIST}
   */
  private static Sequence<ScanResultValue> results2()
  {
    final List<Object> rows = new ArrayList<>();

    ARRAY_RESULTS_2.forEach(arr -> {
      final Map<String, Object> m = new HashMap<>();
      m.put("foo2", arr[0]);
      m.put("bar2", arr[1]);
      rows.add(m);
    });

    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add("foo2", ColumnType.STRING);
    rowSignatureBuilder.add("bar2", ColumnType.DOUBLE);

    return Sequences.simple(
        ImmutableList.of(
            new ScanResultValue(
                null,
                ImmutableList.of("foo2", "bar2"),
                rows,
                rowSignatureBuilder.build()
            )
        )
    );
  }

  /**
   * Returns results that are a single ScanResultValue with two rows, each row having columns "foo3" and "bar3". This
   * generates results in the format of {@link ScanQuery.ResultFormat#RESULT_FORMAT_LIST}
   */
  private static Sequence<ScanResultValue> results3()
  {
    final List<Object> rows = new ArrayList<>();

    ARRAY_RESULTS_3.forEach(arr -> {
      final Map<String, Object> m = new HashMap<>();
      m.put("foo3", arr[0]);
      m.put("bar3", arr[1]);
      rows.add(m);
    });

    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add("foo3", ColumnType.DOUBLE);
    rowSignatureBuilder.add("bar3", ColumnType.STRING);

    return Sequences.simple(
        ImmutableList.of(
            new ScanResultValue(
                null,
                ImmutableList.of("foo3", "bar3"),
                rows,
                rowSignatureBuilder.build()
            )
        )
    );
  }
}
