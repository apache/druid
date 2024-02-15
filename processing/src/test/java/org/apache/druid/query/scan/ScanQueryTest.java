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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class ScanQueryTest
{
  private static QuerySegmentSpec intervalSpec;
  private static ScanResultValue s1;
  private static ScanResultValue s2;
  private static ScanResultValue s3;

  @BeforeClass
  public static void setup()
  {
    intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(
            new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-01").plusHours(1))
        )
    );

    ArrayList<HashMap<String, Object>> events1 = new ArrayList<>();
    HashMap<String, Object> event1 = new HashMap<>();
    event1.put(ColumnHolder.TIME_COLUMN_NAME, new Long(42));
    events1.add(event1);

    s1 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events1
    );

    ArrayList<HashMap<String, Object>> events2 = new ArrayList<>();
    HashMap<String, Object> event2 = new HashMap<>();
    event2.put(ColumnHolder.TIME_COLUMN_NAME, new Long(43));
    events2.add(event2);

    s2 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events2
    );

    // ScanResultValue s3 has no time column
    ArrayList<HashMap<String, Object>> events3 = new ArrayList<>();
    HashMap<String, Object> event3 = new HashMap<>();
    event3.put("yah", "yeet");
    events3.add(event3);

    s3 = new ScanResultValue(
        "segmentId",
        Collections.singletonList("yah"),
        events3
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAscendingScanQueryWithInvalidColumns()
  {
    Druids.newScanQueryBuilder()
          .order(ScanQuery.Order.ASCENDING)
          .columns(ImmutableList.of("not time", "also not time"))
          .dataSource("source")
          .intervals(intervalSpec)
          .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDescendingScanQueryWithInvalidColumns()
  {
    Druids.newScanQueryBuilder()
          .order(ScanQuery.Order.DESCENDING)
          .columns(ImmutableList.of("not time", "also not time"))
          .dataSource("source")
          .intervals(intervalSpec)
          .build();
  }

  @Test
  public void testConflictingOrderByAndTimeOrder()
  {
    Assert.assertThrows(
        "Cannot provide 'order' incompatible with 'orderBy'",
        IllegalArgumentException.class,
        () ->
            Druids.newScanQueryBuilder()
                  .order(ScanQuery.Order.ASCENDING)
                  .orderBy(
                      // Not ok, even though it starts with __time ASC, because it also has non-time component.
                      ImmutableList.of(
                          new ScanQuery.OrderBy("__time", ScanQuery.Order.ASCENDING),
                          new ScanQuery.OrderBy("quality", ScanQuery.Order.DESCENDING)
                      )
                  )
                  .columns(ImmutableList.of("__time", "quality"))
                  .dataSource("source")
                  .intervals(intervalSpec)
                  .build()
    );
  }

  @Test
  public void testCompatibleOrderByAndTimeOrder()
  {
    Assert.assertNotNull(
        Druids.newScanQueryBuilder()
              .order(ScanQuery.Order.ASCENDING)
              .orderBy(ImmutableList.of(new ScanQuery.OrderBy("__time", ScanQuery.Order.ASCENDING)))
              .columns(ImmutableList.of("__time", "quality"))
              .dataSource("source")
              .intervals(intervalSpec)
              .build()
    );
  }

  // No assertions because we're checking that no IllegalArgumentExceptions are thrown
  @Test
  public void testValidScanQueryInitialization()
  {
    List<ScanQuery.Order> nonOrderedOrders = Arrays.asList(null, ScanQuery.Order.NONE);

    for (ScanQuery.Order order : nonOrderedOrders) {
      Druids.newScanQueryBuilder()
            .order(order)
            .columns(ImmutableList.of("not time"))
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .order(order)
            .dataSource("source")
            .intervals(intervalSpec)
            .build();


      Druids.newScanQueryBuilder()
            .order(order)
            .columns(ImmutableList.of())
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .order(order)
            .columns(ImmutableList.of("__time"))
            .dataSource("source")
            .intervals(intervalSpec)
            .build();
    }

    Set<ScanQuery.Order> orderedOrders = ImmutableSet.of(ScanQuery.Order.ASCENDING, ScanQuery.Order.DESCENDING);

    for (ScanQuery.Order order : orderedOrders) {
      Druids.newScanQueryBuilder()
            .order(order)
            .columns((List<String>) null)
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .order(order)
            .columns(ImmutableList.of())
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .order(order)
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .order(order)
            .columns(ImmutableList.of("__time", "col2"))
            .dataSource("source")
            .intervals(intervalSpec)
            .build();
    }
  }

  // Validates that getResultOrdering will work for the broker n-way merge
  @Test
  public void testMergeSequenceForResults()
  {
    // Should be able to handle merging s1, s2, s3
    ScanQuery noOrderScan = Druids.newScanQueryBuilder()
                                  .order(ScanQuery.Order.NONE)
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                  .dataSource("some src")
                                  .intervals(intervalSpec)
                                  .build();

    // Should only handle s1 and s2
    ScanQuery descendingOrderScan = Druids.newScanQueryBuilder()
                                          .order(ScanQuery.Order.DESCENDING)
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                          .dataSource("some src")
                                          .intervals(intervalSpec)
                                          .build();

    // Should only handle s1 and s2
    ScanQuery ascendingOrderScan = Druids.newScanQueryBuilder()
                                         .order(ScanQuery.Order.ASCENDING)
                                         .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                         .dataSource("some src")
                                         .intervals(intervalSpec)
                                         .build();
    // No Order
    Sequence<ScanResultValue> noOrderSeq =
        Sequences.simple(
            ImmutableList.of(
                Sequences.simple(ImmutableList.of(s1, s3)),
                Sequences.simple(ImmutableList.of(s2))
            )
        ).flatMerge(seq -> seq, noOrderScan.getResultOrdering());

    List<ScanResultValue> noOrderList = noOrderSeq.toList();
    Assert.assertEquals(3, noOrderList.size());


    // Ascending
    Sequence<ScanResultValue> ascendingOrderSeq = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s1)),
            Sequences.simple(ImmutableList.of(s2))
        )
    ).flatMerge(seq -> seq, ascendingOrderScan.getResultOrdering());

    List<ScanResultValue> ascendingList = ascendingOrderSeq.toList();
    Assert.assertEquals(2, ascendingList.size());
    Assert.assertEquals(s1, ascendingList.get(0));
    Assert.assertEquals(s2, ascendingList.get(1));

    // Descending
    Sequence<ScanResultValue> descendingOrderSeq = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s1)),
            Sequences.simple(ImmutableList.of(s2))
        )
    ).flatMerge(seq -> seq, descendingOrderScan.getResultOrdering());

    List<ScanResultValue> descendingList = descendingOrderSeq.toList();
    Assert.assertEquals(2, descendingList.size());
    Assert.assertEquals(s2, descendingList.get(0));
    Assert.assertEquals(s1, descendingList.get(1));
  }

  @Test(expected = ISE.class)
  public void testTimeOrderingWithoutTimeColumn()
  {
    ScanQuery descendingOrderScan = Druids.newScanQueryBuilder()
                                          .order(ScanQuery.Order.DESCENDING)
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                          .dataSource("some src")
                                          .intervals(intervalSpec)
                                          .build();
    // This should fail because s3 doesn't have a timestamp
    Sequence<ScanResultValue> borkedSequence = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s1)),
            Sequences.simple(ImmutableList.of(s2, s3))
        )
    ).flatMerge(seq -> seq, descendingOrderScan.getResultOrdering());

    // This should throw an ISE
    List<ScanResultValue> res = borkedSequence.toList();
  }

  @Test
  public void testGetResultOrderingWithTimeBasedOrderBy()
  {
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .columns("__time")
              .orderBy(Collections.singletonList(new ScanQuery.OrderBy("__time", ScanQuery.Order.DESCENDING)))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .build();

    Assert.assertNotNull(scanQuery.getResultOrdering());
  }

  @Test
  public void testGetResultOrderingWithNonTimeOrderBy()
  {
    // Queries with non-time order cannot currently be executed
    final ScanQuery scanQuery =
        Druids.newScanQueryBuilder()
              .columns("quality")
              .orderBy(Collections.singletonList(new ScanQuery.OrderBy("quality", ScanQuery.Order.ASCENDING)))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .build();

    Assert.assertThrows("Cannot execute query with orderBy [quality ASC]", ISE.class, scanQuery::getResultOrdering);
  }

  @Test
  public void testGetRequiredColumnsWithNoColumns()
  {
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .order(ScanQuery.Order.DESCENDING)
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .build();

    Assert.assertNull(query.getRequiredColumns());
  }

  @Test
  public void testGetRequiredColumnsWithEmptyColumns()
  {
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .order(ScanQuery.Order.DESCENDING)
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .columns(Collections.emptyList())
              .build();

    Assert.assertNull(query.getRequiredColumns());
  }

  @Test
  public void testGetRequiredColumnsWithColumns()
  {
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .columns("foo", "bar")
              .build();

    Assert.assertEquals(ImmutableSet.of("__time", "foo", "bar"), query.getRequiredColumns());
  }

  @Test
  public void testGetRowSignature()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
        .dataSource("some src")
        .intervals(intervalSpec)
        .columns("foo", "bar")
        .columnTypes(ImmutableList.<ColumnType>builder().add(ColumnType.LONG, ColumnType.FLOAT).build())
        .build();
    RowSignature sig = RowSignature.builder()
        .add("foo", ColumnType.LONG)
        .add("bar", ColumnType.FLOAT)
        .build();

    Assert.assertEquals(sig, query.getRowSignature());
  }
}
