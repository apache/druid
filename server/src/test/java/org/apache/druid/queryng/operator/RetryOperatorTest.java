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

package org.apache.druid.queryng.operator;

import com.google.common.collect.Ordering;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operator.general.RetryOperator;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorTest;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.scan.MockScanResultReader;
import org.apache.druid.segment.SegmentMissingException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(OperatorTest.class)
public class RetryOperatorTest
{
  /**
   * Input has no rows, no missing segments.
   */
  @Test
  public void testEmptyInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    AtomicBoolean didRun = new AtomicBoolean();
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        fragment,
        queryPlus,
        new NullOperator<ScanResultValue>(fragment),
        Ordering.natural(),
        null,
        (id, ctx) -> Collections.emptyList(),
        1,
        true,
        () -> {
          didRun.set(true);
        }
    );
    fragment.registerRoot(op);
    assertTrue(fragment.toList().isEmpty());
    assertTrue(didRun.get());
  }

  /**
   * Input has values and no missing segments.
   */
  @Test
  public void testSimpleInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(fragment, 3, 10, 4, MockScanResultReader.interval(0));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        fragment,
        queryPlus,
        scan,
        Ordering.natural(),
        null,
        (id, ctx) -> Collections.emptyList(),
        1,
        true,
        () -> { }
    );
    fragment.registerRoot(op);
    assertEquals(3, fragment.toList().size());
  }

  /**
   * One round of missing segments, second round is fine.
   */
  @Test
  public void testRetryInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(fragment, 3, 10, 4, MockScanResultReader.interval(0));
    Operator<ScanResultValue> scan2 = new MockScanResultReader(fragment, 3, 5, 4, MockScanResultReader.interval(0));
    QueryRunner<ScanResultValue> runner2 = (qp, qc) -> Operators.toSequence(scan2);
    AtomicInteger counter = new AtomicInteger();
    List<SegmentDescriptor> dummySegs = Arrays.asList(
        new SegmentDescriptor(MockScanResultReader.interval(0), "vers", 1));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        fragment,
        queryPlus,
        scan,
        Ordering.natural(),
        (q, segs) -> runner2,
        (id, ctx) -> {
          if (counter.getAndAdd(1) == 0) {
            return dummySegs;
          } else {
            return Collections.emptyList();
          }
        },
        2,
        true,
        () -> { }
    );
    fragment.registerRoot(op);
    assertEquals(5, fragment.toList().size());
  }

  /**
   * Continuous missing segments, hit limit of 2, but query allows partial results.
   */
  @Test
  public void testRetryLimitPartial()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(fragment, 3, 10, 4, MockScanResultReader.interval(0));
    List<SegmentDescriptor> dummySegs = Arrays.asList(
        new SegmentDescriptor(MockScanResultReader.interval(0), "vers", 1));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        fragment,
        queryPlus,
        scan,
        Ordering.natural(),
        (q, segs) -> {
          Operator<ScanResultValue> scan2 = new MockScanResultReader(fragment, 3, 5, 4, MockScanResultReader.interval(0));
          return (qp, qc) -> Operators.toSequence(scan2);
        },
        (id, ctx) -> dummySegs,
        2,
        true,
        () -> { }
    );
    fragment.registerRoot(op);
    assertEquals(7, fragment.toList().size());
  }

  /**
   * Continuous missing segments, hit limit of 2, no partial results, so fails.
   */
  @Test
  public void testRetryLimit()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(fragment, 3, 10, 4, MockScanResultReader.interval(0));
    List<SegmentDescriptor> dummySegs = Arrays.asList(
        new SegmentDescriptor(MockScanResultReader.interval(0), "vers", 1));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        fragment,
        queryPlus,
        scan,
        Ordering.natural(),
        (q, segs) -> {
          Operator<ScanResultValue> scan2 = new MockScanResultReader(fragment, 3, 5, 4, MockScanResultReader.interval(0));
          return (qp, qc) -> Operators.toSequence(scan2);
        },
        (id, ctx) -> dummySegs,
        2,
        false,
        () -> { }
    );
    fragment.registerRoot(op);
    try {
      Operators.toList(op);
      fail();
    }
    catch (SegmentMissingException e) {
      // Expected
    }
  }
}
