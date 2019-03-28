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

package org.apache.druid.query.lookbackquery;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Result;
import org.hamcrest.collection.IsMapContaining;
import org.hamcrest.collection.IsMapWithSize;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for the generic time based join mapper
 */
public class JoinResultsTest
{
  private static final DateTime today = DateTimes.nowUtc();
  private static final DateTime yesterday = today.plusDays(-1);
  private static final DateTime tomorrow = today.plusDays(1);
  private static final DateTime dayAfterTomorrow = tomorrow.plusDays(1);

  static class TestJoinResults extends JoinResults<DateTime, Result<Integer>>
  {
    TestJoinResults(Future<Sequence<Result<Integer>>> c, Period p)
    {
      super(c, p);
    }

    @Override
    public DateTime extractTimestamp(Result<Integer> val)
    {
      if (val == null) {
        return null;
      }
      return val.getTimestamp();
    }

    @Override
    public DateTime extractKey(Result<Integer> val)
    {
      return val.getTimestamp();
    }

    @Override
    public DateTime extractOffsetKey(Result<LookbackResultValue> val, Period offset)
    {
      return val.getTimestamp().plus(offset);
    }

    @Override
    public Map<String, Object> extractResult(Result<Integer> cVal)
    {
      return cVal != null ? Collections.singletonMap(cVal.getValue().toString(), cVal.getValue()) : null;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJoin()
  {
    Sequence<Result<LookbackResultValue>> measurement = Sequences.simple(Arrays.asList(
        new Result<>(today, new LookbackResultValue(Collections.singletonMap("1", 1))),
        new Result<>(tomorrow, new LookbackResultValue(Collections.singletonMap("2", 2))),
        new Result<>(dayAfterTomorrow, new LookbackResultValue(Collections.singletonMap("3", 3)))
    ));
    Sequence<Result<Integer>> cohort = Sequences.simple(Arrays.asList(
        new Result<>(yesterday, 0),
        // missing today from cohort to trigger outer join
        new Result<>(tomorrow, 2)
    ));

    Period offset = Period.days(-1);

    JoinResults<DateTime, Result<Integer>> joiner = new TestJoinResults(new Future<Sequence<Result<Integer>>>()
    {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public Sequence<Result<Integer>> get() throws InterruptedException, ExecutionException
      {
        return cohort;
      }

      @Override
      public Sequence<Result<Integer>> get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException
      {
        return cohort;
      }
    }, offset);

    Sequence<Result<LookbackResultValue>> resultSeq = Sequences.map(measurement, joiner);

    List<Result<LookbackResultValue>> result = Sequences.toList(resultSeq, new ArrayList<>());
    assertEquals(3, result.size());

    // today
    Result<LookbackResultValue> r = result.get(0);
    LookbackResultValue l = r.getValue();
    Map<String, Object> m = l.getMeasurementValues();
    Map<Period, Map<String, Object>> c = l.getLookbackValues();
    assertEquals(today, r.getTimestamp());
    assertThat(m, allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("1", 1)));
    assertThat(c, IsMapWithSize.aMapWithSize(1));
    assertThat(c.get(offset), allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("0", 0)));

    // tomorrow
    r = result.get(1);
    l = r.getValue();
    m = l.getMeasurementValues();
    c = l.getLookbackValues();
    assertEquals(tomorrow, r.getTimestamp());
    assertThat(m, allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("2", 2)));
    assertThat(c, IsMapWithSize.aMapWithSize(1));
    assertNull(c.get(offset));

    // day after
    r = result.get(2);
    l = r.getValue();
    m = l.getMeasurementValues();
    c = l.getLookbackValues();
    assertEquals(dayAfterTomorrow, r.getTimestamp());
    assertThat(m, allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("3", 3)));
    assertThat(c, IsMapWithSize.aMapWithSize(1));
    assertThat(c.get(offset), allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("2", 2)));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJoinNoCohort()
  {
    Sequence<Result<LookbackResultValue>> measurement = Sequences.simple(Arrays.asList(
        new Result<>(today, new LookbackResultValue(Collections.singletonMap("1", 1))),
        new Result<>(tomorrow, new LookbackResultValue(Collections.singletonMap("2", 2))),
        new Result<>(dayAfterTomorrow, new LookbackResultValue(Collections.singletonMap("3", 3)))
    ));
    Sequence<Result<Integer>> cohort = Sequences.simple(Collections.emptyList());

    //JoinResults<DateTime, Result<Integer>> joiner = new TestJoinResults(cohort, Period.days(-1));
    JoinResults<DateTime, Result<Integer>> joiner = new TestJoinResults(new Future<Sequence<Result<Integer>>>()
    {

      @Override
      public boolean cancel(boolean mayInterruptIfRunning)
      {
        return false;
      }

      @Override
      public boolean isCancelled()
      {
        return false;
      }

      @Override
      public boolean isDone()
      {
        return true;
      }

      @Override
      public Sequence<Result<Integer>> get() throws InterruptedException, ExecutionException
      {
        return cohort;
      }

      @Override
      public Sequence<Result<Integer>> get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException
      {
        return cohort;
      }

    }, Period.days(-1));

    Sequence<Result<LookbackResultValue>> resultSeq = Sequences.map(measurement, joiner);

    List<Result<LookbackResultValue>> result = Sequences.toList(resultSeq, new ArrayList<>());
    assertEquals(3, result.size());

    // today
    Result<LookbackResultValue> r = result.get(0);
    LookbackResultValue l = r.getValue();
    Map<String, Object> m = l.getMeasurementValues();
    Map<String, Object> c = l.getLookbackValuesForKey(Period.days(-1));
    assertEquals(today, r.getTimestamp());
    assertThat(m, allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("1", 1)));
    assertNull(c);

    // tomorrow
    r = result.get(1);
    l = r.getValue();
    m = l.getMeasurementValues();
    c = l.getLookbackValuesForKey(Period.days(-1));
    assertEquals(tomorrow, r.getTimestamp());
    assertThat(m, allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("2", 2)));
    assertNull(c);

    // day after
    r = result.get(2);
    l = r.getValue();
    m = l.getMeasurementValues();
    c = l.getLookbackValuesForKey(Period.days(-1));
    assertEquals(dayAfterTomorrow, r.getTimestamp());
    assertThat(m, allOf(IsMapWithSize.aMapWithSize(1), IsMapContaining.hasEntry("3", 3)));
    assertNull(c);
  }

}
