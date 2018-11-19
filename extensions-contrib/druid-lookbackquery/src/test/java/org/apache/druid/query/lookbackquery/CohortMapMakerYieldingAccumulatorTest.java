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
import org.apache.druid.query.lookbackquery.CohortMapMakerYieldingAccumulator.ResultMap;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.query.Result;

import static junit.framework.TestCase.assertSame;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for CohortMapMakerYieldingAccumulator
 */
public class CohortMapMakerYieldingAccumulatorTest extends EasyMockSupport
{
  private static final DateTime today = DateTimes.nowUtc();
  private static final DateTime tomorrow = today.plusDays(1);
  private static final DateTime dayAfterTomorrow = tomorrow.plusDays(1);

  @Test
  public void testReset()
  {
    @SuppressWarnings("unchecked")
    ResultMap<Integer, Result<Integer>> map = mock(ResultMap.class);
    CohortMapMakerYieldingAccumulator<Integer, Result<Integer>> y = new CohortMapMakerYieldingAccumulator<>(map);

    map.clear();

    replay(map);

    y.yield();
    assertTrue(y.yielded());
    y.reset();

    verify(map);

    assertFalse(y.yielded());
  }

  @Test
  public void testAccumulate()
  {
    @SuppressWarnings("unchecked")
    ResultMap<Integer, Result<Integer>> map = mock(ResultMap.class);
    CohortMapMakerYieldingAccumulator<Integer, Result<Integer>> y = new CohortMapMakerYieldingAccumulator<>(map);
    @SuppressWarnings("unchecked")
    Result<Integer> in = mock(Result.class);
    Result<Integer> out;

    // expected data - (today, 1), (today,2), (tomorrow,1), (tomorrow,2)
    expect(map.extractTimestamp(null)).andReturn(null);
    expect(map.extractTimestamp(in))
        .andReturn(today).times(4).andReturn(tomorrow).times(3);

    // each piece of data maps to a unique key
    expect(map.extractKey(in)).andAnswer(new IAnswer<Integer>()
    {
      int count = 0;

      @Override
      public Integer answer() throws Throwable
      {
        return count++;
      }
    }).times(3);

    // three values are added to the map (the last is accessible
    // from the yielder via get()
    map.put(anyObject(Integer.class), same(in));
    expectLastCall().times(3);

    // THe map generation is updated every time the map is updated
    map.updateMapGeneration(anyObject(DateTime.class));
    expectLastCall().times(3);

    // The map is cleared once when the accumualtor is reset
    map.clear();
    expectLastCall().times(1);

    // done with specifying behavior. start the test
    replayAll();

    assertFalse(y.yielded());

    // prior value = null, new value = (today, 1)
    out = y.accumulate(null, in);
    assertSame(in, out);
    assertFalse(y.yielded());

    // prior value = (today, 1), new value= (today, 2)
    out = y.accumulate(out, in);
    assertSame(out, in);
    assertFalse(y.yielded());

    // prior value = (today, 2), new value = (tomorrow, 1)
    out = y.accumulate(out, in);
    assertSame(out, in);
    assertTrue(y.yielded());

    // prior value = (tomorrow, 1), new value = (tomorrow, 2)
    y.reset();
    out = y.accumulate(out, in);
    assertSame(out, in);
    assertFalse(y.yielded());

    // verify all expected calls occurred.
    verifyAll();
  }

  @Test
  public void testYieldingAccumulator()
  {
    final Map<Result<Integer>, Result<Integer>> map = new HashMap<>();
    ResultMap<Result<Integer>, Result<Integer>> resultMap = new ResultMap<Result<Integer>, Result<Integer>>()
    {
      @Override
      public void updateMapGeneration(DateTime dateTime)
      {
      }

      @Override
      public void clear()
      {
        map.clear();
      }

      @Override
      public void put(Result<Integer> key, Result<Integer> val)
      {
        map.put(key, val);
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
      public Result<Integer> extractKey(Result<Integer> val)
      {
        return val;
      }
    };
    Sequence<Result<Integer>> sequence = Sequences.simple(Arrays.asList(
        new Result<>(today, 1),
        new Result<>(today, 2),
        new Result<>(tomorrow, 1),
        new Result<>(dayAfterTomorrow, 1),
        new Result<>(dayAfterTomorrow, 2)
    ));

    // map starts out empty
    assertEquals(0, map.size());

    // constructing a yielder reads the first block of data
    // after construction, the map should contain two entries
    Yielder<Result<Integer>> yielder = sequence.toYielder(null, new CohortMapMakerYieldingAccumulator<>(resultMap));
    assertEquals(2, map.size());
    assertEquals(new Result<Integer>(tomorrow, 1), yielder.get());
    assertFalse(yielder.isDone());

    // calling next should read the next day
    yielder = yielder.next(yielder.get());
    assertEquals(1, map.size());
    assertEquals(new Result<Integer>(dayAfterTomorrow, 1), yielder.get());
    assertFalse(yielder.isDone());

    // calling next should read the last day, but be missing the last
    // row, which needs to be retrieved with get() explicitly
    yielder = yielder.next(yielder.get());
    assertEquals(1, map.size());
    assertEquals(new Result<Integer>(dayAfterTomorrow, 2), yielder.get());
    assertTrue(yielder.isDone());


  }
}
