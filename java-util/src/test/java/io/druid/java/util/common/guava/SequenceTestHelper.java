/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.guava;

import junit.framework.Assert;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class SequenceTestHelper
{
  public static void testAll(Sequence<Integer> seq, List<Integer> nums) throws IOException
  {
    testAll("", seq, nums);
  }

  public static void testAll(String prefix, Sequence<Integer> seq, List<Integer> nums) throws IOException
  {
    testAccumulation(prefix, seq, nums);
    testYield(prefix, seq, nums);
  }

  public static void testYield(final String prefix, Sequence<Integer> seq, final List<Integer> nums) throws IOException
  {
    testYield(prefix, 3, seq, nums);
    testYield(prefix, 1, seq, nums);
  }

  public static void testYield(
      final String prefix,
      final int numToTake,
      Sequence<Integer> seq,
      final List<Integer> nums
  ) throws IOException
  {
    Iterator<Integer> numsIter = nums.iterator();
    Yielder<Integer> yielder = seq.toYielder(
        0, new YieldingAccumulator<Integer, Integer>()
    {
      final Iterator<Integer> valsIter = nums.iterator();
      int count = 0;

      @Override
      public Integer accumulate(Integer accumulated, Integer in)
      {
        if (++count >= numToTake) {
          count = 0;
          yield();
        }

        Assert.assertEquals(prefix, valsIter.next(), in);
        return accumulated + in;
      }
    }
    );

    int expectedSum = 0;
    while (numsIter.hasNext()) {
      int i = 0;
      for (; i < numToTake && numsIter.hasNext(); ++i) {
        expectedSum += numsIter.next();
      }

      if (i >= numToTake) {
        Assert.assertFalse(prefix, yielder.isDone());
        Assert.assertEquals(prefix, expectedSum, yielder.get().intValue());

        expectedSum = 0;
        yielder = yielder.next(0);
      }
    }

    Assert.assertEquals(expectedSum, yielder.get().intValue());
    Assert.assertTrue(prefix, yielder.isDone());
    yielder.close();
  }


  public static void testAccumulation(final String prefix, Sequence<Integer> seq, final List<Integer> nums)
  {
    int expectedSum = 0;
    for (Integer num : nums) {
      expectedSum += num;
    }

    int sum = seq.accumulate(
        0, new Accumulator<Integer, Integer>()
    {
      final Iterator<Integer> valsIter = nums.iterator();

      @Override
      public Integer accumulate(Integer accumulated, Integer in)
      {
        Assert.assertEquals(prefix, valsIter.next(), in);
        return accumulated + in;
      }
    }
    );

    Assert.assertEquals(prefix, expectedSum, sum);
  }

  public static void testClosed(AtomicInteger closedCounter, Sequence<Integer> seq)
  {
    boolean exceptionThrown = false;
    try {
      seq.accumulate(
          1,
          new Accumulator<Integer, Integer>()
          {
            @Override
            public Integer accumulate(Integer accumulated, Integer in)
            {
              return ++accumulated;
            }
          }
      );
    }
    catch (UnsupportedOperationException e) {
      exceptionThrown = true;
    }

    Assert.assertTrue(exceptionThrown);
    Assert.assertEquals(1, closedCounter.get());

    exceptionThrown = false;
    Yielder<Integer> yielder = null;
    try {
      yielder = seq.toYielder(
          1,
          new YieldingAccumulator<Integer, Integer>()
          {
            @Override
            public Integer accumulate(Integer accumulated, Integer in)
            {
              return ++accumulated;
            }
          }
      );
    }
    catch (UnsupportedOperationException e) {
      exceptionThrown = true;
    }

    Assert.assertNull(yielder);
    Assert.assertTrue(exceptionThrown);
    Assert.assertEquals(2, closedCounter.get());
  }
}
