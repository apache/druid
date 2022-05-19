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

package org.apache.druid.java.util.common.guava;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class LimitedSequenceTest
{
  private static final List<Integer> NUMS = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

  @Test
  public void testSanityAccumulate() throws Exception
  {
    final int threshold = 5;
    SequenceTestHelper.testAll(
        Sequences.simple(NUMS).limit(threshold),
        Lists.newArrayList(Iterables.limit(NUMS, threshold))
    );
  }

  @Test
  public void testTwo() throws Exception
  {
    final int threshold = 2;
    SequenceTestHelper.testAll(
        Sequences.simple(NUMS).limit(threshold),
        Lists.newArrayList(Iterables.limit(NUMS, threshold))
    );
  }

  @Test
  public void testOne() throws Exception
  {
    final int threshold = 1;
    SequenceTestHelper.testAll(
        Sequences.simple(NUMS).limit(threshold),
        Lists.newArrayList(Iterables.limit(NUMS, threshold))
    );
  }

  @Test
  public void testWithYieldingSequence()
  {
    // Regression test for https://github.com/apache/druid/issues/9291.

    // Create a Sequence whose Yielders will yield for each element, regardless of what the accumulator passed
    // to "toYielder" does.
    final BaseSequence<Integer, Iterator<Integer>> sequence = new BaseSequence<Integer, Iterator<Integer>>(
        new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
        {
          @Override
          public Iterator<Integer> make()
          {
            return NUMS.iterator();
          }

          @Override
          public void cleanup(Iterator<Integer> iterFromMake)
          {
            // Do nothing.
          }
        }
    )
    {
      @Override
      public <OutType> Yielder<OutType> toYielder(
          final OutType initValue,
          final YieldingAccumulator<OutType, Integer> accumulator
      )
      {
        return super.toYielder(
            initValue,
            new DelegatingYieldingAccumulator<OutType, Integer>(accumulator)
            {
              @Override
              public OutType accumulate(OutType accumulated, Integer in)
              {
                final OutType retVal = super.accumulate(accumulated, in);
                yield();
                return retVal;
              }
            }
        );
      }
    };

    final int threshold = 4;

    // Can't use "testAll" because its "testYield" implementation depends on the underlying Sequence _not_ yielding.
    SequenceTestHelper.testAccumulation(
        "",
        sequence.limit(threshold),
        Lists.newArrayList(Iterables.limit(NUMS, threshold))
    );
  }

  @Test
  public void testNoSideEffects()
  {
    final List<Integer> nums = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    final AtomicLong accumulated = new AtomicLong(0);
    final Sequence<Integer> seq = Sequences.simple(
        Iterables.transform(
            nums,
            input -> {
              accumulated.addAndGet(input);
              return input;
            }
        )
    ).limit(5);

    Assert.assertEquals(10, seq.accumulate(0, new IntAdditionAccumulator()).intValue());
    Assert.assertEquals(10, accumulated.get());
    Assert.assertEquals(10, seq.accumulate(0, new IntAdditionAccumulator()).intValue());
    Assert.assertEquals(20, accumulated.get());
  }

  private static class IntAdditionAccumulator implements Accumulator<Integer, Integer>
  {
    @Override
    public Integer accumulate(Integer accumulated, Integer in)
    {
      return accumulated + in;
    }
  }
}
