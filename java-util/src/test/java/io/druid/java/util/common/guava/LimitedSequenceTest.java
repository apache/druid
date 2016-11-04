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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class LimitedSequenceTest
{
  @Test
  public void testSanityAccumulate() throws Exception
  {
    final List<Integer> nums = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    final int threshold = 5;
    SequenceTestHelper.testAll(
        Sequences.limit(Sequences.simple(nums), threshold),
        Lists.newArrayList(Iterables.limit(nums, threshold))
    );
  }

  @Test
  public void testTwo() throws Exception
  {
    final List<Integer> nums = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    final int threshold = 2;

    SequenceTestHelper.testAll(
        Sequences.limit(Sequences.simple(nums), threshold),
        Lists.newArrayList(Iterables.limit(nums, threshold))
    );
  }

  @Test
  public void testOne() throws Exception
  {
    final List<Integer> nums = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    final int threshold = 1;

    SequenceTestHelper.testAll(
        Sequences.limit(Sequences.simple(nums), threshold),
        Lists.newArrayList(Iterables.limit(nums, threshold))
    );
  }

  @Test
  public void testNoSideEffects() throws Exception
  {
    final List<Integer> nums = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    final AtomicLong accumulated = new AtomicLong(0);
    final Sequence<Integer> seq = Sequences.limit(
        Sequences.simple(
            Iterables.transform(
                nums,
                new Function<Integer, Integer>()
                {
                  @Override
                  public Integer apply(Integer input)
                  {
                    accumulated.addAndGet(input);
                    return input;
                  }
                }
            )
        ),
        5
    );

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
