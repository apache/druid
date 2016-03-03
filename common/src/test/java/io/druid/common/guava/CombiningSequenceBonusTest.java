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

package io.druid.common.guava;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.nary.BinaryFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class CombiningSequenceBonusTest
{

  @Test
  public void testFails()
  {
    List<Integer> output = new ArrayList<>();

    Yielder<Integer> yielder = getWeirdSequence().toYielder(
        null,
        new YieldingAccumulator()
        {
          @Override
          public Object accumulate(Object accumulated, Object in)
          {
            yield();
            return in;
          }
        }
    );

    while (!yielder.isDone()) {
      output.add(yielder.get());
      yielder = yielder.next(null);
    }

    Assert.assertEquals(8, Iterables.getOnlyElement(output).intValue());
  }

  @Test
  public void testPasses()
  {
    List<Integer> output = Sequences.toList(getWeirdSequence(), new ArrayList<Integer>());
    Assert.assertEquals(8, Iterables.getOnlyElement(output).intValue());
  }

  private Sequence<Integer> getWeirdSequence()
  {
    Ordering<Integer> alwaysSame = new Ordering<Integer>()
    {
      @Override
      public int compare(Integer left, Integer right)
      {
        return 0;
      }
    };

    BinaryFn<Integer, Integer, Integer> plus = new BinaryFn<Integer, Integer, Integer>()
    {
      @Override
      public Integer apply(Integer arg1, Integer arg2)
      {
        if (arg1 == null) {
          return arg2;
        }

        if (arg2 == null) {
          return arg1;
        }

        return arg1.intValue() + arg2.intValue();
      }
    };

    return CombiningSequence.create(
        Sequences.concat(
            ImmutableList.<Sequence<Integer>>of(
                CombiningSequence.create(
                    Sequences.simple(ImmutableList.<Integer>of(3)),
                    alwaysSame,
                    plus
                ),
                CombiningSequence.create(
                    Sequences.simple(ImmutableList.of(5)),
                    alwaysSame,
                    plus
                )
            )
        ),
        alwaysSame,
        plus
    );
  }
}
