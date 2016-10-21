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

import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;

import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.nary.BinaryFn;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ComplexSequenceTest
{
  @Test
  public void testComplexSequence()
  {
    Sequence<Integer> complex;
    check("[3, 5]", complex = concat(combine(simple(3)), combine(simple(5))));
    check("[8]", complex = combine(complex));
    check("[8, 6, 3, 5]", complex = concat(complex, concat(combine(simple(2, 4)), simple(3, 5))));
    check("[22]", complex = combine(complex));
    check("[22]", concat(complex, simple()));
  }

  private void check(String expected, Sequence<Integer> complex)
  {
    List<Integer> combined = Sequences.toList(complex, new ArrayList<Integer>());
    Assert.assertEquals(expected, combined.toString());

    Yielder<Integer> yielder = complex.toYielder(
        null,
        new YieldingAccumulator<Integer, Integer>()
        {
          @Override
          public Integer accumulate(Integer accumulated, Integer in)
          {
            yield();
            return in;
          }
        }
    );

    List<Integer> combinedByYielder = new ArrayList<>();
    while (!yielder.isDone()) {
      combinedByYielder.add(yielder.get());
      yielder = yielder.next(null);
    }

    Assert.assertEquals(expected, combinedByYielder.toString());
  }

  private Sequence<Integer> simple(int... values)
  {
    return Sequences.simple(Ints.asList(values));
  }

  private Sequence<Integer> combine(Sequence<Integer> sequence)
  {
    return CombiningSequence.create(sequence, alwaysSame, plus);
  }

  private Sequence<Integer> concat(Sequence<Integer>... sequences)
  {
    return Sequences.concat(Arrays.asList(sequences));
  }

  private final Ordering<Integer> alwaysSame = new Ordering<Integer>()
  {
    @Override
    public int compare(Integer left, Integer right)
    {
      return 0;
    }
  };

  private final BinaryFn<Integer, Integer, Integer> plus = new BinaryFn<Integer, Integer, Integer>()
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

      return arg1 + arg2;
    }
  };
}
