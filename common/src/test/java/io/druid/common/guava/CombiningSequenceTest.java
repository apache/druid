/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.common.guava;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.nary.BinaryFn;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@RunWith(Parameterized.class)
public class CombiningSequenceTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> valuesToTry()
  {
    return Arrays.asList(new Object[][] {
        {1}, {2}, {3}, {4}, {5}, {1000}
    });
  }

  private final int yieldEvery;

  public CombiningSequenceTest(int yieldEvery)
  {
    this.yieldEvery = yieldEvery;
  }

  @Test
  public void testMerge() throws IOException
  {
    List<Pair<Integer, Integer>> pairs = Arrays.asList(
        Pair.of(0, 1),
        Pair.of(0, 2),
        Pair.of(0, 3),
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 1),
        Pair.of(5, 10),
        Pair.of(6, 1),
        Pair.of(5, 1)
    );
    List<Pair<Integer, Integer>> expected = Arrays.asList(
        Pair.of(0, 6),
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 11),
        Pair.of(6, 1),
        Pair.of(5, 1)
    );

    testCombining(pairs, expected);
  }

  @Test
  public void testNoMergeOne() throws IOException
  {
    List<Pair<Integer, Integer>> pairs = Arrays.asList(
        Pair.of(0, 1)
    );

    List<Pair<Integer, Integer>> expected = Arrays.asList(
        Pair.of(0, 1)
    );

    testCombining(pairs, expected);
  }

  @Test
  public void testMergeMany() throws IOException
  {
    List<Pair<Integer, Integer>> pairs = Arrays.asList(
        Pair.of(0, 6),
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 11),
        Pair.of(6, 1),
        Pair.of(5, 1)
    );

    List<Pair<Integer, Integer>> expected = Arrays.asList(
        Pair.of(0, 6),
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 11),
        Pair.of(6, 1),
        Pair.of(5, 1)
    );

    testCombining(pairs, expected);
  }

  @Test
  public void testNoMergeTwo() throws IOException
  {
    List<Pair<Integer, Integer>> pairs = Arrays.asList(
        Pair.of(0, 1),
        Pair.of(1, 1)
    );

    List<Pair<Integer, Integer>> expected = Arrays.asList(
        Pair.of(0, 1),
        Pair.of(1, 1)
    );

    testCombining(pairs, expected);
  }

  @Test
  public void testMergeTwo() throws IOException
  {
    List<Pair<Integer, Integer>> pairs = Arrays.asList(
        Pair.of(0, 1),
        Pair.of(0, 1)
    );

    List<Pair<Integer, Integer>> expected = Arrays.asList(
        Pair.of(0, 2)
    );

    testCombining(pairs, expected);
  }

  @Test
  public void testMergeSomeThingsMergedAtEnd() throws IOException
  {
    List<Pair<Integer, Integer>> pairs = Arrays.asList(
        Pair.of(0, 1),
        Pair.of(0, 2),
        Pair.of(0, 3),
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 1),
        Pair.of(5, 10),
        Pair.of(6, 1),
        Pair.of(5, 1),
        Pair.of(5, 2),
        Pair.of(5, 2),
        Pair.of(5, 2),
        Pair.of(5, 2),
        Pair.of(5, 2)
    );
    List<Pair<Integer, Integer>> expected = Arrays.asList(
        Pair.of(0, 6),
        Pair.of(1, 1),
        Pair.of(2, 1),
        Pair.of(5, 11),
        Pair.of(6, 1),
        Pair.of(5, 11)
    );

    testCombining(pairs, expected);
  }

  @Test
  public void testNothing() throws Exception
  {
    testCombining(Arrays.<Pair<Integer, Integer>>asList(), Arrays.<Pair<Integer, Integer>>asList());
  }

  private void testCombining(List<Pair<Integer, Integer>> pairs, List<Pair<Integer, Integer>> expected)
      throws IOException
  {
    Sequence<Pair<Integer, Integer>> seq = new CombiningSequence<Pair<Integer, Integer>>(
        Sequences.simple(pairs),
        Ordering.natural().onResultOf(Pair.<Integer, Integer>lhsFn()),
        new BinaryFn<Pair<Integer, Integer>, Pair<Integer, Integer>, Pair<Integer, Integer>>()
        {
          @Override
          public Pair<Integer, Integer> apply(
              Pair<Integer, Integer> lhs, Pair<Integer, Integer> rhs
          )
          {
            if (lhs == null) {
              return rhs;
            }

            if (rhs == null) {
              return lhs;
            }

            return Pair.of(lhs.lhs, lhs.rhs + rhs.rhs);
          }
        }
    );

    List<Pair<Integer, Integer>> merged = Sequences.toList(seq, Lists.<Pair<Integer, Integer>>newArrayList());

    Assert.assertEquals(expected, merged);

    Yielder<Pair<Integer, Integer>> yielder = seq.toYielder(
        null,
        new YieldingAccumulator<Pair<Integer, Integer>, Pair<Integer, Integer>>()
        {
          int count = 0;

          @Override
          public Pair<Integer, Integer> accumulate(
              Pair<Integer, Integer> lhs, Pair<Integer, Integer> rhs
          )
          {
            count++;
            if(count % yieldEvery == 0) yield();
            return rhs;
          }
        }
    );

    Iterator<Pair<Integer, Integer>> expectedVals = Iterators.filter(
        expected.iterator(),
        new Predicate<Pair<Integer, Integer>>()
        {
          int count = 0;

          @Override
          public boolean apply(
              @Nullable Pair<Integer, Integer> input
          )
          {
            count++;
            if (count % yieldEvery == 0) {
              return true;
            }
            return false;
          }
        }
    );

    if (expectedVals.hasNext()) {
      while (!yielder.isDone()) {
        final Pair<Integer, Integer> expectedVal = expectedVals.next();
        final Pair<Integer, Integer> actual = yielder.get();
        Assert.assertEquals(expectedVal, actual);
        yielder = yielder.next(actual);
      }
    }
    Assert.assertTrue(yielder.isDone());
    Assert.assertFalse(expectedVals.hasNext());
    yielder.close();
  }
}
