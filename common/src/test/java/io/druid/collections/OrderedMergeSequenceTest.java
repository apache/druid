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

package io.druid.collections;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.SequenceTestHelper;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.TestSequence;
import junit.framework.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class OrderedMergeSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSequences = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );

    OrderedMergeSequence<Integer> seq = makeMergedSequence(Ordering.<Integer>natural(), testSequences);

    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSequences) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testScrewsUpOnOutOfOrderBeginningOfList() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSequences = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(4, 6, 8),
        TestSequence.create(2, 8)
    );

    OrderedMergeSequence<Integer> seq = makeMergedSequence(Ordering.<Integer>natural(), testSequences);

    SequenceTestHelper.testAll(seq, Arrays.asList(1, 3, 4, 2, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSequences) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testScrewsUpOnOutOfOrderInList() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSequences = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 4, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6)
    );

    OrderedMergeSequence<Integer> seq = makeMergedSequence(Ordering.<Integer>natural(), testSequences);

    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 4, 6, 7, 8, 9));

    for (TestSequence<Integer> sequence : testSequences) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testLazinessAccumulation() throws Exception
  {
    final ArrayList<Sequence<Integer>> sequences = makeSyncedSequences();
    OrderedMergeSequence<Integer> seq = new OrderedMergeSequence<Integer>(
        Ordering.<Integer>natural(), Sequences.simple(sequences)
    );
    SequenceTestHelper.testAccumulation("", seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  @Test
  public void testLazinessYielder() throws Exception
  {
    final ArrayList<Sequence<Integer>> sequences = makeSyncedSequences();
    OrderedMergeSequence<Integer> seq = new OrderedMergeSequence<Integer>(
        Ordering.<Integer>natural(), Sequences.simple(sequences)
    );
    SequenceTestHelper.testYield("", seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  private ArrayList<Sequence<Integer>> makeSyncedSequences()
  {
    final boolean[] done = new boolean[]{false, false};

    final ArrayList<Sequence<Integer>> sequences = Lists.newArrayList();
    sequences.add(
        new BaseSequence<Integer, Iterator<Integer>>(
            new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
            {
              @Override
              public Iterator<Integer> make()
              {
                return Arrays.asList(1, 2, 3).iterator();
              }

              @Override
              public void cleanup(Iterator<Integer> iterFromMake)
              {
                done[0] = true;
              }
            }
        )
    );
    sequences.add(
        new BaseSequence<Integer, Iterator<Integer>>(
            new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
            {
              @Override
              public Iterator<Integer> make()
              {
                return new IteratorShell<Integer>(Arrays.asList(4, 5, 6).iterator())
                {
                  int count = 0;

                  @Override
                  public boolean hasNext()
                  {
                    if (count >= 1) {
                      Assert.assertTrue("First iterator not complete", done[0]);
                    }
                    return super.hasNext();
                  }

                  @Override
                  public Integer next()
                  {
                    if (count >= 1) {
                      Assert.assertTrue("First iterator not complete", done[0]);
                    }
                    ++count;
                    return super.next();
                  }
                };
              }

              @Override
              public void cleanup(Iterator<Integer> iterFromMake)
              {
                done[1] = true;
              }
            }
        )
    );
    sequences.add(
        new BaseSequence<Integer, Iterator<Integer>>(
            new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
            {
              @Override
              public Iterator<Integer> make()
              {
                return new IteratorShell<Integer>(Arrays.asList(7, 8, 9).iterator())
                {
                  int count = 0;

                  @Override
                  public boolean hasNext()
                  {
                    if (count >= 1) {
                      Assert.assertTrue("Second iterator not complete", done[1]);
                    }
                    Assert.assertTrue("First iterator not complete", done[0]);
                    return super.hasNext();
                  }

                  @Override
                  public Integer next()
                  {
                    if (count >= 1) {
                      Assert.assertTrue("Second iterator not complete", done[1]);
                    }
                    Assert.assertTrue("First iterator not complete", done[0]);
                    ++count;
                    return super.next();
                  }
                };
              }

              @Override
              public void cleanup(Iterator<Integer> iterFromMake)
              {
              }
            }
        )
    );
    return sequences;
  }

  private <T> OrderedMergeSequence<T> makeMergedSequence(
      Ordering<T> ordering,
      List<TestSequence<T>> seqs
  )
  {
    return new OrderedMergeSequence<T>(
        ordering,
        Sequences.simple(
            Lists.transform( // OMG WTF, the java type system really annoys me at times...
                seqs,
                new Function<TestSequence<T>, Sequence<T>>()
                {
                  @Override
                  public Sequence<T> apply(@Nullable TestSequence<T> input)
                  {
                    return input;
                  }
                }
            )
        )
    );
  }
}
