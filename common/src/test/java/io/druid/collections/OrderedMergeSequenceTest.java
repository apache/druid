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

package io.druid.collections;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceTestHelper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.TestSequence;

import org.junit.Assert;
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
  public void testMergeEmptySequence() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSequences = Lists.newArrayList(
        TestSequence.create(ImmutableList.<Integer>of()),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );

    OrderedMergeSequence<Integer> seq = makeMergedSequence(Ordering.<Integer>natural(), testSequences);

    SequenceTestHelper.testAll(seq, Arrays.asList(2, 4, 6, 8, 8));

    for (TestSequence<Integer> sequence : testSequences) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testMergeEmptySequenceAtEnd() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSequences = Lists.newArrayList(
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8),
        TestSequence.create(ImmutableList.<Integer>of())
    );

    OrderedMergeSequence<Integer> seq = makeMergedSequence(Ordering.<Integer>natural(), testSequences);

    SequenceTestHelper.testAll(seq, Arrays.asList(2, 4, 6, 8, 8));

    for (TestSequence<Integer> sequence : testSequences) {
      Assert.assertTrue(sequence.isClosed());
    }
  }


  @Test
  public void testMergeEmptySequenceMiddle() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSequences = Lists.newArrayList(
        TestSequence.create(2, 8),
        TestSequence.create(ImmutableList.<Integer>of()),
        TestSequence.create(4, 6, 8)
    );

    OrderedMergeSequence<Integer> seq = makeMergedSequence(Ordering.<Integer>natural(), testSequences);

    SequenceTestHelper.testAll(seq, Arrays.asList(2, 4, 6, 8, 8));

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

  private <T> MergeSequence<T> makeUnorderedMergedSequence(
      Ordering<T> ordering,
      List<TestSequence<T>> seqs
  )
  {
    return new MergeSequence<T>(
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

  @Test
  public void testHierarchicalMerge() throws Exception
  {
    final Sequence<Integer> seq1 = makeUnorderedMergedSequence(
        Ordering.<Integer>natural(), Lists.newArrayList(
        TestSequence.create(1)
    )
    );


    final Sequence<Integer> seq2 = makeUnorderedMergedSequence(
        Ordering.<Integer>natural(), Lists.newArrayList(
        TestSequence.create(1)
    )
    );
    final OrderedMergeSequence<Integer> finalMerged = new OrderedMergeSequence<Integer>(
        Ordering.<Integer>natural(),
        Sequences.simple(
            Lists.<Sequence<Integer>>newArrayList(seq1, seq2)
        )
    );

    SequenceTestHelper.testAll(finalMerged, Arrays.asList(1, 1));
  }

  @Test
  public void testMergeMerge() throws Exception
  {
    final Sequence<Integer> seq1 = makeUnorderedMergedSequence(
        Ordering.<Integer>natural(), Lists.newArrayList(
            TestSequence.create(1)
        )
    );

    final OrderedMergeSequence<Integer> finalMerged = new OrderedMergeSequence<Integer>(
        Ordering.<Integer>natural(),
        Sequences.simple(
            Lists.<Sequence<Integer>>newArrayList(seq1)
        )
    );

    SequenceTestHelper.testAll(finalMerged, Arrays.asList(1));
  }

  @Test
  public void testOne() throws Exception
  {
    final MergeSequence<Integer> seq1 = makeUnorderedMergedSequence(
        Ordering.<Integer>natural(), Lists.newArrayList(
            TestSequence.create(1)
        )
    );

    SequenceTestHelper.testAll(seq1, Arrays.asList(1));
  }
}
