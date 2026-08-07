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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 */
public class MergeSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );

    MergeSequence<Integer> seq = new MergeSequence<>(
        Ordering.<Integer>natural(),
        (Sequence) Sequences.simple(testSeqs)
    );
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assertions.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testWorksWhenBeginningOutOfOrder() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(2, 8),
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(4, 6, 8)
    );

    MergeSequence<Integer> seq = new MergeSequence<>(
        Ordering.<Integer>natural(),
        (Sequence) Sequences.simple(testSeqs)
    );
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assertions.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testMergeEmpties() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );

    MergeSequence<Integer> seq = new MergeSequence<>(
        Ordering.<Integer>natural(),
        (Sequence) Sequences.simple(testSeqs)
    );
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assertions.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testMergeEmpties1() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(),
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );

    MergeSequence<Integer> seq = new MergeSequence<>(
        Ordering.<Integer>natural(),
        (Sequence) Sequences.simple(testSeqs)
    );
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assertions.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testMergeEmpties2() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(),
        TestSequence.create(4, 6, 8),
        TestSequence.create()
    );

    MergeSequence<Integer> seq = new MergeSequence<>(
        Ordering.<Integer>natural(),
        (Sequence) Sequences.simple(testSeqs)
    );
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assertions.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testScrewsUpOnOutOfOrder() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 4, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6)
    );

    MergeSequence<Integer> seq = new MergeSequence<>(
        Ordering.<Integer>natural(),
        (Sequence) Sequences.simple(testSeqs)
    );
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 4, 6, 7, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assertions.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testHierarchicalMerge() throws Exception
  {
    final Sequence<Integer> seq1 = new MergeSequence<>(
        Ordering.natural(), Sequences.simple(
        Collections.singletonList(TestSequence.create(1))
    )
    );

    final Sequence<Integer> finalMerged = new MergeSequence<>(
        Ordering.natural(),
        Sequences.simple(
            Collections.singletonList(seq1)
        )
    );

    SequenceTestHelper.testAll(finalMerged, Collections.singletonList(1));
  }

  @Test
  public void testMergeOne() throws Exception
  {
    final Sequence<Integer> mergeOne = new MergeSequence<>(
        Ordering.natural(), Sequences.simple(
        Collections.singletonList(TestSequence.create(1))
    )
    );

    SequenceTestHelper.testAll(mergeOne, Collections.singletonList(1));
  }

  @Test
  public void testTwoExplodingOnGetSequences()
  {
    final ExplodingSequence<Integer> bomb1 =
        new ExplodingSequence<>(Sequences.simple(ImmutableList.of(1, 2, 2)), true, false);
    final ExplodingSequence<Integer> bomb2 =
        new ExplodingSequence<>(Sequences.simple(ImmutableList.of(1, 2, 2)), true, false);

    final MergeSequence<Integer> mergeSequence =
        new MergeSequence<>(
            Ordering.natural(),
            Sequences.simple(ImmutableList.of(bomb1, bomb2))
        );

    final Exception e1 = Assertions.assertThrows(Exception.class, () ->
        mergeSequence.toYielder(
            null,
            new YieldingAccumulator<Integer, Integer>()
            {
              @Override
              public Integer accumulate(Integer accumulated, Integer in)
              {
                return in;
              }
            }
        )
    );
    MatcherAssert.assertThat(e1.getMessage(), CoreMatchers.equalTo("get"));

    Assertions.assertEquals(1, bomb1.getCloseCount(), "Closes resources (1)");
    Assertions.assertEquals(1, bomb2.getCloseCount(), "Closes resources (2)");
  }

  @Test
  public void testTwoExplodingOnCloseSequences()
  {
    final ExplodingSequence<Integer> bomb1 =
        new ExplodingSequence<>(Sequences.simple(ImmutableList.of(1, 2, 2)), false, true);
    final ExplodingSequence<Integer> bomb2 =
        new ExplodingSequence<>(Sequences.simple(ImmutableList.of(1, 2, 2)), false, true);

    final MergeSequence<Integer> mergeSequence =
        new MergeSequence<>(
            Ordering.natural(),
            Sequences.simple(ImmutableList.of(bomb1, bomb2))
        );

    final Exception e2 = Assertions.assertThrows(Exception.class, () ->
        mergeSequence.toYielder(
            null,
            new YieldingAccumulator<Integer, Integer>()
            {
              @Override
              public Integer accumulate(Integer accumulated, Integer in)
              {
                if (in > 1) {
                  throw new RuntimeException("boom");
                }

                return in;
              }
            }
        )
    );
    MatcherAssert.assertThat(e2.getMessage(), CoreMatchers.equalTo("boom"));

    Assertions.assertEquals(1, bomb1.getCloseCount(), "Closes resources (1)");
    Assertions.assertEquals(1, bomb2.getCloseCount(), "Closes resources (2)");
  }

  @Test
  public void testOneEmptyOneExplodingSequence()
  {
    final ExplodingSequence<Integer> bomb =
        new ExplodingSequence<>(Sequences.simple(ImmutableList.of(1, 2, 2)), false, true);

    final MergeSequence<Integer> mergeSequence =
        new MergeSequence<>(
            Ordering.natural(),
            Sequences.simple(ImmutableList.of(Sequences.empty(), bomb))
        );

    final Exception e3 = Assertions.assertThrows(Exception.class, () ->
        mergeSequence.toYielder(
            null,
            new YieldingAccumulator<Integer, Integer>()
            {
              @Override
              public Integer accumulate(Integer accumulated, Integer in)
              {
                if (in > 1) {
                  throw new RuntimeException("boom");
                }

                return in;
              }
            }
        )
    );
    MatcherAssert.assertThat(e3.getMessage(), CoreMatchers.equalTo("boom"));

    Assertions.assertEquals(1, bomb.getCloseCount(), "Closes resources");
  }
}
