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

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

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

    MergeSequence<Integer> seq = new MergeSequence<>(Ordering.<Integer>natural(), (Sequence) Sequences.simple(testSeqs));
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assert.assertTrue(sequence.isClosed());
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

    MergeSequence<Integer> seq = new MergeSequence<>(Ordering.<Integer>natural(), (Sequence) Sequences.simple(testSeqs));
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testMergeEmpties() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.<Integer>create(),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );

    MergeSequence<Integer> seq = new MergeSequence<>(Ordering.<Integer>natural(), (Sequence) Sequences.simple(testSeqs));
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testMergeEmpties1() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.<Integer>create(),
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );

    MergeSequence<Integer> seq = new MergeSequence<>(Ordering.<Integer>natural(), (Sequence) Sequences.simple(testSeqs));
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testMergeEmpties2() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.<Integer>create(),
        TestSequence.create(4, 6, 8),
        TestSequence.<Integer>create()
    );

    MergeSequence<Integer> seq = new MergeSequence<>(Ordering.<Integer>natural(), (Sequence) Sequences.simple(testSeqs));
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assert.assertTrue(sequence.isClosed());
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

    MergeSequence<Integer> seq = new MergeSequence<>(Ordering.<Integer>natural(), (Sequence) Sequences.simple(testSeqs));
    SequenceTestHelper.testAll(seq, Arrays.asList(1, 2, 3, 4, 5, 4, 6, 7, 8, 9));

    for (TestSequence<Integer> sequence : testSeqs) {
      Assert.assertTrue(sequence.isClosed());
    }
  }

  @Test
  public void testHierarchicalMerge() throws Exception
  {
    final Sequence<Integer> seq1 = new MergeSequence<>(
        Ordering.<Integer>natural(), Sequences.<Sequence<Integer>>simple(
        Lists.<Sequence<Integer>>newArrayList(
            TestSequence.create(1)
        )
    )
    );

    final Sequence<Integer> finalMerged = new MergeSequence<>(
        Ordering.<Integer>natural(),
        Sequences.simple(
            Lists.<Sequence<Integer>>newArrayList(seq1)
        )
    );

    SequenceTestHelper.testAll(finalMerged, Arrays.asList(1));
  }

  @Test
  public void testMergeOne() throws Exception
  {
    final Sequence<Integer> mergeOne = new MergeSequence<>(
        Ordering.<Integer>natural(), Sequences.<Sequence<Integer>>simple(
        Lists.<Sequence<Integer>>newArrayList(
            TestSequence.create(1)
        )
    )
    );

    SequenceTestHelper.testAll(mergeOne, Arrays.asList(1));
  }

}
