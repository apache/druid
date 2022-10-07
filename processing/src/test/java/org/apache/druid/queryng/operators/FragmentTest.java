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

package org.apache.druid.queryng.operators;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceTestHelper;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class FragmentTest
{
  @Test
  public void testOperatorBasics()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    assertNull(fragment.rootOperator());
    assertNull(fragment.rootSequence());
    assertFalse(fragment.rootIsOperator());
    assertFalse(fragment.rootIsSequence());

    MockOperator<Integer> op = MockOperator.ints(fragment, 2);
    fragment.registerRoot(op);
    assertSame(op, fragment.rootOperator());
    assertTrue(fragment.rootIsOperator());
    assertFalse(fragment.rootIsSequence());

    // Add another operator to the DAG.
    Operator<Integer> op2 = new FilterOperator<Integer>(fragment, op, x -> x < 3);
    fragment.registerRoot(op2);
    assertTrue(fragment.rootIsOperator());
    assertFalse(fragment.rootIsSequence());
    assertSame(op2, fragment.rootOperator());

    // Convert the root to a sequence
    assertNotNull(fragment.rootSequence());
    assertFalse(fragment.rootIsOperator());
    assertTrue(fragment.rootIsSequence());

    // Convert back to an operator. Since op2 was wrapped, we get it back.
    fragment.registerRoot(op2);
    assertSame(op2, fragment.rootOperator());
    assertTrue(fragment.rootIsOperator());
    assertFalse(fragment.rootIsSequence());

    // Don't run, just close.
    fragment.close();

    // Now, the fragment should complain if we try to run.
    assertThrows(IllegalStateException.class, () -> fragment.run());
    assertThrows(IllegalStateException.class, () -> fragment.runAsSequence());
  }

  @Test
  public void testSequenceBasics()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    Sequence<Integer> seq = Operators.toSequence(op);
    fragment.registerRoot(seq);
    assertFalse(fragment.rootIsOperator());
    assertTrue(fragment.rootIsSequence());
    assertSame(seq, fragment.rootSequence());

    // Add another operator to the DAG by unwrapping the above
    // sequence.
    Operator<Integer> op2 = new FilterOperator<Integer>(
        fragment,
        fragment.rootOperator(),
        x -> x < 3);
    fragment.registerRoot(op2);
    assertTrue(fragment.rootIsOperator());
    assertFalse(fragment.rootIsSequence());
    assertSame(op2, fragment.rootOperator());
  }

  @Test
  public void testRun()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 2);
    fragment.registerRoot(op);
    assertEquals(FragmentContext.State.START, fragment.state());
    int i = 0;
    ResultIterator<Integer> resultIter = fragment.run();
    assertEquals(FragmentContext.State.RUN, fragment.state());
    for (Integer value : Iterators.toIterable(resultIter)) {
      assertEquals(i++, (int) value);
    }
    fragment.close();
    assertEquals(FragmentContext.State.CLOSED, fragment.state());
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testToListWithOperator()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 2);
    fragment.registerRoot(op);
    List<Integer> results = fragment.toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Test running a DAG with a root sequence as a sequence.
   */
  @Test
  public void testToListWithSequence()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 2);
    fragment.registerRoot(op);
    Sequence<Integer> resultSeq = fragment.runAsSequence();
    List<Integer> results = resultSeq.toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(FragmentContext.State.CLOSED, fragment.state());
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * An operator is a one-pass object, don't try sequence tests that assume
   * the sequence is reentrant.
   */
  @Test
  public void testSequenceYielder() throws IOException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 5);
    fragment.registerRoot(op);
    final List<Integer> expected = Arrays.asList(0, 1, 2, 3, 4);
    Sequence<Integer> seq = fragment.runAsSequence();
    SequenceTestHelper.testYield("op", 5, seq, expected);
    assertEquals(FragmentContext.State.CLOSED, fragment.state());
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testSequenceAccum()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    fragment.registerRoot(op);
    final List<Integer> vals = Arrays.asList(0, 1, 2, 3);
    Sequence<Integer> seq = fragment.runAsSequence();
    SequenceTestHelper.testAccumulation("op", seq, vals);
    assertEquals(FragmentContext.State.CLOSED, fragment.state());
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testRunEmptyFragmentRun()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    ResultIterator<Integer> resultIter = fragment.run();
    OperatorTests.assertEof(resultIter);
    fragment.close();
  }

  @Test
  public void testRunEmptyFragmentList()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    assertTrue(fragment.toList().isEmpty());
    assertEquals(FragmentContext.State.CLOSED, fragment.state());
  }

  @Test
  public void testRunEmptyFragmentAsSequence()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Sequence<Integer> seq = fragment.runAsSequence();
    SequenceTestHelper.testAccumulation("empty", seq, Collections.emptyList());
    assertEquals(FragmentContext.State.CLOSED, fragment.state());
  }

  @Test
  public void testFragmentContext() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    FragmentContext context = fragment;
    assertEquals(FragmentContext.State.START, context.state());
    assertEquals("unknown", context.queryId());
    assertNotNull(context.responseContext());
    context.checkTimeout(); // Useless here, just prevents a "not used" error
    assertNull(fragment.exception());
    MockOperator<Integer> op = MockOperator.ints(context, 4);
    fragment.registerRoot(op);
    ResultIterator<Integer> iter = fragment.run();
    assertEquals(FragmentContext.State.RUN, context.state());
    // Read from the iterator, just to keep Java 11 happy.
    assertNotNull(iter.next());
    ISE ex = new ISE("oops");
    ((FragmentManager) context).failed(ex);
    assertEquals(FragmentContext.State.FAILED, context.state());
    assertSame(ex, fragment.exception());
    fragment.close();
  }
}
