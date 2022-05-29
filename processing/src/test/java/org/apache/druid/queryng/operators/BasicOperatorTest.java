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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class BasicOperatorTest
{
  @Test
  public void testToIterable()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 2);
    int count = 0;
    for (Integer row : Operators.toIterable(op)) {
      assertEquals(count++, (int) row);
    }
    assertEquals(2, count);
    op.close(false);
  }

  @Test
  public void testFilter()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    Operator<Integer> op2 = new FilterOperator<Integer>(
        fragment,
        op,
        x -> x % 2 == 0);
    fragment.registerRoot(op2);
    List<Integer> results = fragment.toList();
    assertEquals(Arrays.asList(0, 2), results);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testTransform()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    Operator<Integer> op2 = new TransformOperator<Integer, Integer>(
        fragment,
        op,
        x -> x * 2,
        "test"
    );
    fragment.registerRoot(op2);
    List<Integer> results = fragment.toList();
    assertEquals(Arrays.asList(0, 2, 4, 6), results);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testLimit0()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    Operator<Integer> op2 = new LimitOperator<Integer>(
        fragment,
        op,
        0);
    fragment.registerRoot(op2);
    List<Integer> results = fragment.toList();
    assertTrue(results.isEmpty());
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testLimitNotHit()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    Operator<Integer> op2 = new LimitOperator<Integer>(
        fragment,
        op,
        8);
    fragment.registerRoot(op2);
    List<Integer> results = fragment.toList();
    assertEquals(Arrays.asList(0, 1, 2, 3), results);
  }

  @Test
  public void testLimitHit()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    Operator<Integer> op2 = new LimitOperator<Integer>(
        fragment,
        op,
        2);
    fragment.registerRoot(op2);
    List<Integer> results = fragment.toList();
    assertEquals(Arrays.asList(0, 1), results);
  }

  private static class MockWrappingOperator<T> extends WrappingOperator<T>
  {
    boolean openCalled;
    boolean closeCalled;

    public MockWrappingOperator(FragmentContext context, Operator<T> input)
    {
      super(context, input);
    }

    @Override
    protected void onOpen()
    {
      openCalled = true;
      // Silly check just to create a reference to the context variable.
      assertEquals(FragmentContext.State.RUN, context.state());
    }

    @Override
    protected void onClose()
    {
      closeCalled = true;
    }
  }

  @Test
  public void testWrappingOperator()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> op = MockOperator.ints(fragment, 4);
    MockWrappingOperator<Integer> op2 = new MockWrappingOperator<Integer>(
        fragment,
        op);
    fragment.registerRoot(op2);
    List<Integer> results = fragment.toList();
    assertEquals(Arrays.asList(0, 1, 2, 3), results);
    assertTrue(op2.openCalled);
    assertTrue(op2.closeCalled);
  }

  /**
   * Getting weird: an operator that wraps a sequence that wraps an operator.
   * @throws ResultIterator.EofException
   */
  @Test
  public void testSequenceOperator() throws ResultIterator.EofException
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<String> op = MockOperator.strings(fragment, 2);
    Sequence<String> seq = Operators.toSequence(op);
    Operator<String> outer = Operators.toOperator(fragment, seq);
    fragment.registerRoot(outer);
    ResultIterator<String> iter = fragment.run();
    assertEquals("Mock row 0", iter.next());
    assertEquals("Mock row 1", iter.next());
    OperatorTests.assertEof(iter);
    fragment.close();
    assertEquals(Operator.State.CLOSED, op.state);
  }
}
