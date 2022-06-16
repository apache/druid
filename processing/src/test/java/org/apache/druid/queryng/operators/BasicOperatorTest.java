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
import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.fragment.FragmentRun;
import org.apache.druid.queryng.operators.Operator.EofException;
import org.apache.druid.queryng.operators.Operator.RowIterator;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BasicOperatorTest
{
  @Test
  public void testToIterable()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
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
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    Operator<Integer> op2 = new FilterOperator<Integer>(
        builder.context(),
        op,
        x -> x % 2 == 0);
    List<Integer> results = builder.run(op2).toList();
    assertEquals(Arrays.asList(0, 2), results);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testTransform()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    Operator<Integer> op2 = new TransformOperator<Integer, Integer>(
        builder.context(),
        op,
        x -> x * 2);
    List<Integer> results = builder.run(op2).toList();
    assertEquals(Arrays.asList(0, 2, 4, 6), results);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testLimit0()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    Operator<Integer> op2 = new LimitOperator<Integer>(
        builder.context(),
        op,
        0);
    List<Integer> results = builder.run(op2).toList();
    assertTrue(results.isEmpty());
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testLimitNotHit()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    Operator<Integer> op2 = new LimitOperator<Integer>(
        builder.context(),
        op,
        8);
    List<Integer> results = builder.run(op2).toList();
    assertEquals(Arrays.asList(0, 1, 2, 3), results);
  }

  @Test
  public void testLimitHit()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    Operator<Integer> op2 = new LimitOperator<Integer>(
        builder.context(),
        op,
        2);
    List<Integer> results = builder.run(op2).toList();
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
      assertNull(context.exception());
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
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 4);
    MockWrappingOperator<Integer> op2 = new MockWrappingOperator<Integer>(
        builder.context(),
        op);
    List<Integer> results = builder.run(op2).toList();
    assertEquals(Arrays.asList(0, 1, 2, 3), results);
    assertTrue(op2.openCalled);
    assertTrue(op2.closeCalled);
  }

  /**
   * Getting weird: an operator that wraps a sequence that wraps an operator.
   * @throws EofException
   */
  @Test
  public void testSequenceOperator() throws EofException
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<String> op = MockOperator.strings(builder.context(), 2);
    Sequence<String> seq = Operators.toSequence(op);
    Operator<String> outer = Operators.toOperator(builder, seq);
    FragmentRun<String> run = builder.run(outer);
    RowIterator<String> iter = run.iterator();
    assertEquals("Mock row 0", iter.next());
    assertEquals("Mock row 1", iter.next());
    OperatorTests.assertEof(iter);
    run.close();
    assertEquals(Operator.State.CLOSED, op.state);
  }
}
