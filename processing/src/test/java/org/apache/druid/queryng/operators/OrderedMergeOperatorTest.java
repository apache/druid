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

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.State;
import org.apache.druid.queryng.operators.OrderedMergeOperator.Input;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OrderedMergeOperatorTest
{
  @Test
  public void testNoInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Supplier<Iterable<Input<Integer>>> inputs =
        () -> Collections.emptyList();
    Operator<Integer> op = new OrderedMergeOperator<>(
        context,
        Ordering.natural(),
        0,
        inputs);
    Iterator<Integer> iter = op.open();
    assertFalse(iter.hasNext());
    op.close(true);
  }

  @Test
  public void testEmptyInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Supplier<Iterable<Input<Integer>>> inputs =
        () -> Arrays.asList(
            new Input<Integer>(new NullOperator<Integer>(context)),
            new Input<Integer>(new NullOperator<Integer>(context)));
    Operator<Integer> op = new OrderedMergeOperator<>(
        context,
        Ordering.natural(),
        2,
        inputs);
    Iterator<Integer> iter = op.open();
    assertFalse(iter.hasNext());
    op.close(true);
  }

  @Test
  public void testOneInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Supplier<Iterable<Input<Integer>>> inputs =
        () -> Collections.singletonList(
            new Input<Integer>(MockOperator.ints(context, 3)));
    Operator<Integer> op = new OrderedMergeOperator<>(
        context,
        Ordering.natural(),
        1,
        inputs);
    Iterator<Integer> iter = op.open();
    List<Integer> results = Lists.newArrayList(iter);
    op.close(true);
    assertEquals(Arrays.asList(0, 1, 2), results);
  }

  @Test
  public void testTwoInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Supplier<Iterable<Input<Integer>>> inputs =
        () -> Arrays.asList(
            new Input<Integer>(MockOperator.ints(context, 3)),
            new Input<Integer>(MockOperator.ints(context, 5)));
    Operator<Integer> op = new OrderedMergeOperator<>(
        context,
        Ordering.natural(),
        2,
        inputs);
    Iterator<Integer> iter = op.open();
    List<Integer> results = Lists.newArrayList(iter);
    op.close(true);
    assertEquals(Arrays.asList(0, 0, 1, 1, 2, 2, 3, 4), results);
  }

  @Test
  public void testClose()
  {
    FragmentContext context = FragmentContext.defaultContext();
    MockOperator<Integer> input1 = MockOperator.ints(context, 2);
    MockOperator<Integer> input2 = MockOperator.ints(context, 2);
    Supplier<Iterable<Input<Integer>>> inputs =
        () -> Arrays.asList(
            new Input<Integer>(input1),
            new Input<Integer>(input2));
    Operator<Integer> op = new OrderedMergeOperator<>(
        context,
        Ordering.natural(),
        2,
        inputs);
    Iterator<Integer> iter = op.open();
    List<Integer> results = Lists.newArrayList(iter);
    assertEquals(Arrays.asList(0, 0, 1, 1), results);

    // Inputs are closed as exhausted.
    assertTrue(input1.state == State.CLOSED);
    assertTrue(input2.state == State.CLOSED);
    op.close(true);
  }

  /**
   * Test the case where the input to the ordered merge is, instead,
   * used directly as an operator (because it turns out there is only
   * one such input, so a merge is unnecessary).
   */
  @Test
  public void testInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    MockOperator<Integer> input1 = MockOperator.ints(context, 2);
    Input<Integer> input = new Input<>(input1);
    assertFalse(input.eof());
    assertEquals(0, (int) input.get());
    Operator<Integer> op2 = input.toOperator(context);
    List<Integer> results = Operators.toList(op2);
    assertEquals(Arrays.asList(0, 1), results);
    assertTrue(input1.state == State.CLOSED);
  }

  /**
   * As above, but with a no-rows input.
   */
  @Test
  public void testEmptyInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input1 = new NullOperator<>(context);
    Input<Integer> input = new Input<>(input1);
    assertTrue(input.eof());
    assertNull(input.get());
    Operator<Integer> op2 = input.toOperator(context);
    List<Integer> results = Operators.toList(op2);
    assertTrue(results.isEmpty());
  }
}
