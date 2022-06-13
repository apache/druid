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

import org.apache.druid.queryng.fragment.FragmentContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PushBackOperatorTest
{
  @Test
  public void testEmptyInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<String> input = new NullOperator<String>(context);
    Operator<String> op = new PushBackOperator<String>(context, input);
    assertTrue(Operators.toList(op).isEmpty());
  }

  @Test
  public void testSimpleInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input = MockOperator.ints(context, 2);
    Operator<Integer> op = new PushBackOperator<Integer>(context, input);
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testPush()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input = MockOperator.ints(context, 2);
    PushBackOperator<Integer> op = new PushBackOperator<Integer>(context, input);
    Iterator<Integer> iter = op.open();
    assertTrue(iter.hasNext());
    Integer item = iter.next();
    op.push(item);
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testInitialPush()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input = MockOperator.ints(context, 2);
    Iterator<Integer> iter = input.open();
    assertTrue(iter.hasNext());
    Integer item = iter.next();
    PushBackOperator<Integer> op = new PushBackOperator<Integer>(context, input, iter, item);
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }
}
