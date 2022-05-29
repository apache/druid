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

package org.apache.druid.queryng.operators.general;

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.AbstractMergeOperator.OperatorInput;
import org.apache.druid.queryng.operators.DeferredMergeOperator;
import org.apache.druid.queryng.operators.MockOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.State;
import org.apache.druid.queryng.operators.OperatorTest;
import org.apache.druid.queryng.operators.ResultIterator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(OperatorTest.class)
public class DeferredMergeOperatorTest
{
  @Test
  public void testNoInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> op = new DeferredMergeOperator<>(
        fragment,
        Ordering.natural(),
        0,
        Collections.emptyList()
    );
    fragment.registerRoot(op);
    assertTrue(fragment.toList().isEmpty());
  }

  private static <T> OperatorInput<T> makeInput(Operator<T> op)
  {
    try {
      ResultIterator<T> iter = op.open();
      return new OperatorInput<T>(op, iter, iter.next());
    }
    catch (ResultIterator.EofException e) {
      fail();
      return null;
    }
  }

  @Test
  public void testOneInput()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> op = new DeferredMergeOperator<>(
        fragment,
        Ordering.natural(),
        1,
        Collections.singletonList(
            makeInput(MockOperator.ints(fragment, 3))
        )
    );
    fragment.registerRoot(op);
    assertEquals(Arrays.asList(0, 1, 2), fragment.toList());
  }

  @Test
  public void testTwoInputs()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Integer> op = new DeferredMergeOperator<>(
        fragment,
        Ordering.natural(),
        2,
        Arrays.asList(
            makeInput(MockOperator.ints(fragment, 3)),
            makeInput(MockOperator.ints(fragment, 5))
        )
    );
    fragment.registerRoot(op);
    assertEquals(Arrays.asList(0, 0, 1, 1, 2, 2, 3, 4), fragment.toList());
  }

  @Test
  public void testClose()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    MockOperator<Integer> input1 = MockOperator.ints(fragment, 2);
    MockOperator<Integer> input2 = MockOperator.ints(fragment, 2);
    Operator<Integer> op = new DeferredMergeOperator<>(
        fragment,
        Ordering.natural(),
        2,
        Arrays.asList(
            makeInput(input1),
            makeInput(input2)
        )
    );
    fragment.registerRoot(op);
    assertEquals(Arrays.asList(0, 0, 1, 1), fragment.toList());

    // Inputs are closed as exhausted.
    assertTrue(input1.state == State.CLOSED);
    assertTrue(input2.state == State.CLOSED);
  }
}
