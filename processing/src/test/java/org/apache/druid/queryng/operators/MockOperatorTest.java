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

import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.fragment.FragmentHandle;
import org.apache.druid.queryng.fragment.FragmentRun;
import org.apache.druid.queryng.operators.Operator.EofException;
import org.apache.druid.queryng.operators.Operator.RowIterator;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Use a mock operator to test (and illustrate) the basic operator
 * mechanisms.
 */
public class MockOperatorTest
{
  @Test
  public void testMockStringOperator() throws EofException
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<String> op = MockOperator.strings(builder.context(), 2);
    FragmentHandle<String> handle = builder.handle(op);
    try (FragmentRun<String> run = handle.run()) {
      RowIterator<String> iter = run.iterator();
      assertEquals("Mock row 0", iter.next());
      assertEquals("Mock row 1", iter.next());
      OperatorTests.assertEof(iter);
    }
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testMockIntOperator()
  {
    // Test using the toList feature.
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder.context(), 2);
    List<Integer> results = builder.run(op).toList();
    assertEquals(0, (int) results.get(0));
    assertEquals(1, (int) results.get(1));
    assertEquals(Operator.State.CLOSED, op.state);
  }
}
