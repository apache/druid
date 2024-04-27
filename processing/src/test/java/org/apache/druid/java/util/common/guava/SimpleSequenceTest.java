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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SimpleSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    final List<Integer> vals = Arrays.asList(1, 2, 3, 4, 5);
    SequenceTestHelper.testAll(Sequences.simple(vals), vals);
  }

  @Test
  public void testNothing() throws Exception
  {
    final List<Integer> vals = Collections.emptyList();
    SequenceTestHelper.testAll(Sequences.simple(vals), vals);
  }

  @Test
  public void testGetIterable()
  {
    final List<Integer> vals = Collections.singletonList(1);
    Assert.assertSame(vals, ((SimpleSequence<Integer>) Sequences.simple(vals)).getIterable());
  }

  @Test
  public void testToList()
  {
    final List<Integer> vals = Arrays.asList(1, 2);
    Assert.assertEquals(vals, Sequences.simple(vals).toList());
  }
}
