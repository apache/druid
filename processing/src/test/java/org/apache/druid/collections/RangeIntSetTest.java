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

package org.apache.druid.collections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.NoSuchElementException;

public class RangeIntSetTest
{
  @Test
  public void test_constructor_zeroZero()
  {
    Assert.assertEquals(Collections.emptySet(), new RangeIntSet(0, 0));
  }

  @Test
  public void test_constructor_zeroTwo()
  {
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2));
  }

  @Test
  public void test_contains()
  {
    Assert.assertFalse(new RangeIntSet(0, 2).contains(-1));
    Assert.assertTrue(new RangeIntSet(0, 2).contains(0));
    Assert.assertTrue(new RangeIntSet(0, 2).contains(1));
    Assert.assertFalse(new RangeIntSet(0, 2).contains(2));
    Assert.assertFalse(new RangeIntSet(0, 2).contains(3));
  }

  @Test
  public void test_headSet()
  {
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).headSet(-1));
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).headSet(0));
    Assert.assertEquals(ImmutableSet.of(0), new RangeIntSet(0, 2).headSet(1));
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).headSet(2));
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).headSet(3));
  }

  @Test
  public void test_tailSet()
  {
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).tailSet(-1));
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).tailSet(0));
    Assert.assertEquals(ImmutableSet.of(1), new RangeIntSet(0, 2).tailSet(1));
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).tailSet(2));
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).tailSet(3));
  }

  @Test
  public void test_subSet()
  {
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).subSet(-2, -1));
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).subSet(-1, 0));
    Assert.assertEquals(ImmutableSet.of(0), new RangeIntSet(0, 2).subSet(-1, 1));
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).subSet(-1, 2));
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).subSet(-1, 3));
    Assert.assertEquals(ImmutableSet.of(0), new RangeIntSet(0, 2).subSet(0, 1));
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).subSet(0, 2));
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2).subSet(0, 3));
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).subSet(1, 1));
    Assert.assertEquals(ImmutableSet.of(1), new RangeIntSet(0, 2).subSet(1, 2));
    Assert.assertEquals(ImmutableSet.of(1), new RangeIntSet(0, 2).subSet(1, 3));
    Assert.assertEquals(ImmutableSet.of(), new RangeIntSet(0, 2).subSet(2, 3));
  }

  @Test
  public void test_firstInt()
  {
    Assert.assertEquals(0, new RangeIntSet(0, 2).firstInt());
    Assert.assertThrows(NoSuchElementException.class, () -> new RangeIntSet(0, 0).firstInt());
  }

  @Test
  public void test_lastInt()
  {
    Assert.assertEquals(1, new RangeIntSet(0, 2).lastInt());
    Assert.assertThrows(NoSuchElementException.class, () -> new RangeIntSet(0, 0).firstInt());
  }

  @Test
  public void test_size()
  {
    Assert.assertEquals(0, new RangeIntSet(0, 0).size());
    Assert.assertEquals(2, new RangeIntSet(0, 2).size());
  }

  @Test
  public void test_iterator()
  {
    Assert.assertEquals(
        ImmutableList.of(0, 1),
        ImmutableList.copyOf(new RangeIntSet(0, 2).iterator())
    );
  }

  @Test
  public void test_iterator_from()
  {
    Assert.assertEquals(
        ImmutableList.of(0, 1),
        ImmutableList.copyOf(new RangeIntSet(0, 2).iterator(0))
    );

    Assert.assertEquals(
        ImmutableList.of(1),
        ImmutableList.copyOf(new RangeIntSet(0, 2).iterator(1))
    );

    Assert.assertEquals(
        ImmutableList.of(),
        ImmutableList.copyOf(new RangeIntSet(0, 2).iterator(2))
    );

    Assert.assertEquals(
        ImmutableList.of(),
        ImmutableList.copyOf(new RangeIntSet(0, 2).iterator(3))
    );
  }

  @Test
  public void test_equals()
  {
    Assert.assertEquals(new RangeIntSet(0, 0), new RangeIntSet(0, 0));
    Assert.assertEquals(new RangeIntSet(0, 0), new RangeIntSet(1, 0));
    Assert.assertNotEquals(new RangeIntSet(0, 0), new RangeIntSet(0, 1));
  }

  @Test
  public void test_equals_empty()
  {
    Assert.assertEquals(new RangeIntSet(0, 0), new RangeIntSet(1, 1));
    Assert.assertEquals(new RangeIntSet(0, 0), new RangeIntSet(1, 0));
    Assert.assertEquals(new RangeIntSet(0, 0), new RangeIntSet(0, -1));
  }

  @Test
  public void test_equals_otherSet()
  {
    Assert.assertEquals(ImmutableSet.of(0, 1), new RangeIntSet(0, 2));
    Assert.assertNotEquals(ImmutableSet.of(0, 1, 2), new RangeIntSet(0, 2));
  }
}
