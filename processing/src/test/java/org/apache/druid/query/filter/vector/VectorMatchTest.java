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

package org.apache.druid.query.filter.vector;

import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class VectorMatchTest
{
  private static final int VECTOR_SIZE = 7;
  private static final int VECTOR_BITS = 1 << VECTOR_SIZE;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testAddAllExhaustive()
  {
    // Tests every combination of vectors up to length VECTOR_SIZE.
    final VectorMatch scratch = VectorMatch.wrap(new int[VECTOR_SIZE]);

    final int[] arrayOne = new int[VECTOR_SIZE];
    final int[] arrayTwo = new int[VECTOR_SIZE];
    final int[] arrayExpected = new int[VECTOR_SIZE];

    for (int bitsOne = 0; bitsOne < VECTOR_BITS; bitsOne++) {
      for (int bitsTwo = 0; bitsTwo < VECTOR_BITS; bitsTwo++) {
        final int lenOne = populate(arrayOne, bitsOne);
        final int lenTwo = populate(arrayTwo, bitsTwo);
        final int lenExpected = populate(arrayExpected, bitsOne | bitsTwo);

        final VectorMatch matchOne = VectorMatch.wrap(arrayOne).setSelectionSize(lenOne);
        final VectorMatch matchTwo = VectorMatch.wrap(arrayTwo).setSelectionSize(lenTwo);
        final VectorMatch matchExpected = VectorMatch.wrap(arrayExpected).setSelectionSize(lenExpected);

        assertMatchEquals(
            StringUtils.format("%s + %s", matchOne, matchTwo),
            matchExpected,
            matchOne.addAll(matchTwo, scratch)
        );
      }
    }
  }

  @Test
  public void testAddAllOnSelf()
  {
    final VectorMatch match = VectorMatch.wrap(new int[]{0, 1}).setSelectionSize(2);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("'other' must be a different instance from 'this'");
    match.addAll(match, VectorMatch.wrap(new int[2]));
  }

  @Test
  public void testRemoveAllExhaustive()
  {
    // Tests every combination of vectors up to length VECTOR_SIZE.

    final int[] arrayOne = new int[VECTOR_SIZE];
    final int[] arrayTwo = new int[VECTOR_SIZE];
    final int[] arrayExpected = new int[VECTOR_SIZE];

    for (int bitsOne = 0; bitsOne < VECTOR_BITS; bitsOne++) {
      for (int bitsTwo = 0; bitsTwo < VECTOR_BITS; bitsTwo++) {
        final int lenOne = populate(arrayOne, bitsOne);
        final int lenTwo = populate(arrayTwo, bitsTwo);
        final int lenExpected = populate(arrayExpected, bitsOne & ~bitsTwo);

        final VectorMatch matchOne = VectorMatch.wrap(arrayOne).setSelectionSize(lenOne);
        final VectorMatch matchTwo = VectorMatch.wrap(arrayTwo).setSelectionSize(lenTwo);
        final VectorMatch matchExpected = VectorMatch.wrap(arrayExpected).setSelectionSize(lenExpected);

        assertMatchEquals(
            StringUtils.format("%s - %s", matchOne, matchTwo),
            matchExpected,
            matchOne.removeAll(matchTwo)
        );
      }
    }
  }

  @Test
  public void testRemoveAllOnSelf()
  {
    final VectorMatch match = VectorMatch.wrap(new int[]{0, 1}).setSelectionSize(2);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("'other' must be a different instance from 'this'");
    match.removeAll(match);
  }

  @Test
  public void testCopyFromExhaustive()
  {
    // Tests every vector up to length VECTOR_SIZE.

    final VectorMatch target = VectorMatch.wrap(new int[VECTOR_SIZE]);

    final int[] array = new int[VECTOR_SIZE];
    final int[] arrayTwo = new int[VECTOR_SIZE];

    for (int bits = 0; bits < VECTOR_BITS; bits++) {
      final int len = populate(array, bits);
      populate(arrayTwo, bits);

      final VectorMatch match = VectorMatch.wrap(array).setSelectionSize(len);
      target.copyFrom(match);

      // Sanity check: array shouldn't have been modified
      Assert.assertArrayEquals(array, arrayTwo);

      assertMatchEquals(match.toString(), match, target);
    }
  }

  @Test
  public void testCopyFromOnSelf()
  {
    final VectorMatch match = VectorMatch.wrap(new int[]{0, 1}).setSelectionSize(2);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("'other' must be a different instance from 'this'");
    match.copyFrom(match);
  }

  /**
   * Useful because VectorMatch equality is based on identity, not value. (Since they are mutable.)
   */
  private static void assertMatchEquals(String message, ReadableVectorMatch expected, ReadableVectorMatch actual)
  {
    Assert.assertEquals(message, expected.toString(), actual.toString());
  }

  private static int populate(final int[] array, final int bits)
  {
    int len = 0;

    for (int bit = 0; bit < VECTOR_SIZE; bit++) {
      final int mask = (1 << bit);
      if ((bits & mask) == mask) {
        array[len++] = bit;
      }
    }

    return len;
  }
}
