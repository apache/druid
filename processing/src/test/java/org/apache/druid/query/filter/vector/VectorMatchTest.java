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

import org.junit.Assert;
import org.junit.Test;

public class VectorMatchTest
{
  private static final int VECTOR_SIZE = 10;

  @Test
  public void testRemoveAll()
  {
    assertMatchEquals(
        VectorMatch.allFalse(),
        copy(VectorMatch.allTrue(VECTOR_SIZE)).removeAll(VectorMatch.allTrue(VECTOR_SIZE))
    );

    assertMatchEquals(
        VectorMatch.allTrue(VECTOR_SIZE),
        copy(VectorMatch.allTrue(VECTOR_SIZE)).removeAll(VectorMatch.allFalse())
    );

    assertMatchEquals(
        createMatch(new int[]{3, 6, 7, 8, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).removeAll(createMatch(new int[]{4, 5, 9}))
    );

    assertMatchEquals(
        createMatch(new int[]{3, 6, 7, 8, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).removeAll(createMatch(new int[]{2, 5, 9}))
    );

    assertMatchEquals(
        createMatch(new int[]{6, 7, 8, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).removeAll(createMatch(new int[]{3, 5, 9}))
    );

    assertMatchEquals(
        createMatch(new int[]{6, 7, 8}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).removeAll(createMatch(new int[]{3, 5, 10}))
    );
  }

  @Test
  public void testAddAll()
  {
    final VectorMatch scratch = VectorMatch.wrap(new int[VECTOR_SIZE]);

    assertMatchEquals(
        VectorMatch.allTrue(VECTOR_SIZE),
        copy(VectorMatch.allTrue(VECTOR_SIZE)).addAll(VectorMatch.allTrue(VECTOR_SIZE), scratch)
    );

    assertMatchEquals(
        VectorMatch.allTrue(VECTOR_SIZE),
        createMatch(new int[]{}).addAll(VectorMatch.allTrue(VECTOR_SIZE), scratch)
    );

    assertMatchEquals(
        createMatch(new int[]{3, 4, 5, 6, 7, 8, 9, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).addAll(createMatch(new int[]{4, 5, 9}), scratch)
    );

    assertMatchEquals(
        createMatch(new int[]{3, 4, 5, 6, 7, 8, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8}).addAll(createMatch(new int[]{4, 5, 10}), scratch)
    );

    assertMatchEquals(
        createMatch(new int[]{2, 3, 5, 6, 7, 8, 9, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).addAll(createMatch(new int[]{2, 5, 9}), scratch)
    );

    assertMatchEquals(
        createMatch(new int[]{3, 5, 6, 7, 8, 9, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).addAll(createMatch(new int[]{3, 5, 9}), scratch)
    );

    assertMatchEquals(
        createMatch(new int[]{3, 5, 6, 7, 8, 10}),
        createMatch(new int[]{3, 5, 6, 7, 8, 10}).addAll(createMatch(new int[]{3, 5, 10}), scratch)
    );
  }

  /**
   * Useful because VectorMatch equality is based on identity, not value. (Since they are mutable.)
   */
  private static void assertMatchEquals(ReadableVectorMatch expected, ReadableVectorMatch actual)
  {
    Assert.assertEquals(expected.toString(), actual.toString());
  }

  private static VectorMatch copy(final ReadableVectorMatch match)
  {
    final int[] selection = match.getSelection();
    final int[] newSelection = new int[selection.length];
    System.arraycopy(selection, 0, newSelection, 0, selection.length);
    return VectorMatch.wrap(newSelection).setSelectionSize(match.getSelectionSize());
  }

  private static VectorMatch createMatch(final int[] selection)
  {
    final VectorMatch match = VectorMatch.wrap(new int[VECTOR_SIZE]);
    System.arraycopy(selection, 0, match.getSelection(), 0, selection.length);
    match.setSelectionSize(selection.length);
    return match;
  }
}
