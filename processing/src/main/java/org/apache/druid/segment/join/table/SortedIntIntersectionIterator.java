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

package org.apache.druid.segment.join.table;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Iterates over the intersection of an array of sorted int lists. Intended for situations where the number
 * of iterators is fairly small. The iterators must be composed of ascending, nonnegative ints.
 *
 * @see RowBasedIndexedTable#columnReader uses this
 */
public class SortedIntIntersectionIterator implements IntIterator
{
  private static final int NIL = -1;

  private final IntIterator[] iterators;
  private final int[] currents;

  private int next = NIL;

  SortedIntIntersectionIterator(final IntIterator[] iterators)
  {
    Preconditions.checkArgument(iterators.length > 0, "iterators.length > 0");
    this.iterators = iterators;
    this.currents = new int[iterators.length];
    Arrays.fill(currents, NIL);
    advance();
  }

  @Override
  public int nextInt()
  {
    if (next == NIL) {
      throw new NoSuchElementException();
    }

    final int retVal = next;

    advance();

    return retVal;
  }

  @Override
  public boolean hasNext()
  {
    return next != NIL;
  }

  private void advance()
  {
    next++;

    // This is the part that assumes the number of iterators is fairly small.
    boolean foundNext = false;
    while (!foundNext) {
      foundNext = true;

      for (int i = 0; i < iterators.length; i++) {
        while (currents[i] < next && iterators[i].hasNext()) {
          currents[i] = iterators[i].nextInt();
        }

        if (currents[i] < next && !iterators[i].hasNext()) {
          next = NIL;
          return;
        } else if (currents[i] > next) {
          next = currents[i];
          foundNext = false;
        }
      }
    }

    assert Arrays.stream(currents).allMatch(x -> x == next);
  }
}
