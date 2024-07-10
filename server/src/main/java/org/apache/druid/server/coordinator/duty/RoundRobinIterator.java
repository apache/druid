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

package org.apache.druid.server.coordinator.duty;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A round-robin iterator that is always backed by an ordered list of candidates containing no duplicates.
 * The iterator has the following properties:
 * <ul>
 *   <li> Starts with an initial random cursor position in an ordered list of candidates. </li>
 *   <li> Invoking {@code next()} on {@link #getIterator()} is guaranteed to be deterministic
 *   unless the set of candidates change when {@link #updateCandidates(Set)} is called. When the candidates change,
 *   the cursor is reset to a random position in the new list of ordered candidates. </li>
 *   <li> Guarantees that no duplicate candidates are returned in two consecutive {@code next()} iterations. </li>
 * </ul>
 *
 */
@NotThreadSafe
public class RoundRobinIterator
{
  private final List<String> candidates = new ArrayList<>();
  private int cursorPosition;
  private String previousCandidate;

  /**
   * Update the candidates with the supplied input if the input set is different from the current candidates.
   * If the input set is the same as the existing candidates, the cursor position remains unchanged so the iteration order
   * is determistic and resumed. If the input set is different, the cursor position is reset to a random location in
   * the new list of sorted candidates.
   */
  public void updateCandidates(final Set<String> input)
  {
    if (new HashSet<>(candidates).equals(input)) {
      return;
    }
    this.candidates.clear();
    this.candidates.addAll(input);
    Collections.sort(this.candidates);
    this.cursorPosition = generateRandomCursorPosition(input.size());
  }

  public Iterator<String> getIterator()
  {
    return new Iterator<String>()
    {
      @Override
      public boolean hasNext()
      {
        return candidates.size() > 0;
      }

      @Override
      public String next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        // If the next candidate to be served is the same as the previous candidate served (duplicate),
        // advance the cursor twice. Otherwise, advance the cursor only once.
        String nextCandidate = peakNextCandiate();
        advanceCursor();
        if (nextCandidate.equals(previousCandidate)) {
          nextCandidate = peakNextCandiate();
          advanceCursor();
        }
        previousCandidate = nextCandidate;
        return nextCandidate;
      }

      private String peakNextCandiate()
      {
        final int nextPosition = cursorPosition < candidates.size() ? cursorPosition : 0;
        return candidates.get(nextPosition);
      }

      private void advanceCursor()
      {
        if (++cursorPosition >= candidates.size()) {
          cursorPosition = 0;
        }
      }
    };
  }

  @VisibleForTesting
  int generateRandomCursorPosition(final int maxBound)
  {
    return maxBound <= 0 ? 0 : ThreadLocalRandom.current().nextInt(0, maxBound);
  }

  @VisibleForTesting
  int getCurrentCursorPosition()
  {
    return this.cursorPosition;
  }
}
