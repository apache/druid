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

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A circular list that is backed by an ordered list of elements containing no duplicates. The list is ordered by the
 * supplied comparator. The iterator keeps track of the current position, so iterating the list multiple times will
 * resume from the last location and continue until a caller explicitly terminates it.
 * <p>
 * This class is not thread-safe and must be used from a single thread.
 */
@NotThreadSafe
public class CircularList<T> implements Iterable<T>
{
  private final List<T> elements = new ArrayList<>();
  private int currentPosition;

  public CircularList(final Set<T> elements, final Comparator<? super T> comparator)
  {
    this.elements.addAll(elements);
    this.elements.sort(comparator);
    this.currentPosition = -1;
  }

  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<>()
    {
      @Override
      public boolean hasNext()
      {
        return elements.size() > 0;
      }

      @Override
      public T next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        advanceCursor();
        return elements.get(currentPosition);
      }

      private void advanceCursor()
      {
        if (++currentPosition >= elements.size()) {
          currentPosition = 0;
        }
      }
    };
  }

  /**
   * @return true if the supplied set is equal to the set used to instantiate this circular list, otherwise false.
   */
  public boolean equalsSet(final Set<T> inputSet)
  {
    return new HashSet<>(elements).equals(inputSet);
  }
}
