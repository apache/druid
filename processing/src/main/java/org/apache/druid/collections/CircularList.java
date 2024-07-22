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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 * @param <T>
 */
public class CircularList<T> implements Iterable<T>
{
  private final List<T> collection = new ArrayList<>();
  private final Comparator<? super T> comparator;
  private int currentPosition;

  public CircularList(final Set<T> elements, Comparator<? super T> comparator)
  {
    this.collection.addAll(elements);
    this.comparator = comparator;
    this.collection.sort(comparator);
  }

  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<T>()
    {
      @Override
      public boolean hasNext()
      {
        return collection.size() > 0;
      }

      @Override
      public T next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        T nextCandidate = peekNext();
        advanceCursor();
        return nextCandidate;
      }

      T peekNext()
      {
        int nextPosition = currentPosition < collection.size() ? currentPosition : 0;
        return collection.get(nextPosition);
      }
    };
  }

  public void advanceCursor()
  {
    if (++currentPosition >= collection.size()) {
      currentPosition = 0;
    }
  }

  public void resetCursor(final int position)
  {
    this.currentPosition = position;
  }

  public boolean equalsSet(final Set<T> s)
  {
    final List<T> x = new ArrayList<>(s);
    x.sort(comparator);
    return collection.equals(x);
  }
}
